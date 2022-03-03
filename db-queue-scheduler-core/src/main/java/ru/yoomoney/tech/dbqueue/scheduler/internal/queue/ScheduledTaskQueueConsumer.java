package ru.yoomoney.tech.dbqueue.scheduler.internal.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.api.Task;
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.api.TaskPayloadTransformer;
import ru.yoomoney.tech.dbqueue.api.impl.NoopPayloadTransformer;
import ru.yoomoney.tech.dbqueue.scheduler.config.ScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskDefinition;
import ru.yoomoney.tech.dbqueue.scheduler.internal.db.ScheduledTaskQueueDao;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskContext;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static java.util.Objects.requireNonNull;

/**
 * Executor of scheduled tasks is backed on {@code db-queue} library consumer abstraction.
 *
 * <p>When the consumer receives {@link Task}, it executes linked {@link ScheduledTask},
 * then schedules next execution of the task.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 21.10.2021
 */
class ScheduledTaskQueueConsumer implements QueueConsumer<String> {
    private static final Logger log = LoggerFactory.getLogger(ScheduledTaskQueueConsumer.class);

    private static final Duration MIN_HEARTBEAT_INTERVAL = Duration.ofSeconds(10L);

    private final QueueConfig queueConfig;
    private final ScheduledTaskDefinition scheduledTaskDefinition;
    private final ScheduledTaskLifecycleListener scheduledTaskLifecycleListener;
    private final ScheduledTaskQueueDao scheduledTaskQueueDao;
    private final Clock clock;

    ScheduledTaskQueueConsumer(@Nonnull QueueConfig queueConfig,
                               @Nonnull ScheduledTaskDefinition scheduledTaskDefinition,
                               @Nonnull ScheduledTaskLifecycleListener scheduledTaskLifecycleListener,
                               @Nonnull ScheduledTaskQueueDao scheduledTaskQueueDao) {
        this(queueConfig, scheduledTaskDefinition, scheduledTaskLifecycleListener, scheduledTaskQueueDao,
                Clock.systemDefaultZone());
    }

    ScheduledTaskQueueConsumer(@Nonnull QueueConfig queueConfig,
                               @Nonnull ScheduledTaskDefinition scheduledTaskDefinition,
                               @Nonnull ScheduledTaskLifecycleListener scheduledTaskLifecycleListener,
                               @Nonnull ScheduledTaskQueueDao scheduledTaskQueueDao,
                               @Nonnull Clock clock) {
        this.queueConfig = requireNonNull(queueConfig, "queueConfig");
        this.scheduledTaskDefinition = requireNonNull(scheduledTaskDefinition, "scheduledTaskDefinition");
        this.scheduledTaskLifecycleListener = requireNonNull(scheduledTaskLifecycleListener, "scheduledTaskLifecycleListener");
        this.scheduledTaskQueueDao = requireNonNull(scheduledTaskQueueDao, "scheduledTaskQueueDao");
        this.clock = requireNonNull(clock, "clock");
    }

    @Nonnull
    @Override
    public TaskExecutionResult execute(@Nonnull Task<String> task) {
        ScheduledTaskContext scheduledTaskContext = ScheduledTaskContext.builder()
                .withCreatedAt(task.getCreatedAt().toInstant())
                .withState(task.getPayload().orElse(null))
                .withAttemptsCount(task.getAttemptsCount())
                .withSuccessfulAttemptsCount(task.getReenqueueAttemptsCount())
                .withTotalAttemptsCount(task.getTotalAttemptsCount())
                .build();

        scheduledTaskLifecycleListener.started(scheduledTaskDefinition.getIdentity(), scheduledTaskContext);
        log.debug("execute(): scheduledTaskIdentity={}, task={}", scheduledTaskDefinition.getIdentity(), task);

        long start = clock.millis();
        ScheduledTaskExecutionContext internalContext = new ScheduledTaskExecutionContext();
        internalContext.setAttemptsCount(task.getAttemptsCount());
        internalContext.setExecutionStartTime(scheduledTaskQueueDao.getDatabaseCurrentTime());
        ScheduledTaskExecutionResult executionResult = executeTask(scheduledTaskContext, internalContext);
        internalContext.setExecutionResultType(executionResult.getType());
        internalContext.setProcessingTime(Duration.ofMillis(clock.millis() - start));

        Duration nextExecutionDelay = executionResult.getNextExecutionTime()
                .map(nextExecutionTime -> Duration.between(clock.instant(), nextExecutionTime))
                .orElseGet(() -> scheduledTaskDefinition.getNextExecutionDelayProvider().getNextExecutionDelay(internalContext));
        Duration roundedNextExecutionDelay = roundToSeconds(nextExecutionDelay);

        log.debug("task executed: executionResult={}, nextExecutionDelay={}", executionResult, roundedNextExecutionDelay);
        scheduledTaskLifecycleListener.finished(scheduledTaskDefinition.getIdentity(), scheduledTaskContext, executionResult,
                clock.instant().plus(roundedNextExecutionDelay), internalContext.getProcessingTime().orElseThrow().toMillis());

        if (executionResult.getType() == ScheduledTaskExecutionResult.Type.ERROR) {
            scheduledTaskQueueDao.updateNextProcessDate(queueConfig.getLocation().getQueueId(), roundedNextExecutionDelay);
            return TaskExecutionResult.fail();
        }
        return TaskExecutionResult.reenqueue(roundedNextExecutionDelay);
    }

    private Duration roundToSeconds(Duration duration) {
        Duration truncatedToSeconds = duration.truncatedTo(ChronoUnit.SECONDS);
        return truncatedToSeconds.equals(duration) ? truncatedToSeconds : truncatedToSeconds.plusSeconds(1L);
    }

    private ScheduledTaskExecutionResult executeTask(ScheduledTaskContext scheduledTaskContext,
                                                     ScheduledTaskExecutionContext internalContext) {
        HeartbeatAgent heartbeatAgent = createHeartbeatAgent(internalContext);
        try {
            heartbeatAgent.start();
            ScheduledTaskExecutionResult result = scheduledTaskDefinition.getScheduledTask().execute(scheduledTaskContext);
            if (result.getState().isPresent()) {
                scheduledTaskQueueDao.updatePayload(queueConfig.getLocation().getQueueId(), result.getState().orElseThrow());
            }
            return result;
        } catch (RuntimeException ex) {
            scheduledTaskLifecycleListener.crashed(scheduledTaskDefinition.getIdentity(), scheduledTaskContext, ex);
            log.debug("failed to execute scheduled task: scheduledTask={}", scheduledTaskDefinition, ex);
            return ScheduledTaskExecutionResult.error();
        } finally {
            heartbeatAgent.stop();
        }
    }

    /**
     * Creates heartbeat agent that helps to postpone next execution date-time of the task in case of time-consuming
     * execution of the current one. That helps to prevent concurrent execution of the same task.
     *
     * @param internalContext internal context of a current execution
     * @return prepared heartbeat agent
     */
    private HeartbeatAgent createHeartbeatAgent(ScheduledTaskExecutionContext internalContext) {
        ScheduledTaskExecutionContext failInternalContext = internalContext.copy();
        failInternalContext.setExecutionResultType(ScheduledTaskExecutionResult.Type.ERROR);

        Duration precomputeNextExecutionDelay =
                scheduledTaskDefinition.getNextExecutionDelayProvider().getNextExecutionDelay(failInternalContext);

        Duration heartbeatInterval = MIN_HEARTBEAT_INTERVAL.compareTo(precomputeNextExecutionDelay.dividedBy(2L)) > 0
                ? MIN_HEARTBEAT_INTERVAL
                : precomputeNextExecutionDelay.dividedBy(2L);

        return new HeartbeatAgent(
                scheduledTaskDefinition.getIdentity().asString(),
                heartbeatInterval,
                () -> shiftNextExecutionTime(heartbeatInterval.multipliedBy(2L))
        );
    }

    private void shiftNextExecutionTime(Duration interval) {
        scheduledTaskQueueDao.updateNextProcessDate(queueConfig.getLocation().getQueueId(), interval);
    }

    @Nonnull
    @Override
    public QueueConfig getQueueConfig() {
        return queueConfig;
    }

    @Nonnull
    @Override
    public TaskPayloadTransformer<String> getPayloadTransformer() {
        return NoopPayloadTransformer.getInstance();
    }
}

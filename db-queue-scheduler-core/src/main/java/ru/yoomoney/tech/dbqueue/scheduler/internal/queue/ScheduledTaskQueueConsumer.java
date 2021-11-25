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
import java.time.Instant;

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

    private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(5L);

    private final QueueConfig queueConfig;
    private final HeartbeatAgent heartbeatAgent;
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
        this.heartbeatAgent = new HeartbeatAgent(
                scheduledTaskDefinition.getIdentity().asString(),
                HEARTBEAT_INTERVAL,
                () -> shiftNextExecutionTime(HEARTBEAT_INTERVAL.multipliedBy(2L))
        );
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
        ScheduledTaskExecutionResult executionResult = executeTask(scheduledTaskContext, internalContext);
        long processingTaskTime = clock.millis() - start;

        Instant nextExecutionTime = executionResult.getNextExecutionTime().orElseGet(() ->
                scheduledTaskDefinition.getNextExecutionTimeProvider().getNextExecutionTime(internalContext));

        log.debug("task executed: executionResult={}, nextExecutionTime={}", executionResult, nextExecutionTime);

        scheduledTaskLifecycleListener.finished(scheduledTaskDefinition.getIdentity(), scheduledTaskContext, executionResult,
                nextExecutionTime, processingTaskTime);
        if (executionResult.getType() == ScheduledTaskExecutionResult.Type.ERROR) {
            scheduledTaskQueueDao.updateNextProcessDate(queueConfig.getLocation().getQueueId(), nextExecutionTime);
            return TaskExecutionResult.fail();
        }
        return TaskExecutionResult.reenqueue(Duration.between(clock.instant(), nextExecutionTime));
    }

    private ScheduledTaskExecutionResult executeTask(ScheduledTaskContext scheduledTaskContext,
                                                     ScheduledTaskExecutionContext internalContext) {

        ScheduledTaskExecutionResult result;
        internalContext.setLastExecutionStartTime(clock.instant());
        try {
            heartbeatAgent.start();
            result = scheduledTaskDefinition.getScheduledTask().execute(scheduledTaskContext);
            if (result.getState().isPresent()) {
                scheduledTaskQueueDao.updatePayload(queueConfig.getLocation().getQueueId(), result.getState().orElseThrow());
            }
        } catch (RuntimeException ex) {
            scheduledTaskLifecycleListener.crashed(scheduledTaskDefinition.getIdentity(), scheduledTaskContext, ex);
            log.debug("failed to execute scheduled task: scheduledTask={}", scheduledTaskDefinition, ex);
            result = ScheduledTaskExecutionResult.error();
        } finally {
            internalContext.setLastExecutionFinishTime(clock.instant());
            heartbeatAgent.stop();
        }
        internalContext.setExecutionResultType(result.getType());
        return result;
    }

    private void shiftNextExecutionTime(Duration interval) {
        scheduledTaskQueueDao.updateNextProcessDate(queueConfig.getLocation().getQueueId(), clock.instant().plus(interval));
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

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
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask;
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

    private final QueueConfig queueConfig;
    private final ScheduledTaskDefinition scheduledTaskDefinition;
    private final ScheduledTaskLifecycleListener scheduledTaskLifecycleListener;
    private final Clock clock;

    ScheduledTaskQueueConsumer(@Nonnull QueueConfig queueConfig,
                               @Nonnull ScheduledTaskDefinition scheduledTaskDefinition,
                               @Nonnull ScheduledTaskLifecycleListener scheduledTaskLifecycleListener) {
        this(queueConfig, scheduledTaskDefinition, scheduledTaskLifecycleListener, Clock.systemDefaultZone());
    }

    ScheduledTaskQueueConsumer(@Nonnull QueueConfig queueConfig,
                               @Nonnull ScheduledTaskDefinition scheduledTaskDefinition,
                               @Nonnull ScheduledTaskLifecycleListener scheduledTaskLifecycleListener,
                               @Nonnull Clock clock) {
        this.queueConfig = requireNonNull(queueConfig, "queueConfig");
        this.scheduledTaskDefinition = requireNonNull(scheduledTaskDefinition, "scheduledTaskDefinition");
        this.scheduledTaskLifecycleListener = requireNonNull(scheduledTaskLifecycleListener, "scheduledTaskLifecycleListener");
        this.clock = requireNonNull(clock, "clock");
    }

    @Nonnull
    @Override
    public TaskExecutionResult execute(@Nonnull Task<String> task) {
        scheduledTaskLifecycleListener.started(scheduledTaskDefinition.getIdentity());
        log.debug("execute(): scheduledTaskIdentity={}, task={}", scheduledTaskDefinition.getIdentity(), task);

        long start = clock.millis();
        ScheduledTaskExecutionContext context = new ScheduledTaskExecutionContext();
        ScheduledTaskExecutionResult executionResult = executeTask(context);
        long processingTaskTime = clock.millis() - start;

        Instant nextExecutionTime = executionResult.getNextExecutionTime().orElseGet(() ->
                        scheduledTaskDefinition.getNextExecutionTimeProvider().getNextExecutionTime(context));

        log.debug("task executed: executionResult={}, nextExecutionTime={}", executionResult, nextExecutionTime);
        scheduledTaskLifecycleListener.finished(scheduledTaskDefinition.getIdentity(), executionResult, nextExecutionTime,
                processingTaskTime);
        return TaskExecutionResult.reenqueue(Duration.between(clock.instant(), nextExecutionTime));
    }

    private ScheduledTaskExecutionResult executeTask(ScheduledTaskExecutionContext context) {
        context.setLastExecutionStartTime(clock.instant());
        try {
            return scheduledTaskDefinition.getScheduledTask().execute();
        } catch (RuntimeException ex) {
            scheduledTaskLifecycleListener.crashed(scheduledTaskDefinition.getIdentity(), ex);
            log.debug("failed to execute scheduled task: scheduledTask={}", scheduledTaskDefinition, ex);
            return ScheduledTaskExecutionResult.error();
        } finally {
            context.setLastExecutionFinishTime(clock.instant());
        }
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

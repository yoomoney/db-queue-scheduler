package ru.yoomoney.tech.dbqueue.scheduler.config;

import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;

import javax.annotation.Nonnull;
import java.time.Instant;

/**
 * Listener for task processing lifecycle
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 27.10.2021
 */
public interface ScheduledTaskLifecycleListener {
    /**
     * Event of task starting execution.
     * <p>
     * Always triggered before scheduled task execution.
     * <p>
     * Might be useful for updating a logging context.
     *
     * @param taskIdentity identity of executing task
     */
    void started(@Nonnull ScheduledTaskIdentity taskIdentity);

    /**
     * Event for completion of client logic when task processing.
     * <p>
     * Always triggered when task processing has finished with any result.
     * <p>
     * Might be useful for recovery of initial logging context state.
     *
     * @param taskIdentity identity of executing task
     * @param executionResult result of task processing
     * @param nextExecutionTime task next execution date
     * @param processTaskTimeInMills time spent on task processing in millis
     */
    void finished(@Nonnull ScheduledTaskIdentity taskIdentity,
                  @Nonnull ScheduledTaskExecutionResult executionResult,
                  @Nonnull Instant nextExecutionTime,
                  long processTaskTimeInMills);
}

package ru.yoomoney.tech.dbqueue.scheduler.models;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Task executed periodically at a scheduled time
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 20.10.2021
 */
public interface ScheduledTask {
    /**
     * Get unique identity of a scheduled task
     *
     * @return identity of a scheduled task
     */
    @Nonnull
    ScheduledTaskIdentity getIdentity();

    /**
     * Executes scheduled task
     *
     * @return execution result
     */
    @Nonnull
    ScheduledTaskExecutionResult execute();

    /**
     * Executes scheduled task with a state
     *
     * @return execution result
     */
    @Nonnull
    default ScheduledTaskExecutionResult execute(@Nullable String state) {
        return execute();
    }
}

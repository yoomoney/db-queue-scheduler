package ru.yoomoney.tech.dbqueue.scheduler.models;

import javax.annotation.Nonnull;

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
}

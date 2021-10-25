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
     * Executes scheduled task
     *
     * @return execution result
     */
    @Nonnull
    ScheduledTaskExecutionResult execute();
}

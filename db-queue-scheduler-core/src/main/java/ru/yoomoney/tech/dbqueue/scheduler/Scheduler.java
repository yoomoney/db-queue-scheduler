package ru.yoomoney.tech.dbqueue.scheduler;

import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduledTaskSettings;

import javax.annotation.Nonnull;

/**
 * Scheduler manages {@link ScheduledTask}s for periodic execution - configures, registers, starts and pauses.
 *
 * <p>Scheduler guarantees exactly-once task execution that means that any registered tasks is executed exactly once per each
 * scheduled time in spite of any numbers of working application nodes.
 *
 * <p>Scheduler uses RDBMS for persisting registered tasks. Currently, scheduled backed on {@code db-queue} library.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 19.10.2021
 */
public interface Scheduler {
    /**
     * Register a task for periodic executions
     *
     * @param scheduledTaskIdentity unique identity of a scheduled task
     * @param scheduledTaskSettings settings of the scheduled task
     * @param scheduledTask task for periodic executions
     * @throws RuntimeException if any scheduled task with the same identity already registered
     */
    void schedule(@Nonnull ScheduledTaskIdentity scheduledTaskIdentity,
                  @Nonnull ScheduledTaskSettings scheduledTaskSettings,
                  @Nonnull ScheduledTask scheduledTask);

    /**
     * Starts executing scheduled tasks
     */
    void start();

    /**
     * Resumes executing scheduled tasks
     */
    void unpause();

    /**
     * Pauses executing scheduled tasks.
     *
     * <p>Method does not interrupt currently processing tasks but prevents starting the new ones.
     */
    void pause();
}

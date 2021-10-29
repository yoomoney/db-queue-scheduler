package ru.yoomoney.tech.dbqueue.scheduler;

import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduledTaskSettings;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.List;

/**
 * Scheduler manages {@link ScheduledTask}s for periodic execution - configures, registers, starts and pauses.
 *
 * <p>Scheduler guarantees exactly-once task execution that means that any registered tasks is executed exactly once per each
 * scheduled time in spite of any numbers of working application nodes.
 *
 * <p>Scheduler uses RDBMS for persisting registered tasks. Currently, scheduler backed on {@code db-queue} library.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 19.10.2021
 */
public interface Scheduler {
    /**
     * Registers a task for periodic executions
     *
     * @param scheduledTask task for periodic executions
     * @param scheduledTaskSettings settings of the scheduled task
     * @throws RuntimeException if any scheduled task with the same identity already registered
     */
    void schedule(@Nonnull ScheduledTask scheduledTask,
                  @Nonnull ScheduledTaskSettings scheduledTaskSettings);

    /**
     * Starts scheduler - makes scheduler available for executing scheduled tasks
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

    /**
     * Shutdowns the executor
     */
    void shutdown();

    /**
     * Waits for tasks (and threads) termination within given timeout.
     *
     * @param timeout wait timeout.
     * @return list of scheduled task identities, which didn't stop their work (didn't terminate).
     */
    List<ScheduledTaskIdentity> awaitTermination(@Nonnull Duration timeout);
}

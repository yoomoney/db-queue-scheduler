package ru.yoomoney.tech.dbqueue.scheduler.internal.db;

import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;

/**
 * Direct access to db-queue tables
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 20.10.2021
 */
public interface ScheduledTaskQueueDao {
    /**
     * Checks if a queue is empty
     *
     * @param queueLocation checking queue location
     * @return true if the queue is empty otherwise false
     */
    boolean isQueueEmpty(@Nonnull QueueLocation queueLocation);

    /**
     * Deletes all tasks from a queue
     *
     * @param queueLocation location of the queue which should be cleaned
     */
    void clean(@Nonnull QueueLocation queueLocation);
}

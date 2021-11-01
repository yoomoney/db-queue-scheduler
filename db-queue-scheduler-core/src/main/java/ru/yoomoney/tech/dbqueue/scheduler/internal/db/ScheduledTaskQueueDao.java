package ru.yoomoney.tech.dbqueue.scheduler.internal.db;

import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.util.List;

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
     * Finds all records.
     *
     * <p>Table is not supposed to be big furthermore the method is used only for statistical purposes.
     *
     * @return list of all records
     */
    List<ScheduledTaskRecord> findAll();
}

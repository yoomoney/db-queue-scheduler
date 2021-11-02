package ru.yoomoney.tech.dbqueue.scheduler.internal.db;

import ru.yoomoney.tech.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.List;

/**
 * Direct access to db-queue tables
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 20.10.2021
 */
public interface ScheduledTaskQueueDao {
    /**
     * Finds queue tasks
     *
     * @param queueId identity of the queue where task should be found
     * @return list of found tasks
     */
    List<ScheduledTaskRecord> findQueueTasks(@Nonnull QueueId queueId);

    /**
     * Delete tasks from the queue
     * @param queueId identity of the queue where task should be deleted
     * @param taskIds identity of the tasks that should be deleted
     */
    void deleteQueueTasks(@Nonnull QueueId queueId, List<Long> taskIds);

    /**
     * Updates next process date column of a queue tasks
     *
     * @param queueId identity of the queue
     * @param nextProcessDate new column value
     * @return count of updated rows
     */
    int updateNextProcessDate(@Nonnull QueueId queueId, @Nonnull Instant nextProcessDate);

    /**
     * Finds all records.
     *
     * <p>Table is not supposed to be big furthermore the method is used only for statistical purposes.
     *
     * @return list of all records
     */
    List<ScheduledTaskRecord> findAll();
}

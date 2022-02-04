package ru.yoomoney.tech.dbqueue.scheduler.internal.db;

import ru.yoomoney.tech.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

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
     * @param queueId identity of the checking queue
     * @return true if the queue is empty otherwise false
     */
    Optional<ScheduledTaskRecord> findQueueTask(@Nonnull QueueId queueId);

    /**
     * Updates next process date column of a queue tasks
     *
     * @param queueId identity of the queue
     * @param executionDelay execution delay
     * @return count of updated rows
     */
    int updateNextProcessDate(@Nonnull QueueId queueId, @Nonnull Duration executionDelay);

    /**
     * Updates payload column of a queue tasks
     *
     * @param queueId identity of the queue
     * @param payload new payload
     * @return count of updated rows
     */
    int updatePayload(@Nonnull QueueId queueId, @Nullable String payload);

    /**
     * Finds all records.
     *
     * <p>Table is not supposed to be big furthermore the method is used only for statistical purposes.
     *
     * @return list of all records
     */
    List<ScheduledTaskRecord> findAll();
}

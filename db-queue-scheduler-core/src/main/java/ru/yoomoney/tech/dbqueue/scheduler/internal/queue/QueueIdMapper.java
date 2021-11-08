package ru.yoomoney.tech.dbqueue.scheduler.internal.queue;

import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;
import ru.yoomoney.tech.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;

/**
 * Mapper {@link QueueId} to {@link ScheduledTaskIdentity} and vice versa.
 *
 * <p>Scheduled tasks is built on top of {@code db-queue} library. Each scheduled task has its own db-queue queue.
 * The mapper encapsulates identifiers` transforming logic.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 01.11.2021
 */
public class QueueIdMapper {
    /**
     * Creates {@link QueueId} according to {@link ScheduledTaskIdentity}
     *
     * @param identity identity of a scheduled task
     * @return identity of a db-queue queue
     */
    public QueueId toQueueId(@Nonnull ScheduledTaskIdentity identity) {
        return new QueueId(identity.asString());
    }

    /**
     * Creates {@link ScheduledTaskIdentity} according to {@link QueueId}
     *
     * @param queueId identity of a db-queue queue
     * @return identity of a scheduled task
     */
    public ScheduledTaskIdentity toScheduledTaskIdentity(@Nonnull QueueId queueId) {
        return ScheduledTaskIdentity.of(queueId.asString());
    }
}

package ru.yoomoney.tech.dbqueue.scheduler.internal.queue;

import ru.yoomoney.tech.dbqueue.config.QueueService;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Manager of {@link ScheduledTaskQueue}s
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
public class ScheduledTaskQueueRegistry {
    private final QueueService queueService;

    public ScheduledTaskQueueRegistry(QueueService queueService) {
        this.queueService = queueService;
    }

    /**
     * Registers a scheduledTaskQueue
     *
     * @param scheduledTaskQueue scheduled task queue
     */
    public void register(@Nonnull ScheduledTaskQueue scheduledTaskQueue) {
        requireNonNull(scheduledTaskQueue, "scheduledTaskQueue");
        queueService.registerQueue(scheduledTaskQueue.getQueueConsumer());
    }
}

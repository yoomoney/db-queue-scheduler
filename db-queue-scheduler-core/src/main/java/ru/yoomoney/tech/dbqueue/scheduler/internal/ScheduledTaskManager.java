package ru.yoomoney.tech.dbqueue.scheduler.internal;

import ru.yoomoney.tech.dbqueue.config.QueueService;
import ru.yoomoney.tech.dbqueue.scheduler.internal.queue.ScheduledTaskQueue;
import ru.yoomoney.tech.dbqueue.scheduler.internal.queue.ScheduledTaskQueueFactory;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Manager of scheduled tasks - internal abstraction that hides the details of initializing and schedules periodic tasks.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 22.10.2021
 */
public class ScheduledTaskManager {
    private final QueueService queueService;
    private final ScheduledTaskQueueFactory scheduledTaskQueueFactory;
    private final Map<ScheduledTaskIdentity, ScheduledTaskDefinition> registry = new ConcurrentHashMap<>();

    ScheduledTaskManager(@Nonnull QueueService queueService,
                         @Nonnull ScheduledTaskQueueFactory scheduledTaskQueueFactory) {
        this.queueService = requireNonNull(queueService, "queueService");
        this.scheduledTaskQueueFactory = requireNonNull(scheduledTaskQueueFactory, "scheduledTaskQueueFactory");
    }

    /**
     * Registers a task for periodic execution
     *
     * @param scheduledTaskDefinition definition of a scheduled task
     */
    public void register(@Nonnull ScheduledTaskDefinition scheduledTaskDefinition) {
        requireNonNull(scheduledTaskDefinition, "scheduledTaskDefinition");

        if (registry.putIfAbsent(scheduledTaskDefinition.getIdentity(), scheduledTaskDefinition) == null) {
            throw new RuntimeException(String.format("scheduled task already registered: identity=%s",
                    scheduledTaskDefinition.getIdentity()));
        }

        ScheduledTaskQueue scheduledTaskQueue = scheduledTaskQueueFactory.createScheduledTasksQueue(scheduledTaskDefinition);
        scheduledTaskQueue.trySchedule(scheduledTaskDefinition);

        queueService.registerQueue(scheduledTaskQueue.getQueueConsumer());
    }
}

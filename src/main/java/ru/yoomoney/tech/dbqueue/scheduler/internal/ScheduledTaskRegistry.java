package ru.yoomoney.tech.dbqueue.scheduler.internal;

import ru.yoomoney.tech.dbqueue.scheduler.internal.queue.ScheduledTaskQueue;
import ru.yoomoney.tech.dbqueue.scheduler.internal.queue.ScheduledTaskQueueFactory;
import ru.yoomoney.tech.dbqueue.scheduler.internal.queue.ScheduledTaskQueueRegistry;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Registry of scheduled tasks - internal abstraction that hides the details of initializing and schedules periodic tasks.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 22.10.2021
 */
public class ScheduledTaskRegistry {
    private final ScheduledTaskQueueFactory scheduledTaskQueueFactory;
    private final ScheduledTaskQueueRegistry scheduledTaskQueueRegistry;
    private final Map<String, ScheduledTaskDefinition> registry = new ConcurrentHashMap<>();

    public ScheduledTaskRegistry(@Nonnull ScheduledTaskQueueFactory scheduledTaskQueueFactory,
                                 @Nonnull ScheduledTaskQueueRegistry scheduledTaskQueueRegistry) {
        this.scheduledTaskQueueFactory = requireNonNull(scheduledTaskQueueFactory, "scheduledTaskQueueFactory");
        this.scheduledTaskQueueRegistry = requireNonNull(scheduledTaskQueueRegistry, "scheduledTaskQueueRegistry");
    }

    /**
     * Registers a task for periodic execution
     *
     * @param scheduledTaskDefinition definition of a scheduled task
     */
    public void register(@Nonnull ScheduledTaskDefinition scheduledTaskDefinition) {
        requireNonNull(scheduledTaskDefinition, "scheduledTaskDefinition");

        if (registry.putIfAbsent(scheduledTaskDefinition.getName(), scheduledTaskDefinition) == null) {
            throw new RuntimeException(String.format("scheduled task already registered: name=%s",
                    scheduledTaskDefinition.getName()));
        }

        ScheduledTaskQueue scheduledTaskQueue = scheduledTaskQueueFactory.createScheduledTasksQueue(scheduledTaskDefinition);
        scheduledTaskQueue.trySchedule(scheduledTaskDefinition);

        scheduledTaskQueueRegistry.register(scheduledTaskQueue);
    }
}

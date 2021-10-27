package ru.yoomoney.tech.dbqueue.scheduler.internal;

import ru.yoomoney.tech.dbqueue.config.QueueService;
import ru.yoomoney.tech.dbqueue.scheduler.internal.queue.ScheduledTaskQueue;
import ru.yoomoney.tech.dbqueue.scheduler.internal.queue.ScheduledTaskQueueFactory;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;
import ru.yoomoney.tech.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
    private final Map<QueueId, ScheduledTaskIdentity> queueToScheduledTaskMap = new ConcurrentHashMap<>();
    private final Object mutex = new Object();
    private volatile boolean started = false;

    ScheduledTaskManager(@Nonnull QueueService queueService,
                         @Nonnull ScheduledTaskQueueFactory scheduledTaskQueueFactory) {
        this.queueService = requireNonNull(queueService, "queueService");
        this.scheduledTaskQueueFactory = requireNonNull(scheduledTaskQueueFactory, "scheduledTaskQueueFactory");
    }

    /**
     * Schedules a task for periodic execution
     *
     * @param scheduledTaskDefinition definition of a scheduled task
     * @throws RuntimeException if any scheduled task with the same identity already registered
     */
    public void schedule(@Nonnull ScheduledTaskDefinition scheduledTaskDefinition) {
        requireNonNull(scheduledTaskDefinition, "scheduledTaskDefinition");

        if (registry.putIfAbsent(scheduledTaskDefinition.getIdentity(), scheduledTaskDefinition) != null) {
            throw new RuntimeException(String.format("scheduled task already registered: identity=%s",
                    scheduledTaskDefinition.getIdentity()));
        }

        ScheduledTaskQueue scheduledTaskQueue = scheduledTaskQueueFactory.createScheduledTasksQueue(scheduledTaskDefinition);
        scheduledTaskQueue.trySchedule(scheduledTaskDefinition);

        queueService.registerQueue(scheduledTaskQueue.getQueueConsumer());

        queueToScheduledTaskMap.put(
                scheduledTaskQueue.getQueueConsumer().getQueueConfig().getLocation().getQueueId(),
                scheduledTaskDefinition.getIdentity()
        );

        synchronized (mutex) {
            if (started) {
                queueService.start(scheduledTaskQueue.getQueueConsumer().getQueueConfig().getLocation().getQueueId());
            }
        }
    }

    /**
     * Starts executing scheduled tasks
     */
    public void start() {
        synchronized (mutex) {
            if (started) {
                return;
            }
            queueService.start();
            started = true;
        }
    }

    /**
     * Unpauses executing scheduled tasks
     */
    public void unpause() {
        queueService.unpause();
    }

    /**
     * Pauses executing scheduled tasks
     */
    public void pause() {
        queueService.pause();
    }

    /**
     * Shutdowns the executor
     */
    public void shutdown() {
        queueService.shutdown();
    }

    /**
     * Waits for tasks (and threads) termination within given timeout.
     *
     * @param timeout wait timeout.
     * @return list of scheduled task identities, which didn't stop their work (didn't terminate).
     */
    public List<ScheduledTaskIdentity> awaitTermination(@Nonnull Duration timeout) {
        requireNonNull(timeout, "timeout");
        return queueService.awaitTermination(timeout).stream()
                .map(queueToScheduledTaskMap::get)
                .collect(Collectors.toList());
    }
}

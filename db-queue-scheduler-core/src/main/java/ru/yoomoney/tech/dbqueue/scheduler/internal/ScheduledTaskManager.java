package ru.yoomoney.tech.dbqueue.scheduler.internal;

import ru.yoomoney.tech.dbqueue.config.QueueService;
import ru.yoomoney.tech.dbqueue.scheduler.internal.db.ScheduledTaskQueueDao;
import ru.yoomoney.tech.dbqueue.scheduler.internal.queue.QueueIdMapper;
import ru.yoomoney.tech.dbqueue.scheduler.internal.queue.ScheduledTaskQueue;
import ru.yoomoney.tech.dbqueue.scheduler.internal.queue.ScheduledTaskQueueFactory;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;
import ru.yoomoney.tech.dbqueue.scheduler.models.info.ScheduledTaskInfo;
import ru.yoomoney.tech.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
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
    private final QueueIdMapper queueIdMapper;
    private final ScheduledTaskQueueDao scheduledTaskQueueDao;
    private final ScheduledTaskQueueFactory scheduledTaskQueueFactory;
    private final Map<ScheduledTaskIdentity, ScheduledTaskDefinition> registry = new ConcurrentHashMap<>();
    private final Object mutex = new Object();
    private volatile boolean started = false;

    ScheduledTaskManager(@Nonnull QueueService queueService,
                         @Nonnull QueueIdMapper queueIdMapper,
                         @Nonnull ScheduledTaskQueueDao scheduledTaskQueueDao,
                         @Nonnull ScheduledTaskQueueFactory scheduledTaskQueueFactory) {
        this.queueService = requireNonNull(queueService, "queueService");
        this.queueIdMapper = requireNonNull(queueIdMapper, "queueIdMapper");
        this.scheduledTaskQueueDao = requireNonNull(scheduledTaskQueueDao, "scheduledTaskQueueDao");
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
        scheduledTaskQueue.initTask();

        if (!scheduledTaskDefinition.isEnabled()) {
            return;
        }

        queueService.registerQueue(scheduledTaskQueue.getQueueConsumer());

        synchronized (mutex) {
            if (started) {
                queueService.start(scheduledTaskQueue.getQueueConsumer().getQueueConfig().getLocation().getQueueId());
            }
        }
    }

    /**
     * Updates next execution time of a scheduled task
     *
     * @param taskIdentity identity of the task that should be rescheduled
     * @param nextExecutionTime date time at which the task should be executed
     */
    public void reschedule(@Nonnull ScheduledTaskIdentity taskIdentity, @Nonnull Instant nextExecutionTime) {
        requireNonNull(taskIdentity, "taskIdentity");
        requireNonNull(nextExecutionTime, "nextExecutionTime");

        scheduledTaskQueueDao.updateNextProcessDate(queueIdMapper.toQueueId(taskIdentity), nextExecutionTime);
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
                .map(queueIdMapper::toScheduledTaskIdentity)
                .collect(Collectors.toList());
    }

    /**
     * Collects scheduler statistics
     *
     * @return collected statistics
     */
    public List<ScheduledTaskInfo> getScheduledTaskInfo() {
        return scheduledTaskQueueDao.findAll().stream()
                .map(record -> ScheduledTaskInfo.builder()
                        .withIdentity(queueIdMapper.toScheduledTaskIdentity(new QueueId(record.getQueueName())))
                        .withNextExecutionTime(record.getNextProcessAt())
                        .build())
                .collect(Collectors.toList());
    }
}

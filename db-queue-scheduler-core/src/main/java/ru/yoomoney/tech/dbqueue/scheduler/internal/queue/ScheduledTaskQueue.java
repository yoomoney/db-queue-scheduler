package ru.yoomoney.tech.dbqueue.scheduler.internal.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.api.QueueProducer;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskDefinition;
import ru.yoomoney.tech.dbqueue.scheduler.internal.db.ScheduledTaskQueueDao;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Queue represents a particular task that should be executed periodically.
 * That means the periodic execution backed on queues, namely {@code db-queue} library.
 *
 * <p>One scheduled task equals to one db-queue queue.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 21.10.2021
 */
public class ScheduledTaskQueue {
    private final Logger log = LoggerFactory.getLogger(ScheduledTaskQueue.class);

    private final QueueConfig queueConfig;
    private final QueueConsumer<String> queueConsumer;
    private final QueueProducer<String> queueProducer;
    private final ScheduledTaskQueueDao scheduledQueueDao;
    private final ScheduledTaskDefinition taskDefinition;

    public ScheduledTaskQueue(@Nonnull QueueConfig queueConfig,
                              @Nonnull QueueConsumer<String> queueConsumer,
                              @Nonnull QueueProducer<String> queueProducer,
                              @Nonnull ScheduledTaskQueueDao scheduledQueueDao,
                              @Nonnull ScheduledTaskDefinition taskDefinition) {
        this.queueConfig = requireNonNull(queueConfig, "queueConfig");
        this.queueConsumer = requireNonNull(queueConsumer, "queueConsumer");
        this.queueProducer = requireNonNull(queueProducer, "queueProducer");
        this.scheduledQueueDao = requireNonNull(scheduledQueueDao, "scheduledQueueDao");
        this.taskDefinition = requireNonNull(taskDefinition, "taskDefinition");
    }

    /**
     * Initialises periodic tasks
     */
    public void initTask() {
        if (!scheduledQueueDao.isQueueEmpty(queueConfig.getLocation().getQueueId())) {
            log.debug("scheduled task already enqueued: taskDefinition={}", taskDefinition);
            return;
        }

        ScheduledTaskExecutionContext taskExecutionContext = new ScheduledTaskExecutionContext();
        Instant nextExecutionTime = taskDefinition.getNextExecutionTimeProvider().getNextExecutionTime(taskExecutionContext);
        queueProducer.enqueue(EnqueueParams.create("").withExecutionDelay(Duration.between(Instant.now(), nextExecutionTime)));
        log.debug("scheduled task enqueued: taskDefinition={}, nextExecutionTime={}", taskDefinition, nextExecutionTime);
    }

    /**
     * Get backed {@link QueueConsumer}
     * @return queue's consumer
     */
    @Nonnull
    public QueueConsumer<?> getQueueConsumer() {
        return queueConsumer;
    }
}

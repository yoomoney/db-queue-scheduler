package ru.yoomoney.tech.dbqueue.scheduler.internal.queue;

import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.api.QueueProducer;
import ru.yoomoney.tech.dbqueue.api.QueueShardRouter;
import ru.yoomoney.tech.dbqueue.api.impl.NoopPayloadTransformer;
import ru.yoomoney.tech.dbqueue.api.impl.ShardingQueueProducer;
import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskDefinition;
import ru.yoomoney.tech.dbqueue.scheduler.internal.db.ScheduledTaskQueueDao;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Factory creates {@link ScheduledTaskQueue} for managing scheduled tasks.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 22.10.2021
 */
public class ScheduledTaskQueueFactory {
    private final String queueTableName;
    private final QueueSettings queueSettings;
    private final ScheduledTaskQueueDao scheduledTaskQueueDao;
    private final QueueShardRouter<String, ? extends DatabaseAccessLayer> queueShardRouter;

    public ScheduledTaskQueueFactory(@Nonnull String queueTableName,
                                     @Nonnull QueueSettings queueSettings,
                                     @Nonnull ScheduledTaskQueueDao scheduledTaskQueueDao,
                                     @Nonnull QueueShardRouter<String, ? extends DatabaseAccessLayer> queueShardRouter) {
        this.queueTableName = requireNonNull(queueTableName, "queueTableName");
        this.queueSettings = requireNonNull(queueSettings, "queueSettings");
        this.scheduledTaskQueueDao = requireNonNull(scheduledTaskQueueDao, "scheduledTaskQueueDao");
        this.queueShardRouter = requireNonNull(queueShardRouter, "queueShardRouter");
    }

    /**
     * Create a {@link ScheduledTaskQueue} according to passed {@code queueConfig}
     *
     * @return created scheduled task queue
     */
    public ScheduledTaskQueue createScheduledTasksQueue(@Nonnull ScheduledTaskDefinition scheduledTaskDefinition) {
        requireNonNull(scheduledTaskDefinition, "scheduledTaskDefinition");
        QueueConfig queueConfig = createQueueConfig(scheduledTaskDefinition);

        QueueConsumer<String> queueConsumer = createQueueConsumer(queueConfig, scheduledTaskDefinition);
        QueueProducer<String> queueProducer = createQueueProducer(queueConfig);

        return new ScheduledTaskQueue(queueConfig, queueConsumer, queueProducer, scheduledTaskQueueDao);
    }

    private QueueConfig createQueueConfig(ScheduledTaskDefinition scheduledTaskDefinition) {
        return new QueueConfig(
                QueueLocation.builder()
                        .withQueueId(new QueueId(scheduledTaskDefinition.getName()))
                        .withTableName(queueTableName)
                        .build(),
                queueSettings
        );
    }

    private QueueConsumer<String> createQueueConsumer(QueueConfig queueConfig, ScheduledTaskDefinition scheduledTaskDefinition) {
        return new ScheduledTaskQueueConsumer(queueConfig, scheduledTaskDefinition);
    }

    private QueueProducer<String> createQueueProducer(QueueConfig queueConfig) {
        return new ShardingQueueProducer<>(queueConfig, NoopPayloadTransformer.getInstance(), queueShardRouter);
    }
}

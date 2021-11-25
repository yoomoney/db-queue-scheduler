package ru.yoomoney.tech.dbqueue.scheduler.internal.queue;

import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.api.QueueProducer;
import ru.yoomoney.tech.dbqueue.api.QueueShardRouter;
import ru.yoomoney.tech.dbqueue.api.impl.NoopPayloadTransformer;
import ru.yoomoney.tech.dbqueue.api.impl.ShardingQueueProducer;
import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.scheduler.config.ScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskDefinition;
import ru.yoomoney.tech.dbqueue.scheduler.internal.db.ScheduledTaskQueueDao;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Factory creates {@link ScheduledTaskQueue} for managing scheduled tasks.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 22.10.2021
 */
public class ScheduledTaskQueueFactory {
    private final String queueTableName;
    private final String idSequenceName;
    private final QueueIdMapper queueIdMapper;
    private final QueueSettings defaultQueueSettings;
    private final ScheduledTaskQueueDao scheduledTaskQueueDao;
    private final QueueShardRouter<String, ? extends DatabaseAccessLayer> queueShardRouter;
    private final ScheduledTaskLifecycleListener scheduledTaskLifecycleListener;

    public ScheduledTaskQueueFactory(@Nonnull String queueTableName,
                                     @Nullable String idSequenceName,
                                     @Nonnull QueueIdMapper queueIdMapper,
                                     @Nonnull QueueSettings defaultQueueSettings,
                                     @Nonnull ScheduledTaskQueueDao scheduledTaskQueueDao,
                                     @Nonnull QueueShardRouter<String, ? extends DatabaseAccessLayer> queueShardRouter,
                                     @Nonnull ScheduledTaskLifecycleListener scheduledTaskLifecycleListener) {
        this.queueTableName = requireNonNull(queueTableName, "queueTableName");
        this.idSequenceName = idSequenceName;
        this.queueIdMapper = requireNonNull(queueIdMapper, "queueIdMapper");
        this.defaultQueueSettings = requireNonNull(defaultQueueSettings, "defaultQueueSettings");
        this.scheduledTaskQueueDao = requireNonNull(scheduledTaskQueueDao, "scheduledTaskQueueDao");
        this.queueShardRouter = requireNonNull(queueShardRouter, "queueShardRouter");
        this.scheduledTaskLifecycleListener = requireNonNull(scheduledTaskLifecycleListener, "scheduledTaskLifecycleListener");
    }

    /**
     * Create a {@link ScheduledTaskQueue} according to passed {@code scheduledTaskDefinition}
     *
     * @param scheduledTaskDefinition definition of a scheduled task
     * @return created scheduled task queue
     */
    public ScheduledTaskQueue createScheduledTasksQueue(@Nonnull ScheduledTaskDefinition scheduledTaskDefinition) {
        requireNonNull(scheduledTaskDefinition, "scheduledTaskDefinition");
        QueueConfig queueConfig = createQueueConfig(scheduledTaskDefinition);

        QueueConsumer<String> queueConsumer = createQueueConsumer(queueConfig, scheduledTaskDefinition);
        QueueProducer<String> queueProducer = createQueueProducer(queueConfig);

        return new ScheduledTaskQueue(queueConfig, queueConsumer, queueProducer, scheduledTaskQueueDao, scheduledTaskDefinition);
    }

    private QueueConfig createQueueConfig(ScheduledTaskDefinition scheduledTaskDefinition) {
        return new QueueConfig(
                QueueLocation.builder()
                        .withQueueId(queueIdMapper.toQueueId(scheduledTaskDefinition.getIdentity()))
                        .withTableName(queueTableName)
                        .withIdSequence(idSequenceName)
                        .build(),
                QueueSettings.builder()
                        .withProcessingSettings(defaultQueueSettings.getProcessingSettings())
                        .withPollSettings(defaultQueueSettings.getPollSettings())
                        .withFailureSettings(defaultQueueSettings.getFailureSettings())
                        .withReenqueueSettings(defaultQueueSettings.getReenqueueSettings())
                        .withExtSettings(defaultQueueSettings.getExtSettings())
                        .build()
        );
    }

    private QueueConsumer<String> createQueueConsumer(QueueConfig queueConfig, ScheduledTaskDefinition scheduledTaskDefinition) {
        return new ScheduledTaskQueueConsumer(queueConfig, scheduledTaskDefinition, scheduledTaskLifecycleListener,
                scheduledTaskQueueDao);
    }

    private QueueProducer<String> createQueueProducer(QueueConfig queueConfig) {
        return new ShardingQueueProducer<>(queueConfig, NoopPayloadTransformer.getInstance(), queueShardRouter);
    }
}

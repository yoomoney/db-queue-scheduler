package ru.yoomoney.tech.dbqueue.scheduler.internal;

import ru.yoomoney.tech.dbqueue.api.impl.SingleQueueShardRouter;
import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.config.QueueService;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.config.impl.NoopTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.config.impl.NoopThreadLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.config.ScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.internal.db.ScheduledTaskQueueDao;
import ru.yoomoney.tech.dbqueue.scheduler.internal.queue.QueueIdMapper;
import ru.yoomoney.tech.dbqueue.scheduler.internal.queue.ScheduledTaskQueueFactory;
import ru.yoomoney.tech.dbqueue.settings.ExtSettings;
import ru.yoomoney.tech.dbqueue.settings.FailRetryType;
import ru.yoomoney.tech.dbqueue.settings.FailureSettings;
import ru.yoomoney.tech.dbqueue.settings.PollSettings;
import ru.yoomoney.tech.dbqueue.settings.ProcessingMode;
import ru.yoomoney.tech.dbqueue.settings.ProcessingSettings;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueRetryType;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueSettings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collections;

import static java.util.Objects.requireNonNull;

/**
 * Configurator of internal {@link ScheduledTaskManager} - main facade that manages scheduled tasks
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
public class ScheduledTaskManagerBuilder {
    private static final QueueShardId DEFAULT_DB_QUEUE_SHARD_ID = new QueueShardId("db-queue-scheduler");
    private static final Duration DEFAULT_DB_QUEUE_FETCH_TASK_TIMEOUT = Duration.ofSeconds(1L);
    private static final Duration DEFAULT_DB_QUEUE_TIMEOUT_AFTER_FAILURE = Duration.ofMinutes(1L);

    private String tableName;
    private String idSequenceName;
    private DatabaseAccessLayer databaseAccessLayer;
    private ScheduledTaskQueueDao scheduledTaskQueueDao;
    private ScheduledTaskLifecycleListener scheduledTaskLifecycleListener;

    /**
     * Sets backed table name for storing scheduled tasks
     *
     * @param tableName table name that stores scheduled tasks
     * @return the same instance of {@link ScheduledTaskManagerBuilder}
     */
    public ScheduledTaskManagerBuilder withTableName(@Nonnull String tableName) {
        this.tableName = requireNonNull(tableName, "tableName");
        return this;
    }

    /**
     * Sets sequence name for generating primary key of tasks table.
     *
     * @param idSequenceName sequence name for generating primary key of tasks table.
     * @return the same instance of {@link ScheduledTaskManagerBuilder}
     */
    public ScheduledTaskManagerBuilder withIdSequenceName(@Nullable String idSequenceName) {
        this.idSequenceName = idSequenceName;
        return this;
    }

    /**
     * Sets {@link DatabaseAccessLayer} for db-queue configuring
     *
     * @param databaseAccessLayer layer for interacting with a database
     * @return the same instance of {@link ScheduledTaskManagerBuilder}
     */
    public ScheduledTaskManagerBuilder withDatabaseAccessLayer(@Nonnull DatabaseAccessLayer databaseAccessLayer) {
        this.databaseAccessLayer = requireNonNull(databaseAccessLayer, "databaseAccessLayer");
        return this;
    }

    /**
     * Sets {@link ScheduledTaskQueueDao} for direct access to db-queue tables
     *
     * @param scheduledTaskQueueDao DAO that implements direct access to {@link #tableName}
     * @return the same instance of {@link ScheduledTaskManagerBuilder}
     */
    public ScheduledTaskManagerBuilder withScheduledTaskQueueDao(@Nonnull ScheduledTaskQueueDao scheduledTaskQueueDao) {
        this.scheduledTaskQueueDao = requireNonNull(scheduledTaskQueueDao, "scheduledTaskQueueDao");
        return this;
    }

    /**
     * Sets {@link ScheduledTaskLifecycleListener} for observing task execution
     *
     * @param scheduledTaskLifecycleListener listener of scheduled task lifecycles
     * @return the same instance of {@link ScheduledTaskManagerBuilder}
     */
    public ScheduledTaskManagerBuilder withScheduledTaskLifecycleListener(
            @Nonnull ScheduledTaskLifecycleListener scheduledTaskLifecycleListener
    ) {
        this.scheduledTaskLifecycleListener = requireNonNull(scheduledTaskLifecycleListener, "scheduledTaskLifecycleListener");
        return this;
    }

    /**
     * Builds {@link ScheduledTaskManager} according to set properties
     *
     * @return configured instance of {@link ScheduledTaskManager}
     */
    public ScheduledTaskManager build() {
        requireNonNull(tableName, "tableName");
        requireNonNull(databaseAccessLayer, "databaseAccessLayer");
        requireNonNull(scheduledTaskQueueDao, "scheduledTaskQueueDao");
        requireNonNull(scheduledTaskLifecycleListener, "scheduledTaskLifecycleListener");

        QueueShard<?> singleQueueShard = new QueueShard<>(DEFAULT_DB_QUEUE_SHARD_ID, databaseAccessLayer);
        QueueSettings defaultQueueSettings = buildDefaultQueueSettings();

        QueueIdMapper queueIdMapper = new QueueIdMapper();
        QueueService queueService = new QueueService(
                Collections.singletonList(singleQueueShard),
                NoopThreadLifecycleListener.getInstance(),
                NoopTaskLifecycleListener.getInstance()
        );
        ScheduledTaskQueueFactory scheduledTaskQueueFactory = new ScheduledTaskQueueFactory(
                tableName,
                idSequenceName,
                queueIdMapper,
                defaultQueueSettings,
                scheduledTaskQueueDao,
                new SingleQueueShardRouter<>(singleQueueShard),
                scheduledTaskLifecycleListener
        );

        return new ScheduledTaskManager(queueService, queueIdMapper, scheduledTaskQueueDao, scheduledTaskQueueFactory);
    }

    private QueueSettings buildDefaultQueueSettings() {
        return QueueSettings.builder()
                .withProcessingSettings(ProcessingSettings.builder()
                        .withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS)
                        .withThreadCount(1)
                        .build()
                )
                .withPollSettings(PollSettings.builder()
                        .withBetweenTaskTimeout(DEFAULT_DB_QUEUE_FETCH_TASK_TIMEOUT)
                        .withNoTaskTimeout(DEFAULT_DB_QUEUE_FETCH_TASK_TIMEOUT)
                        .withFatalCrashTimeout(DEFAULT_DB_QUEUE_FETCH_TASK_TIMEOUT)
                        .build()
                )
                .withFailureSettings(FailureSettings.builder()
                        .withRetryType(FailRetryType.LINEAR_BACKOFF)
                        .withRetryInterval(DEFAULT_DB_QUEUE_TIMEOUT_AFTER_FAILURE)
                        .build()
                )
                .withReenqueueSettings(ReenqueueSettings.builder()
                        .withRetryType(ReenqueueRetryType.MANUAL)
                        .build()
                )
                .withExtSettings(ExtSettings.builder().withSettings(Collections.emptyMap()).build())
                .build();
    }
}

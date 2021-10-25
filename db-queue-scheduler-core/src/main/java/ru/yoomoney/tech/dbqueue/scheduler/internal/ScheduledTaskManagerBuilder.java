package ru.yoomoney.tech.dbqueue.scheduler.internal;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yoomoney.tech.dbqueue.api.impl.SingleQueueShardRouter;
import ru.yoomoney.tech.dbqueue.config.QueueService;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.config.impl.NoopTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.config.impl.NoopThreadLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.scheduler.internal.db.ScheduledTaskQueueDao;
import ru.yoomoney.tech.dbqueue.scheduler.internal.db.spring.ScheduledTaskQueuePostgresDao;
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
import ru.yoomoney.tech.dbqueue.spring.dao.SpringDatabaseAccessLayer;

import javax.annotation.Nonnull;
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
    private static final Duration DEFAULT_DB_QUEUE_TIMEOUT_AFTER_FAILURE = Duration.ofSeconds(1L);

    private final String tableName;
    private final DatabaseDialect databaseDialect;
    private SpringDatabaseAccessLayer databaseAccessLayer;
    private ScheduledTaskQueueDao scheduledTaskQueueDao;

    public ScheduledTaskManagerBuilder(@Nonnull String tableName,
                                       @Nonnull DatabaseDialect databaseDialect) {
        this.tableName = requireNonNull(tableName, "tableName");
        this.databaseDialect = requireNonNull(databaseDialect, "databaseDialect");
    }

    /**
     * Configures data access objects via spring jdbc abstractions
     */
    public ScheduledTaskManagerBuilder withSpringConfiguration(
            @Nonnull JdbcOperations jdbcOperations,
            @Nonnull TransactionOperations transactionOperations
    ) {
        requireNonNull(jdbcOperations, "jdbcOperations");
        requireNonNull(transactionOperations, "transactionOperations");

        if (databaseDialect != DatabaseDialect.POSTGRESQL) {
            throw new IllegalStateException("received unsupported dialect: databaseDialec=" + databaseDialect);
        }

        this.databaseAccessLayer = new SpringDatabaseAccessLayer(
                ru.yoomoney.tech.dbqueue.config.DatabaseDialect.POSTGRESQL,
                buildQueueTableSchema(),
                jdbcOperations,
                transactionOperations
        );
        this.scheduledTaskQueueDao = new ScheduledTaskQueuePostgresDao(
                jdbcOperations,
                transactionOperations,
                QueueTableSchema.builder().build()
        );
        return this;
    }

    /**
     * Builds {@link ScheduledTaskManager} according to set properties
     */
    public ScheduledTaskManager build() {
        QueueShard<?> singleQueueShard = new QueueShard<>(DEFAULT_DB_QUEUE_SHARD_ID, databaseAccessLayer);
        QueueSettings queueSettings = buildQueueSettings();

        QueueService queueService = new QueueService(
                Collections.singletonList(singleQueueShard),
                NoopThreadLifecycleListener.getInstance(),
                NoopTaskLifecycleListener.getInstance()
        );
        ScheduledTaskQueueFactory scheduledTaskQueueFactory = new ScheduledTaskQueueFactory(
                tableName,
                queueSettings,
                scheduledTaskQueueDao,
                new SingleQueueShardRouter<>(singleQueueShard)
        );

        return new ScheduledTaskManager(queueService, scheduledTaskQueueFactory);
    }

    private QueueTableSchema buildQueueTableSchema() {
        return QueueTableSchema.builder().build();
    }

    private QueueSettings buildQueueSettings() {
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
                        .withRetryType(FailRetryType.GEOMETRIC_BACKOFF)
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

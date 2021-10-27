package ru.yoomoney.tech.dbqueue.scheduler;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskManagerBuilder;
import ru.yoomoney.tech.dbqueue.scheduler.internal.db.ScheduledTaskQueueDao;
import ru.yoomoney.tech.dbqueue.scheduler.internal.db.SpringScheduledTaskQueuePostgresDao;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProviderFactory;
import ru.yoomoney.tech.dbqueue.spring.dao.SpringDatabaseAccessLayer;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Entry point for the library configuration via {@code spring framework}
 *
 * <p>Database configuration is required as scheduler uses it for storing scheduled tasks and guaranteeing
 * exactly-once task execution.
 *
 * <p>Example:
 *
 * <pre> {@code
 *  Scheduler scheduler = new SpringSchedulerConfigurator()
 *       .withTableName("scheduled_tasks")
 *       .withDatabaseDialect(DatabaseDialect.POSTGRESQL)
 *       .withJdbcOperations(jdbcOperations)
 *       .withTransactionOperations(transactionOperations)
 *       .build();
 *  scheduler.start();
 * }</pre>
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
public class SpringSchedulerConfigurator implements SchedulerConfigurator {
    private String tableName;
    private DatabaseDialect databaseDialect;
    private JdbcOperations jdbcOperations;
    private TransactionOperations transactionOperations;

    /**
     * Sets backed table name for storing scheduled tasks.
     *
     * <p>PostgreSQL table schema:</p>
     * <pre>{@code
     *  CREATE TABLE scheduled_tasks (
     *     id                BIGSERIAL PRIMARY KEY,
     *     queue_name        TEXT NOT NULL,
     *     payload           TEXT,
     *     created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),
     *     next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),
     *     attempt           INTEGER                  DEFAULT 0,
     *     reenqueue_attempt INTEGER                  DEFAULT 0,
     *     total_attempt     INTEGER                  DEFAULT 0
     *  );
     *  CREATE INDEX scheduled_tasks_name_time_desc_idx
     *  ON scheduled_tasks USING btree (queue_name, next_process_at, id DESC);
     *  }</pre>
     */
    public SpringSchedulerConfigurator withTableName(@Nonnull String tableName) {
        requireNonNull(tableName, "tableName");
        this.tableName = tableName;
        return this;
    }

    /**
     * Sets database dialect.
     *
     * <p>The dialect is required for querying the database properly.
     */
    public SpringSchedulerConfigurator withDatabaseDialect(@Nonnull DatabaseDialect databaseDialect) {
        requireNonNull(databaseDialect, "databaseDialect");
        this.databaseDialect = databaseDialect;
        return this;
    }

    /**
     * Set preconfigured {@link JdbcOperations} instance
     */
    public SpringSchedulerConfigurator withJdbcOperations(@Nonnull JdbcOperations jdbcOperations) {
        requireNonNull(jdbcOperations, "jdbcOperations");
        this.jdbcOperations = jdbcOperations;
        return this;
    }

    /**
     * Set preconfigured {@link TransactionOperations} instance
     */
    public SpringSchedulerConfigurator withTransactionOperations(@Nonnull TransactionOperations transactionOperations) {
        requireNonNull(transactionOperations, "transactionOperations");
        this.transactionOperations = transactionOperations;
        return this;
    }

    @Override
    public Scheduler configure() {
        requireNonNull(tableName, "tableName");
        requireNonNull(databaseDialect, "databaseDialect");
        requireNonNull(jdbcOperations, "jdbcOperations");
        requireNonNull(transactionOperations, "transactionOperations");

        if (databaseDialect != DatabaseDialect.POSTGRESQL) {
            throw new IllegalStateException("got unsupported databaseDialect: databaseDialect=" + databaseDialect);
        }

        DatabaseAccessLayer databaseAccessLayer = new SpringDatabaseAccessLayer(
                ru.yoomoney.tech.dbqueue.config.DatabaseDialect.POSTGRESQL,
                QueueTableSchema.builder().build(),
                jdbcOperations,
                transactionOperations
        );
        ScheduledTaskQueueDao scheduledTaskQueueDao = new SpringScheduledTaskQueuePostgresDao(
                jdbcOperations,
                transactionOperations,
                QueueTableSchema.builder().build()
        );
        return new DefaultScheduler(
                new ScheduledTaskManagerBuilder()
                        .withTableName(tableName)
                        .withScheduledTaskQueueDao(scheduledTaskQueueDao)
                        .withDatabaseAccessLayer(databaseAccessLayer)
                        .build(),
                new NextExecutionTimeProviderFactory()
        );
    }
}

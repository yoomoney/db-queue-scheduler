package ru.yoomoney.tech.dbqueue.scheduler;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.scheduler.config.ScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.config.impl.NoopScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskManagerBuilder;
import ru.yoomoney.tech.dbqueue.scheduler.internal.db.ScheduledTaskQueueDao;
import ru.yoomoney.tech.dbqueue.scheduler.internal.db.DefaultScheduledTaskQueueDao;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionDelayProviderFactory;
import ru.yoomoney.tech.dbqueue.spring.dao.SpringDatabaseAccessLayer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
    private String idSequenceName;
    private DatabaseDialect databaseDialect;
    private JdbcOperations jdbcOperations;
    private TransactionOperations transactionOperations;
    private ScheduledTaskLifecycleListener scheduledTaskLifecycleListener = NoopScheduledTaskLifecycleListener.getInstance();

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
     *  CREATE UNIQUE INDEX scheduled_tasks_uq ON scheduled_tasks (queue_name);
     *  }</pre>
     *
     * @param tableName table name that stores scheduled tasks
     * @return the same instance of {@link SpringSchedulerConfigurator}
     */
    public SpringSchedulerConfigurator withTableName(@Nonnull String tableName) {
        requireNonNull(tableName, "tableName");
        this.tableName = tableName;
        return this;
    }

    /**
     * Sets sequence name for generating primary key of tasks table.
     *
     * @param idSequenceName sequence name for generating primary key of tasks table.
     * @return the same instance of {@link SpringSchedulerConfigurator}
     */
    public SpringSchedulerConfigurator withIdSequenceName(@Nullable String idSequenceName) {
        this.idSequenceName = idSequenceName;
        return this;
    }

    /**
     * Sets database dialect.
     *
     * <p>The dialect is required for querying the database properly.
     *
     * @param databaseDialect type of using database
     * @return the same instance of {@link SpringSchedulerConfigurator}
     */
    public SpringSchedulerConfigurator withDatabaseDialect(@Nonnull DatabaseDialect databaseDialect) {
        requireNonNull(databaseDialect, "databaseDialect");
        this.databaseDialect = databaseDialect;
        return this;
    }

    /**
     * Set preconfigured {@link JdbcOperations} instance
     *
     * @param jdbcOperations preconfigured {@link JdbcOperations}
     * @return the same instance of {@link SpringSchedulerConfigurator}
     */
    public SpringSchedulerConfigurator withJdbcOperations(@Nonnull JdbcOperations jdbcOperations) {
        requireNonNull(jdbcOperations, "jdbcOperations");
        this.jdbcOperations = jdbcOperations;
        return this;
    }

    /**
     * Set preconfigured {@link TransactionOperations} instance
     *
     * @param transactionOperations preconfigured {@link TransactionOperations}
     * @return the same instance of {@link SpringSchedulerConfigurator}
     */
    public SpringSchedulerConfigurator withTransactionOperations(@Nonnull TransactionOperations transactionOperations) {
        requireNonNull(transactionOperations, "transactionOperations");
        this.transactionOperations = transactionOperations;
        return this;
    }

    /**
     * Sets {@link ScheduledTaskLifecycleListener} for observing task execution
     *
     * @param scheduledTaskLifecycleListener listener of scheduled task lifecycles
     * @return the same instance of {@link SpringSchedulerConfigurator}
     */
    public SpringSchedulerConfigurator withScheduledTaskLifecycleListener(
            @Nonnull ScheduledTaskLifecycleListener scheduledTaskLifecycleListener
    ) {
        this.scheduledTaskLifecycleListener = requireNonNull(scheduledTaskLifecycleListener, "scheduledTaskLifecycleListener");
        return this;
    }

    @Override
    public Scheduler configure() {
        requireNonNull(tableName, "tableName");
        requireNonNull(databaseDialect, "databaseDialect");
        requireNonNull(jdbcOperations, "jdbcOperations");
        requireNonNull(transactionOperations, "transactionOperations");
        requireNonNull(scheduledTaskLifecycleListener, "scheduledTaskLifecycleListener");

        DatabaseAccessLayer databaseAccessLayer = new SpringDatabaseAccessLayer(
                mapDatabaseDialect(databaseDialect),
                QueueTableSchema.builder().build(),
                jdbcOperations,
                transactionOperations
        );
        ScheduledTaskQueueDao scheduledTaskQueueDao = new DefaultScheduledTaskQueueDao(
                tableName,
                databaseDialect,
                jdbcOperations,
                transactionOperations,
                QueueTableSchema.builder().build()
        );
        return new DefaultScheduler(
                new ScheduledTaskManagerBuilder()
                        .withTableName(tableName)
                        .withIdSequenceName(idSequenceName)
                        .withScheduledTaskQueueDao(scheduledTaskQueueDao)
                        .withDatabaseAccessLayer(databaseAccessLayer)
                        .withScheduledTaskLifecycleListener(scheduledTaskLifecycleListener)
                        .build(),
                new NextExecutionDelayProviderFactory()
        );
    }

    private ru.yoomoney.tech.dbqueue.config.DatabaseDialect mapDatabaseDialect(DatabaseDialect databaseDialect) {
        switch (databaseDialect) {
            case POSTGRESQL:
                return ru.yoomoney.tech.dbqueue.config.DatabaseDialect.POSTGRESQL;
            case H2:
                return ru.yoomoney.tech.dbqueue.config.DatabaseDialect.H2;
            case ORACLE_11G:
                return ru.yoomoney.tech.dbqueue.config.DatabaseDialect.ORACLE_11G;
            case MSSQL:
                return ru.yoomoney.tech.dbqueue.config.DatabaseDialect.MSSQL;
            default:
                throw new IllegalStateException("got unsupported databaseDialect: databaseDialect=" + databaseDialect);
        }
    }
}

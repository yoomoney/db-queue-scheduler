package ru.yoomoney.tech.dbqueue.scheduler;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskManagerBuilder;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProviderFactory;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Entry point for the library configuration via {@code spring framework}
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

    @Override
    public SpringSchedulerConfigurator withTableName(@Nonnull String tableName) {
        requireNonNull(tableName, "tableName");
        this.tableName = tableName;
        return this;
    }

    @Override
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

        return new ClassicScheduler(
                new ScheduledTaskManagerBuilder(tableName, databaseDialect)
                        .withSpringConfiguration(jdbcOperations, transactionOperations)
                        .build(),
                new NextExecutionTimeProviderFactory()
        );
    }
}

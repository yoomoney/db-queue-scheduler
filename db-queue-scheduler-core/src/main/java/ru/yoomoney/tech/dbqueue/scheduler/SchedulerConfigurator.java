package ru.yoomoney.tech.dbqueue.scheduler;

import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;

import javax.annotation.Nonnull;

/**
 * Entry point for the library configuration.
 *
 * <p>Database configuration is required as scheduler uses it for storing scheduled tasks
 * and guaranteeing exactly-once task execution.
 *
 * <p>Core library does not contain any implementations of the configurator. Use specific modules
 * such as {@code db-queue-scheduler-spring}, please.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
public interface SchedulerConfigurator {
    /**
     * Sets backed table name for storing scheduled tasks
     */
    SchedulerConfigurator withTableName(@Nonnull String tableName);

    /**
     * Sets database dialect.
     *
     * <p>The dialect is required for querying the database properly.
     */
    SchedulerConfigurator withDatabaseDialect(@Nonnull DatabaseDialect databaseDialect);

    /**
     * Configures {@link Scheduler}.
     * @return configured scheduler
     */
    Scheduler configure();
}

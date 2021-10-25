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
     * Sets backed table name for storing scheduled tasks.
     *
     * <p>Table schema:</p>
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

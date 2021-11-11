package ru.yoomoney.tech.dbqueue.scheduler.db;

import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;

/**
 * Postgres Database configurator for testing purposes
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 11.11.2021
 */
public class PostgresDatabaseAccessConfigurator {
    private static final String TASKS_TABLE_DDL = "" +
            "CREATE TABLE scheduled_tasks (" +
            "  id                BIGSERIAL PRIMARY KEY," +
            "  queue_name        TEXT NOT NULL," +
            "  payload           TEXT," +
            "  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now()," +
            "  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now()," +
            "  attempt           INTEGER                  DEFAULT 0," +
            "  reenqueue_attempt INTEGER                  DEFAULT 0," +
            "  total_attempt     INTEGER                  DEFAULT 0" +
            ");" +
            "CREATE UNIQUE INDEX scheduled_tasks_uq ON scheduled_tasks (queue_name);";

    public static DatabaseAccess configure(PostgreSQLContainer<?> container) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(container.getJdbcUrl());
        dataSource.setPassword(container.getPassword());
        dataSource.setUser(container.getUsername());

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        TransactionTemplate transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        transactionTemplate.executeWithoutResult(status -> jdbcTemplate.update(TASKS_TABLE_DDL));
        return new DatabaseAccess(DatabaseDialect.POSTGRESQL, jdbcTemplate, transactionTemplate);
    }
}

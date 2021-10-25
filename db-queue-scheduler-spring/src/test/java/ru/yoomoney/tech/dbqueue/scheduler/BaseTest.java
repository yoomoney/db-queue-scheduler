package ru.yoomoney.tech.dbqueue.scheduler;

import org.junit.jupiter.api.BeforeAll;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Common configuration
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
@Testcontainers
public abstract class BaseTest {
    private static final String SCHEDULED_TASKS_TABLE_DDL = "" +
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
            "CREATE INDEX scheduled_tasks_name_time_desc_idx" +
            "  ON scheduled_tasks (queue_name, next_process_at, id DESC);";

    @Container
    private static PostgreSQLContainer postgresqlContainer = new PostgreSQLContainer("postgres:9.5");

    protected static JdbcTemplate jdbcTemplate;
    protected static TransactionTemplate transactionTemplate;
    protected static AtomicLong uniqueCounter = new AtomicLong();

    @BeforeAll
    public static void configurePostgres() {
        DataSource dataSource = dataSource();
        jdbcTemplate = new JdbcTemplate(dataSource);
        transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));

        transactionTemplate.executeWithoutResult(status -> jdbcTemplate.update(SCHEDULED_TASKS_TABLE_DDL));
    }

    private static DataSource dataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgresqlContainer.getJdbcUrl());
        dataSource.setPassword(postgresqlContainer.getPassword());
        dataSource.setUser(postgresqlContainer.getUsername());
        return dataSource;
    }
}

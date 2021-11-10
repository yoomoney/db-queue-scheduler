package ru.yoomoney.tech.dbqueue.scheduler;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Common configuration
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
@Testcontainers
public abstract class BaseTest {
    private static final String POSTGRES_SCHEDULED_TASKS_TABLE_DDL = "" +
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
            "CREATE UNIQUE INDEX scheduled_tasks_name_queue_name_uq ON scheduled_tasks (queue_name);";

    private static final String H2_SCHEDULED_TASKS_TABLE_DDL = "" +
            "CREATE TABLE scheduled_tasks (" +
            "  id                BIGSERIAL PRIMARY KEY," +
            "  queue_name        VARCHAR(100) NOT NULL," +
            "  payload           VARCHAR(100)," +
            "  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now()," +
            "  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now()," +
            "  attempt           INTEGER                  DEFAULT 0," +
            "  reenqueue_attempt INTEGER                  DEFAULT 0," +
            "  total_attempt     INTEGER                  DEFAULT 0" +
            ");" +
            "CREATE UNIQUE INDEX scheduled_tasks_name_queue_name_uq ON scheduled_tasks (queue_name);";

    @Container
    private static final PostgreSQLContainer<?> postgresqlContainer = new PostgreSQLContainer<>("postgres:9.5");

    protected static DatabaseAccess postgres;
    protected static DatabaseAccess h2;
    protected static AtomicLong uniqueCounter = new AtomicLong();

    @BeforeAll
    public static void configureDatabases() {
        postgres = configurePostgres();
        h2 = configureH2();
    }

    private static DatabaseAccess configurePostgres() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgresqlContainer.getJdbcUrl());
        dataSource.setPassword(postgresqlContainer.getPassword());
        dataSource.setUser(postgresqlContainer.getUsername());

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        TransactionTemplate transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        transactionTemplate.executeWithoutResult(status -> jdbcTemplate.update(POSTGRES_SCHEDULED_TASKS_TABLE_DDL));
        return new DatabaseAccess(DatabaseDialect.POSTGRESQL, jdbcTemplate, transactionTemplate);
    }

    private static DatabaseAccess configureH2() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:~/scheduled_task_h2");
        dataSource.setUser("sa");
        dataSource.setPassword("sa");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        TransactionTemplate transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        transactionTemplate.executeWithoutResult(status -> jdbcTemplate.update("DROP ALL OBJECTS DELETE FILES"));
        transactionTemplate.executeWithoutResult(status -> jdbcTemplate.update(H2_SCHEDULED_TASKS_TABLE_DDL));
        return new DatabaseAccess(DatabaseDialect.H2, jdbcTemplate, transactionTemplate);
    }

    public static Stream<DatabaseAccess> databaseAccessStream() {
        return Stream.of(postgres, h2);
    }

    protected static class DatabaseAccess {
        private final DatabaseDialect databaseDialect;
        private final JdbcTemplate jdbcTemplate;
        private final TransactionTemplate transactionTemplate;

        DatabaseAccess(DatabaseDialect databaseDialect, JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
            this.databaseDialect = databaseDialect;
            this.jdbcTemplate = jdbcTemplate;
            this.transactionTemplate = transactionTemplate;
        }

        public DatabaseDialect getDatabaseDialect() {
            return databaseDialect;
        }

        public JdbcTemplate getJdbcTemplate() {
            return jdbcTemplate;
        }

        public TransactionTemplate getTransactionTemplate() {
            return transactionTemplate;
        }

        @Override
        public String toString() {
            return "DatabaseAccess{" +
                    "databaseDialect=" + databaseDialect +
                    '}';
        }
    }
}

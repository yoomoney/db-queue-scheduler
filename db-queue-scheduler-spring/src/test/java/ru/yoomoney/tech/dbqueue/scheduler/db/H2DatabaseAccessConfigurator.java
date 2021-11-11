package ru.yoomoney.tech.dbqueue.scheduler.db;

import org.h2.jdbcx.JdbcDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;

/**
 * H2 Database configurator for testing purposes
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 11.11.2021
 */
public class H2DatabaseAccessConfigurator {
    private static final String TASKS_TABLE_DDL = "" +
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
            "CREATE UNIQUE INDEX scheduled_tasks_uq ON scheduled_tasks (queue_name);";

    public static DatabaseAccess configure() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:~/scheduled_task_h2");
        dataSource.setUser("sa");
        dataSource.setPassword("sa");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        TransactionTemplate transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        transactionTemplate.executeWithoutResult(status -> jdbcTemplate.update("DROP ALL OBJECTS DELETE FILES"));
        transactionTemplate.executeWithoutResult(status -> jdbcTemplate.update(TASKS_TABLE_DDL));
        return new DatabaseAccess(DatabaseDialect.H2, jdbcTemplate, transactionTemplate);
    }
}

package ru.yoomoney.tech.dbqueue.scheduler.db;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MSSQLServerContainer;
import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;

/**
 * MS SQL Database configurator for testing purposes
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 11.11.2021
 */
public class MsSqlDatabaseAccessConfigurator {
    private static final String TASKS_TABLE_DDL = "" +
            "CREATE TABLE scheduled_tasks (" +
            "  id                INT IDENTITY(1,1) NOT NULL," +
            "  queue_name        VARCHAR(100) NOT NULL," +
            "  payload           TEXT," +
            "  created_at        DATETIMEOFFSET NOT NULL  DEFAULT SYSDATETIMEOFFSET()," +
            "  next_process_at   DATETIMEOFFSET NOT NULL  DEFAULT SYSDATETIMEOFFSET()," +
            "  attempt           INTEGER NOT NULL         DEFAULT 0," +
            "  reenqueue_attempt INTEGER NOT NULL         DEFAULT 0," +
            "  total_attempt     INTEGER NOT NULL         DEFAULT 0," +
            "  PRIMARY KEY (id)" +
            ')';
    private static final String INDEX_DDL = "CREATE UNIQUE INDEX scheduled_tasks_uq ON scheduled_tasks (queue_name)";

    public static DatabaseAccess configure(MSSQLServerContainer<?> container) {
        SQLServerDataSource dataSource = new SQLServerDataSource();
        dataSource.setServerName(container.getHost());
        dataSource.setPortNumber(container.getFirstMappedPort());
        dataSource.setPassword(container.getPassword());
        dataSource.setUser(container.getUsername());

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        TransactionTemplate transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        transactionTemplate.executeWithoutResult(status -> jdbcTemplate.update(TASKS_TABLE_DDL));
        transactionTemplate.executeWithoutResult(status -> jdbcTemplate.update(INDEX_DDL));
        return new DatabaseAccess(DatabaseDialect.MSSQL, jdbcTemplate, transactionTemplate);
    }
}

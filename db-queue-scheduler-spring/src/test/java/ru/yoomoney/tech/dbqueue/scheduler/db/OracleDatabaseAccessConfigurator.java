package ru.yoomoney.tech.dbqueue.scheduler.db;

import oracle.jdbc.pool.OracleConnectionPoolDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.OracleContainer;
import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;

import java.sql.SQLException;

/**
 * Oracle Database configurator for testing purposes
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 11.11.2021
 */
public class OracleDatabaseAccessConfigurator {
    private static final String TASKS_TABLE_DDL = "" +
            "CREATE TABLE scheduled_tasks (" +
            "  id                NUMBER(38) NOT NULL PRIMARY KEY," +
            "  queue_name        VARCHAR2(128) NOT NULL," +
            "  payload           CLOB," +
            "  created_at        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP," +
            "  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP," +
            "  attempt           NUMBER(38)                  DEFAULT 0," +
            "  reenqueue_attempt NUMBER(38)                  DEFAULT 0," +
            "  total_attempt     NUMBER(38)                  DEFAULT 0" +
            ')';
    private static final String INDEX_DDL = "CREATE UNIQUE INDEX scheduled_tasks_uq ON scheduled_tasks (queue_name)";
    private static final String SEQUENCE_DDL = "CREATE SEQUENCE scheduled_tasks_seq";

    public static DatabaseAccess configure(OracleContainer container) {
        OracleConnectionPoolDataSource dataSource;
        try {
            dataSource = new OracleConnectionPoolDataSource();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        dataSource.setURL("jdbc:oracle:thin:" + container.getUsername() + "/" + container.getUsername() + "@localhost:"
                + container.getFirstMappedPort() + ":xe");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        TransactionTemplate transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        transactionTemplate.executeWithoutResult(status -> jdbcTemplate.update(TASKS_TABLE_DDL));
        transactionTemplate.executeWithoutResult(status -> jdbcTemplate.update(INDEX_DDL));
        transactionTemplate.executeWithoutResult(status -> jdbcTemplate.update(SEQUENCE_DDL));
        return new DatabaseAccess(DatabaseDialect.ORACLE_11G, jdbcTemplate, transactionTemplate);
    }
}

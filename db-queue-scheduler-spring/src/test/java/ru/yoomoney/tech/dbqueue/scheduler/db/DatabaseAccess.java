package ru.yoomoney.tech.dbqueue.scheduler.db;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;

/**
 * Facade encapsulates objects to work with a database
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 11.11.2021
 */
public class DatabaseAccess {
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

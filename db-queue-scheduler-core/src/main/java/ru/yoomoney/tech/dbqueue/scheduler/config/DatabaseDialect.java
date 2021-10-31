package ru.yoomoney.tech.dbqueue.scheduler.config;

/**
 * Supported database type (dialect)
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
public enum DatabaseDialect {
    /**
     * PostgreSQL (version equals or higher than 9.5).
     */
    POSTGRESQL,
    /**
     * H2 in-memory database
     */
    H2
}

package ru.yoomoney.tech.dbqueue.scheduler;

import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import ru.yoomoney.tech.dbqueue.scheduler.db.DatabaseAccess;
import ru.yoomoney.tech.dbqueue.scheduler.db.H2DatabaseAccessConfigurator;
import ru.yoomoney.tech.dbqueue.scheduler.db.MsSqlDatabaseAccessConfigurator;
import ru.yoomoney.tech.dbqueue.scheduler.db.OracleDatabaseAccessConfigurator;
import ru.yoomoney.tech.dbqueue.scheduler.db.PostgresDatabaseAccessConfigurator;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Common configuration
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
public abstract class BaseTest {
    private static final MSSQLServerContainer<?> mssqlServerContainer;
    private static final PostgreSQLContainer<?> postgresqlContainer;
    private static final OracleContainer oracleContainer;

    protected static DatabaseAccess postgres;
    protected static DatabaseAccess oracle;
    protected static DatabaseAccess mssql;
    protected static DatabaseAccess h2;

    protected static AtomicLong uniqueCounter = new AtomicLong();

    static {
        //noinspection resource
        mssqlServerContainer = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2019-CU1-ubuntu-16.04").acceptLicense();
        postgresqlContainer = new PostgreSQLContainer<>("postgres:9.5");
        oracleContainer = new OracleContainer("gvenzl/oracle-xe:11.2.0.2-slim");

        mssqlServerContainer.start();
        postgresqlContainer.start();
        oracleContainer.start();

        postgres = PostgresDatabaseAccessConfigurator.configure(postgresqlContainer);
        oracle = OracleDatabaseAccessConfigurator.configure(oracleContainer);
        mssql = MsSqlDatabaseAccessConfigurator.configure(mssqlServerContainer);
        h2 = H2DatabaseAccessConfigurator.configure();
    }

    public static Stream<DatabaseAccess> databaseAccessStream() {
        return Stream.of(postgres, oracle, mssql, h2);
    }
}

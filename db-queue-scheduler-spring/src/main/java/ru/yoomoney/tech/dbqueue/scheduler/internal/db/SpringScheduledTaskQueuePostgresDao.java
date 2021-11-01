package ru.yoomoney.tech.dbqueue.scheduler.internal.db;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionOperations;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Spring implementation of {@link ScheduledTaskQueueDao}.
 *
 * Spring is not connected to the library by default - use {@code db-queue-scheduler-spring} module.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
public class SpringScheduledTaskQueuePostgresDao implements ScheduledTaskQueueDao {

    private final String tableName;
    private final QueueTableSchema queueTableSchema;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public SpringScheduledTaskQueuePostgresDao(@Nonnull String tableName,
                                               @Nonnull JdbcOperations jdbcOperations,
                                               @Nonnull TransactionOperations transactionOperations,
                                               @Nonnull QueueTableSchema queueTableSchema) {
        requireNonNull(tableName, "tableName");
        requireNonNull(jdbcOperations, "jdbcOperations");
        requireNonNull(transactionOperations, "transactionOperations");
        requireNonNull(queueTableSchema, "queueTableSchema");

        this.queueTableSchema = queueTableSchema;
        this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcOperations);
        this.tableName = tableName;
    }

    @Override
    public boolean isQueueEmpty(@Nonnull QueueLocation queueLocation) {
        requireNonNull(queueLocation, "queueLocation");

        String isQueueEmptyQuery = String.format(
                "select count(1) from %s where %s = :queueName",
                queueLocation.getTableName(),
                queueTableSchema.getQueueNameField()

        );
        Long taskCount = namedParameterJdbcTemplate.queryForObject(
                isQueueEmptyQuery,
                Map.of("queueName", queueLocation.getQueueId().asString()),
                Long.class
        );
        return taskCount == null || taskCount.equals(0L);
    }

    @Override
    public List<ScheduledTaskRecord> findAll() {
        String findAllQuery = ' ' +
                "select " + queueTableSchema.getIdField() + " as id" +
                "     , " + queueTableSchema.getQueueNameField() + " as queue_name" +
                "     , " + queueTableSchema.getNextProcessAtField() + " as next_process_at" +
                "  from " + tableName;

        return namedParameterJdbcTemplate.query(
                findAllQuery,
                (rs, index) -> ScheduledTaskRecord.builder()
                        .withId(rs.getLong("id"))
                        .withQueueName(rs.getString("queue_name"))
                        .withNextProcessAt(rs.getTimestamp("next_process_at").toInstant())
                        .build()
        );
    }
}

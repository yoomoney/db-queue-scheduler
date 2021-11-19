package ru.yoomoney.tech.dbqueue.scheduler.internal.db;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionOperations;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import static java.util.Objects.requireNonNull;

/**
 * Spring implementation of {@link ScheduledTaskQueueDao}.
 *
 * Spring is not connected to the library by default - use {@code db-queue-scheduler-spring} module.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
public class DefaultScheduledTaskQueueDao implements ScheduledTaskQueueDao {

    private final String tableName;
    private final DatabaseDialect databaseDialect;
    private final QueueTableSchema queueTableSchema;
    private final TransactionOperations transactionOperations;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public DefaultScheduledTaskQueueDao(@Nonnull String tableName,
                                        @Nonnull DatabaseDialect databaseDialect,
                                        @Nonnull JdbcOperations jdbcOperations,
                                        @Nonnull TransactionOperations transactionOperations,
                                        @Nonnull QueueTableSchema queueTableSchema) {
        requireNonNull(tableName, "tableName");
        requireNonNull(databaseDialect, "databaseDialect");
        requireNonNull(jdbcOperations, "jdbcOperations");
        requireNonNull(transactionOperations, "transactionOperations");
        requireNonNull(queueTableSchema, "queueTableSchema");

        this.queueTableSchema = queueTableSchema;
        this.databaseDialect = databaseDialect;
        this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcOperations);
        this.transactionOperations = transactionOperations;
        this.tableName = tableName;
    }

    @Override
    public Optional<ScheduledTaskRecord> findQueueTask(@Nonnull QueueId queueId) {
        requireNonNull(queueId, "queueId");

        String findQueueTaskQuery = ' ' +
                "select " + queueTableSchema.getIdField() + " as id" +
                "     , " + queueTableSchema.getQueueNameField() + " as queue_name" +
                "     , " + queueTableSchema.getNextProcessAtField() + " as next_process_at" +
                "  from " + tableName +
                " where " + queueTableSchema.getQueueNameField() + " = :queueName";

        return namedParameterJdbcTemplate.query(
                findQueueTaskQuery,
                Map.of("queueName", queueId.asString()),
                (rs, index) -> ScheduledTaskRecord.builder()
                        .withId(rs.getLong("id"))
                        .withQueueName(rs.getString("queue_name"))
                        .withNextProcessAt(rs.getTimestamp("next_process_at").toInstant())
                        .build()
        ).stream().findFirst();
    }

    @Override
    public int updateNextProcessDate(@Nonnull QueueId queueId, @Nonnull Instant nextProcessDate) {
        requireNonNull(queueId, "queueId");
        requireNonNull(nextProcessDate, "nextProcessDate");

        String rescheduleQuery = String.format(
                "update %s set %s = :nextProcessDate where %s = :queueName",
                tableName,
                queueTableSchema.getNextProcessAtField(),
                queueTableSchema.getQueueNameField()
        );
        Calendar calendar;
        if (databaseDialect == DatabaseDialect.MSSQL) {
            // https://github.com/Microsoft/mssql-jdbc/issues/758
            calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            calendar.setTime(Timestamp.from(nextProcessDate));
        } else {
            // on the other hand oracle and h2 can't process calendar properly
            calendar = Calendar.getInstance();
            calendar.setTime(Timestamp.from(nextProcessDate));
        }
        Integer updatedRows = transactionOperations.execute(status -> namedParameterJdbcTemplate.update(
                rescheduleQuery,
                Map.<String, Object>of("queueName", queueId.asString(), "nextProcessDate", calendar)
        ));
        return updatedRows == null ? 0 : updatedRows;
    }

    @Override
    public int updatePayload(@Nonnull QueueId queueId, @Nullable String payload) {
        requireNonNull(queueId, "queueId");

        String updatePayloadQuery = String.format(
                "update %s set %s = :payload where %s = :queueName",
                tableName,
                queueTableSchema.getPayloadField(),
                queueTableSchema.getQueueNameField()
        );
        Map<String, Object> params = new HashMap<>();
        params.put("queueName", queueId.asString());
        params.put("payload", payload);
        Integer updatedRows = transactionOperations.execute(status -> namedParameterJdbcTemplate.update(updatePayloadQuery,
                params));
        return updatedRows == null ? 0 : updatedRows;
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

package ru.yoomoney.tech.dbqueue.scheduler.internal.db;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionOperations;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.Date;
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
    private final TransactionOperations transactionOperations;
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
        this.transactionOperations = transactionOperations;
        this.tableName = tableName;
    }

    @Override
    public List<ScheduledTaskRecord> findQueueTasks(@Nonnull QueueId queueId) {
        String findQueueTasksQuery = ' ' +
                "select " + queueTableSchema.getIdField() + " as id" +
                "     , " + queueTableSchema.getQueueNameField() + " as queue_name" +
                "     , " + queueTableSchema.getNextProcessAtField() + " as next_process_at" +
                "  from " + tableName +
                " where " + queueTableSchema.getQueueNameField() + " = :queue_name";

        return namedParameterJdbcTemplate.query(
                findQueueTasksQuery,
                Map.of("queue_name", queueId.asString()),
                (rs, index) -> ScheduledTaskRecord.builder()
                        .withId(rs.getLong("id"))
                        .withQueueName(rs.getString("queue_name"))
                        .withNextProcessAt(rs.getTimestamp("next_process_at").toInstant())
                        .build()
        );
    }

    @Override
    public void deleteQueueTasks(@Nonnull QueueId queueId, List<Long> taskIds) {
        String deleteTasksQuery = ' ' +
                "delete from " + tableName +
                " where " + queueTableSchema.getQueueNameField() + " = :queue_name" +
                "   and " + queueTableSchema.getIdField() + " in (:task_ids)";

        transactionOperations.executeWithoutResult(status -> namedParameterJdbcTemplate.update(
                deleteTasksQuery,
                Map.of("queue_name", queueId.asString(), "task_ids", taskIds)
        ));
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
        Integer updatedRows = transactionOperations.execute(status -> namedParameterJdbcTemplate.update(
                rescheduleQuery,
                Map.<String, Object>of("queueName", queueId.asString(), "nextProcessDate", Date.from(nextProcessDate))
        ));
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

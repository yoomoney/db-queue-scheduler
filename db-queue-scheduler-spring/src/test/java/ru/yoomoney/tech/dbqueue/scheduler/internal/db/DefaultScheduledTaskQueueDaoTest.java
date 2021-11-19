package ru.yoomoney.tech.dbqueue.scheduler.internal.db;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.scheduler.BaseTest;
import ru.yoomoney.tech.dbqueue.scheduler.db.DatabaseAccess;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.spring.dao.SpringDatabaseAccessLayer;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 01.11.2021
 */
class DefaultScheduledTaskQueueDaoTest extends BaseTest {
    @ParameterizedTest
    @MethodSource("databaseAccessStream")
    void findQueueTask_should_return_empty_when_no_queue_tasks(DatabaseAccess databaseAccess) {
        // given
        ScheduledTaskQueueDao scheduledTaskQueueDao = scheduledTaskQueueDao(databaseAccess);
        DatabaseAccessLayer databaseAccessLayer = databaseAccessLayer(databaseAccess);
        QueueLocation location1 = queueLocation(databaseAccess, "queue-" + uniqueCounter.incrementAndGet());
        QueueLocation location2 = queueLocation(databaseAccess, "queue-" + uniqueCounter.incrementAndGet());

        // when
        databaseAccessLayer.getQueueDao().enqueue(location2, EnqueueParams.create(""));

        // then
        assertThat(scheduledTaskQueueDao.findQueueTask(location1.getQueueId()).isEmpty(), equalTo(true));
    }


    @ParameterizedTest
    @MethodSource("databaseAccessStream")
    void findQueueTask_should_return_task_when_queue_tasks_exist(DatabaseAccess databaseAccess) {
        // given
        ScheduledTaskQueueDao scheduledTaskQueueDao = scheduledTaskQueueDao(databaseAccess);
        DatabaseAccessLayer databaseAccessLayer = databaseAccessLayer(databaseAccess);
        QueueLocation location = queueLocation(databaseAccess, "queue-" + uniqueCounter.incrementAndGet());

        // when
        long taskId = databaseAccessLayer.getQueueDao().enqueue(location, EnqueueParams.create(""));

        // then
        Optional<ScheduledTaskRecord> queueTask = scheduledTaskQueueDao.findQueueTask(location.getQueueId());
        assertThat(queueTask.isPresent(), equalTo(true));
        assertThat(queueTask.orElseThrow().getId(), equalTo(taskId));
        assertThat(queueTask.orElseThrow().getQueueName(), equalTo(location.getQueueId().asString()));
        assertThat(queueTask.orElseThrow().getNextProcessAt(), notNullValue());
    }

    @ParameterizedTest
    @MethodSource("databaseAccessStream")
    void should_update_next_processing_date(DatabaseAccess databaseAccess) {
        // given
        ScheduledTaskQueueDao scheduledTaskQueueDao = scheduledTaskQueueDao(databaseAccess);
        DatabaseAccessLayer databaseAccessLayer = databaseAccessLayer(databaseAccess);
        QueueLocation location1 = queueLocation(databaseAccess, "queue-" + uniqueCounter.incrementAndGet());
        QueueLocation location2 = queueLocation(databaseAccess, "queue-" + uniqueCounter.incrementAndGet());
        Instant nextProcessingDate = LocalDateTime.of(2010, 1, 1, 0, 0).toInstant(ZoneOffset.UTC);

        // when
        databaseAccessLayer.getQueueDao().enqueue(location1, EnqueueParams.create(""));
        databaseAccessLayer.getQueueDao().enqueue(location2, EnqueueParams.create(""));
        scheduledTaskQueueDao.updateNextProcessDate(location1.getQueueId(), nextProcessingDate);

        // then
        ScheduledTaskRecord location1Record = scheduledTaskQueueDao.findAll().stream()
                .filter(it -> it.getQueueName().equals(location1.getQueueId().asString()))
                .findFirst()
                .orElseThrow();
        ScheduledTaskRecord location2Record = scheduledTaskQueueDao.findAll().stream()
                .filter(it -> it.getQueueName().equals(location2.getQueueId().asString()))
                .findFirst()
                .orElseThrow();
        assertThat(location1Record.getNextProcessAt(), equalTo(nextProcessingDate));
        assertThat(location2Record.getNextProcessAt(), not(equalTo(nextProcessingDate)));
    }

    @ParameterizedTest
    @MethodSource("databaseAccessStream")
    void should_update_payload(DatabaseAccess databaseAccess) {
        // given
        ScheduledTaskQueueDao scheduledTaskQueueDao = scheduledTaskQueueDao(databaseAccess);
        DatabaseAccessLayer databaseAccessLayer = databaseAccessLayer(databaseAccess);
        QueueLocation location1 = queueLocation(databaseAccess, "queue-" + uniqueCounter.incrementAndGet());
        QueueLocation location2 = queueLocation(databaseAccess, "queue-" + uniqueCounter.incrementAndGet());
        QueueLocation location3 = queueLocation(databaseAccess, "queue-" + uniqueCounter.incrementAndGet());

        // when
        databaseAccessLayer.getQueueDao().enqueue(location1, EnqueueParams.create("payload_1"));
        databaseAccessLayer.getQueueDao().enqueue(location2, EnqueueParams.create("payload_2"));
        databaseAccessLayer.getQueueDao().enqueue(location3, EnqueueParams.create("payload_2"));
        scheduledTaskQueueDao.updatePayload(location1.getQueueId(), "new_payload");
        scheduledTaskQueueDao.updatePayload(location3.getQueueId(), null);

        // then
        String payload1 = databaseAccess.getJdbcTemplate().queryForObject(
                "select payload from scheduled_tasks where queue_name=?",
                String.class,
                location1.getQueueId().asString()
        );
        String payload2 = databaseAccess.getJdbcTemplate().queryForObject(
                "select payload from scheduled_tasks where queue_name=?",
                String.class,
                location2.getQueueId().asString()
        );
        String payload3 = databaseAccess.getJdbcTemplate().queryForObject(
                "select payload from scheduled_tasks where queue_name=?",
                String.class,
                location3.getQueueId().asString()
        );
        assertThat(payload1, equalTo("new_payload"));
        assertThat(payload2, equalTo("payload_2"));
        assertThat(payload3, nullValue());
    }

    @ParameterizedTest
    @MethodSource("databaseAccessStream")
    void should_find_all_tasks(DatabaseAccess databaseAccess) {
        // given
        ScheduledTaskQueueDao scheduledTaskQueueDao = scheduledTaskQueueDao(databaseAccess);
        DatabaseAccessLayer databaseAccessLayer = databaseAccessLayer(databaseAccess);
        QueueLocation location1 = queueLocation(databaseAccess, "queue-" + uniqueCounter.incrementAndGet());
        QueueLocation location2 = queueLocation(databaseAccess, "queue-" + uniqueCounter.incrementAndGet());

        // when
        long taskId1 = databaseAccessLayer.getQueueDao().enqueue(location1, EnqueueParams.create(""));
        long taskId2 = databaseAccessLayer.getQueueDao().enqueue(location2, EnqueueParams.create(""));
        Map<Long, ScheduledTaskRecord> scheduledTasks = scheduledTaskQueueDao.findAll().stream()
                .collect(Collectors.toMap(ScheduledTaskRecord::getId, Function.identity()));

        // then
        assertThat(scheduledTasks.containsKey(taskId1), equalTo(true));
        assertThat(scheduledTasks.get(taskId1).getQueueName(), equalTo(location1.getQueueId().asString()));
        assertThat(scheduledTasks.get(taskId1).getNextProcessAt(), notNullValue());

        assertThat(scheduledTasks.containsKey(taskId2), equalTo(true));
        assertThat(scheduledTasks.get(taskId2).getQueueName(), equalTo(location2.getQueueId().asString()));
    }

    private QueueLocation queueLocation(DatabaseAccess databaseAccess, String queueName) {
        return QueueLocation.builder()
                .withTableName("scheduled_tasks")
                .withIdSequence(databaseAccess.getDatabaseDialect() == ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect.ORACLE_11G
                        ? "scheduled_tasks_seq"
                        : null
                )
                .withQueueId(new QueueId(queueName))
                .build();
    }

    private ScheduledTaskQueueDao scheduledTaskQueueDao(DatabaseAccess databaseAccess) {
        return new DefaultScheduledTaskQueueDao(
                "scheduled_tasks",
                databaseAccess.getDatabaseDialect(),
                databaseAccess.getJdbcTemplate(),
                databaseAccess.getTransactionTemplate(),
                QueueTableSchema.builder().build()
        );
    }

    private DatabaseAccessLayer databaseAccessLayer(DatabaseAccess databaseAccess) {
        DatabaseDialect databaseDialect;
        switch (databaseAccess.getDatabaseDialect()) {
            case POSTGRESQL:
                databaseDialect = DatabaseDialect.POSTGRESQL;
                break;
            case H2:
                databaseDialect = DatabaseDialect.H2;
                break;
            case MSSQL:
                databaseDialect = DatabaseDialect.MSSQL;
                break;
            case ORACLE_11G:
                databaseDialect = DatabaseDialect.ORACLE_11G;
                break;
            default:
                throw new RuntimeException("got unexpected databaseDialect: databaseDialect="
                        + databaseAccess.getDatabaseDialect());
        }
        return new SpringDatabaseAccessLayer(
                databaseDialect,
                QueueTableSchema.builder().build(),
                databaseAccess.getJdbcTemplate(),
                databaseAccess.getTransactionTemplate()
        );
    }
}
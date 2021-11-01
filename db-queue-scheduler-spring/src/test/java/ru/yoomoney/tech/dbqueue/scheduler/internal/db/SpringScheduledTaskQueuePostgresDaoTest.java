package ru.yoomoney.tech.dbqueue.scheduler.internal.db;

import org.junit.jupiter.api.Test;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.scheduler.BaseTest;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.spring.dao.SpringDatabaseAccessLayer;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 01.11.2021
 */
class SpringScheduledTaskQueuePostgresDaoTest extends BaseTest {
    private final ScheduledTaskQueueDao scheduledTaskQueueDao = new SpringScheduledTaskQueuePostgresDao(
            "scheduled_tasks",
            jdbcTemplate,
            transactionTemplate, QueueTableSchema.builder().build()
    );

    private final DatabaseAccessLayer databaseAccessLayer = new SpringDatabaseAccessLayer(
            DatabaseDialect.POSTGRESQL,
            QueueTableSchema.builder().build(),
            jdbcTemplate,
            transactionTemplate
    );

    @Test
    void isQueueEmpty_should_return_true_when_no_queue_tasks() {
        // given
        QueueLocation location1 = queueLocation("queue-" + uniqueCounter.incrementAndGet());
        QueueLocation location2 = queueLocation("queue-" + uniqueCounter.incrementAndGet());

        // when
        databaseAccessLayer.getQueueDao().enqueue(location2, EnqueueParams.create(""));

        // then
        assertThat(scheduledTaskQueueDao.isQueueEmpty(location1), equalTo(true));
    }


    @Test
    void isQueueEmpty_should_return_false_when_queue_tasks_exist() {
        // given
        QueueLocation location = queueLocation("queue-" + uniqueCounter.incrementAndGet());

        // when
        databaseAccessLayer.getQueueDao().enqueue(location, EnqueueParams.create(""));

        // then
        assertThat(scheduledTaskQueueDao.isQueueEmpty(location), equalTo(false));
    }

    @Test
    void should_find_all_tasks() {
        // given
        QueueLocation location1 = queueLocation("queue-" + uniqueCounter.incrementAndGet());
        QueueLocation location2 = queueLocation("queue-" + uniqueCounter.incrementAndGet());

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

    private QueueLocation queueLocation(String queueName) {
        return QueueLocation.builder()
                .withTableName("scheduled_tasks")
                .withQueueId(new QueueId(queueName))
                .build();
    }
}
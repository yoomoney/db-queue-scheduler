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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
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
    void should_find_all_queue_tasks() {
        // given
        QueueLocation location1 = queueLocation("queue-" + uniqueCounter.incrementAndGet());
        QueueLocation location2 = queueLocation("queue-" + uniqueCounter.incrementAndGet());
        QueueLocation location3 = queueLocation("queue-" + uniqueCounter.incrementAndGet());

        // when
        long task1 = databaseAccessLayer.getQueueDao().enqueue(location1, EnqueueParams.create(""));
        long task2 = databaseAccessLayer.getQueueDao().enqueue(location1, EnqueueParams.create(""));
        databaseAccessLayer.getQueueDao().enqueue(location2, EnqueueParams.create(""));

        // then
        assertThat(
                scheduledTaskQueueDao.findQueueTasks(location1.getQueueId()).stream()
                        .map(ScheduledTaskRecord::getId)
                        .collect(Collectors.toList()),
                containsInAnyOrder(task1, task2)
        );
        assertThat(scheduledTaskQueueDao.findQueueTasks(location3.getQueueId()), equalTo(Collections.emptyList()));
    }

    @Test
    void should_delete_queue_tasks() {
        // given
        QueueLocation location1 = queueLocation("queue-" + uniqueCounter.incrementAndGet());
        QueueLocation location2 = queueLocation("queue-" + uniqueCounter.incrementAndGet());

        // when
        long task1 = databaseAccessLayer.getQueueDao().enqueue(location1, EnqueueParams.create(""));
        long task2 = databaseAccessLayer.getQueueDao().enqueue(location1, EnqueueParams.create(""));
        long task3 = databaseAccessLayer.getQueueDao().enqueue(location2, EnqueueParams.create(""));
        scheduledTaskQueueDao.deleteQueueTasks(location1.getQueueId(), List.of(task1, task3));

        // then
        assertThat(
                scheduledTaskQueueDao.findQueueTasks(location1.getQueueId()).stream()
                        .map(ScheduledTaskRecord::getId)
                        .collect(Collectors.toList()),
                containsInAnyOrder(task2)
        );

        assertThat(
                scheduledTaskQueueDao.findQueueTasks(location2.getQueueId()).stream()
                        .map(ScheduledTaskRecord::getId)
                        .collect(Collectors.toList()),
                containsInAnyOrder(task3)
        );
    }

    @Test
    void should_update_next_processing_date() {
        // given
        QueueLocation location1 = queueLocation("queue-" + uniqueCounter.incrementAndGet());
        QueueLocation location2 = queueLocation("queue-" + uniqueCounter.incrementAndGet());
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
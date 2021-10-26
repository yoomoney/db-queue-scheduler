package ru.yoomoney.tech.dbqueue.scheduler;

import org.junit.jupiter.api.Test;
import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduleSettings;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduledTaskSettings;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
public class SchedulerTest extends BaseTest {

    @Test
    public void should_schedule_new_task() {
        // given
        Scheduler scheduler = new SpringSchedulerConfigurator()
                .withDatabaseDialect(DatabaseDialect.POSTGRESQL)
                .withTableName("scheduled_tasks")
                .withJdbcOperations(jdbcTemplate)
                .withTransactionOperations(transactionTemplate)
                .configure();
        AtomicBoolean executed = new AtomicBoolean(false);

        // when
        scheduler.start();
        scheduler.schedule(
                ScheduledTaskIdentity.of("scheduled-task" + uniqueCounter.incrementAndGet()),
                ScheduledTaskSettings.builder()
                        .withExecutionLock(Duration.ofHours(1L))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(0L)))
                        .build(),
                () -> {
                    executed.set(true);
                    return ScheduledTaskExecutionResult.success();
                }
        );

        // then
        await().atMost(Duration.ofSeconds(5L)).until(executed::get);
    }

    @Test
    public void should_schedule_task_once() {
        // given
        Scheduler scheduler1 = new SpringSchedulerConfigurator()
                .withDatabaseDialect(DatabaseDialect.POSTGRESQL)
                .withTableName("scheduled_tasks")
                .withJdbcOperations(jdbcTemplate)
                .withTransactionOperations(transactionTemplate)
                .configure();
        Scheduler scheduler2 = new SpringSchedulerConfigurator()
                .withDatabaseDialect(DatabaseDialect.POSTGRESQL)
                .withTableName("scheduled_tasks")
                .withJdbcOperations(jdbcTemplate)
                .withTransactionOperations(transactionTemplate)
                .configure();
        String taskName = "scheduled-task" + uniqueCounter.incrementAndGet();

        // when
        scheduler1.schedule(
                ScheduledTaskIdentity.of(taskName),
                ScheduledTaskSettings.builder()
                        .withExecutionLock(Duration.ofHours(1L))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(1L)))
                        .build(),
                ScheduledTaskExecutionResult::success
        );
        scheduler2.schedule(
                ScheduledTaskIdentity.of(taskName),
                ScheduledTaskSettings.builder()
                        .withExecutionLock(Duration.ofHours(1L))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(1L)))
                        .build(),
                ScheduledTaskExecutionResult::success
        );

        // then
        assertThat(
                jdbcTemplate.queryForObject("select count(1) from scheduled_tasks where queue_name = ?", Long.class, taskName),
                equalTo(1L)
        );
    }

    @Test
    public void should_throw_exception_when_task_with_the_same_name_already_scheduled() {
        // given
        Scheduler scheduler = new SpringSchedulerConfigurator()
                .withDatabaseDialect(DatabaseDialect.POSTGRESQL)
                .withTableName("scheduled_tasks")
                .withJdbcOperations(jdbcTemplate)
                .withTransactionOperations(transactionTemplate)
                .configure();
        String taskName = "scheduled-task" + uniqueCounter.incrementAndGet();

        // when
        scheduler.schedule(
                ScheduledTaskIdentity.of(taskName),
                ScheduledTaskSettings.builder()
                        .withExecutionLock(Duration.ofHours(1L))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(1L)))
                        .build(),
                ScheduledTaskExecutionResult::success
        );

        // then
        assertThrows(RuntimeException.class, () -> scheduler.schedule(
                ScheduledTaskIdentity.of(taskName),
                ScheduledTaskSettings.builder()
                        .withExecutionLock(Duration.ofHours(1L))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(1L)))
                        .build(),
                ScheduledTaskExecutionResult::success));
    }
}

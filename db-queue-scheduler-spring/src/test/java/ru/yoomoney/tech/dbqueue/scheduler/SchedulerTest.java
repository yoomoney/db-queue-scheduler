package ru.yoomoney.tech.dbqueue.scheduler;

import org.junit.jupiter.api.Test;
import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.SimpleScheduledTask;
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
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task" + uniqueCounter.incrementAndGet(),
                () -> {
                    executed.set(true);
                    return ScheduledTaskExecutionResult.success();
                }
        );

        // when
        scheduler.start();
        scheduler.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withMaxExecutionLockInterval(Duration.ofHours(1L))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(0L)))
                        .build()
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
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task" + uniqueCounter.incrementAndGet(),
                ScheduledTaskExecutionResult::success
        );

        // when
        scheduler1.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withMaxExecutionLockInterval(Duration.ofHours(1L))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(1L)))
                        .build()
        );
        scheduler2.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withMaxExecutionLockInterval(Duration.ofHours(1L))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(1L)))
                        .build()
        );

        // then
        assertThat(
                jdbcTemplate.queryForObject("select count(1) from scheduled_tasks where queue_name = ?", Long.class,
                        scheduledTask.getIdentity().asString()),
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
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task" + uniqueCounter.incrementAndGet(),
                ScheduledTaskExecutionResult::success
        );

        // when
        scheduler.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withMaxExecutionLockInterval(Duration.ofHours(1L))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(1L)))
                        .build()
        );

        // then
        assertThrows(RuntimeException.class, () -> scheduler.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withMaxExecutionLockInterval(Duration.ofHours(1L))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(1L)))
                        .build()));
    }
}

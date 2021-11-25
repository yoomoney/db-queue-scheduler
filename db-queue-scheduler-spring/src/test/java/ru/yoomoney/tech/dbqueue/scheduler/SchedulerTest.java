package ru.yoomoney.tech.dbqueue.scheduler;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.scheduler.db.DatabaseAccess;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.SimpleScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.models.info.ScheduledTaskInfo;
import ru.yoomoney.tech.dbqueue.scheduler.settings.FailureSettings;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduleSettings;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduledTaskSettings;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
public class SchedulerTest extends BaseTest {

    @ParameterizedTest
    @MethodSource("databaseAccessStream")
    void should_schedule_new_task(DatabaseAccess databaseAccess) {
        // given
        Scheduler scheduler = createScheduler(databaseAccess);
        AtomicBoolean executed = new AtomicBoolean(false);
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task" + uniqueCounter.incrementAndGet(),
                context -> {
                    executed.set(true);
                    return ScheduledTaskExecutionResult.success();
                }
        );

        // when
        scheduler.start();
        scheduler.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(0L)))
                        .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
                        .build()
        );

        // then
        await().atMost(Duration.ofSeconds(5L)).until(executed::get);
    }

    @ParameterizedTest
    @MethodSource("databaseAccessStream")
    void should_update_scheduled_task_state(DatabaseAccess databaseAccess) {
        // given
        Scheduler scheduler = createScheduler(databaseAccess);
        AtomicBoolean executed = new AtomicBoolean(false);
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task" + uniqueCounter.incrementAndGet(),
                context -> {
                    executed.set(true);
                    return ScheduledTaskExecutionResult.success().withState("new_state");
                }
        );

        // when
        scheduler.start();
        scheduler.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(0L)))
                        .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
                        .build()
        );

        // then
        await().atMost(Duration.ofSeconds(5L)).until(executed::get);
        assertThat(
                databaseAccess.getJdbcTemplate().queryForObject("select payload from scheduled_tasks where queue_name = ?",
                        String.class, scheduledTask.getIdentity().asString()),
                equalTo("new_state")
        );
    }

    @ParameterizedTest
    @MethodSource("databaseAccessStream")
    void should_schedule_task_once(DatabaseAccess databaseAccess) throws InterruptedException {
        // given
        Scheduler scheduler1 = createScheduler(databaseAccess);
        Scheduler scheduler2 = createScheduler(databaseAccess);
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task" + uniqueCounter.incrementAndGet(),
                context -> ScheduledTaskExecutionResult.success()
        );

        // when
        scheduler1.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(1L)))
                        .build()
        );
        scheduler2.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(1L)))
                        .build()
        );
        runConcurrently(
                () -> scheduler1.start(),
                () -> scheduler2.start()
        );

        // then
        assertThat(
                databaseAccess.getJdbcTemplate().queryForObject("select count(1) from scheduled_tasks where queue_name = ?",
                        Long.class, scheduledTask.getIdentity().asString()),
                equalTo(1L)
        );
    }

    @ParameterizedTest
    @MethodSource("databaseAccessStream")
    void should_task_executed_once(DatabaseAccess databaseAccess) throws Exception {
        // given
        Scheduler scheduler1 = createScheduler(databaseAccess);
        Scheduler scheduler2 = createScheduler(databaseAccess);
        AtomicInteger counter = new AtomicInteger();

        Class<?> scheduledTaskQueueConsumerClass =
                Class.forName("ru.yoomoney.tech.dbqueue.scheduler.internal.queue.ScheduledTaskQueueConsumer");
        Field heartbeatIntervalField = scheduledTaskQueueConsumerClass.getDeclaredField("HEARTBEAT_INTERVAL");
        heartbeatIntervalField.setAccessible(true);
        Duration heartbeatInterval = (Duration) heartbeatIntervalField.get(null);

        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task" + uniqueCounter.incrementAndGet(),
                context -> {
                    counter.incrementAndGet();
                    try {
                        Thread.sleep(heartbeatInterval.multipliedBy(2L).toMillis());
                    } catch (InterruptedException ex) {
                        throw new RuntimeException("got interrupted exception", ex);
                    }
                    return ScheduledTaskExecutionResult.success();
                }
        );

        // when
        scheduler1.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withFailureSettings(FailureSettings.none())
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(1L)))
                        .build()
        );
        scheduler2.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(1L)))
                        .build()
        );
        runConcurrently(
                () -> scheduler1.start(),
                () -> scheduler2.start()
        );

        Thread.sleep(heartbeatInterval.multipliedBy(2L).toMillis());

        // then
        assertThat(counter.get(), equalTo(1));
    }

    private void runConcurrently(Runnable... bodies) throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(bodies.length);

        List<Thread> threads = Arrays.stream(bodies)
                .map(body -> new Thread(() -> {
                    try {
                        barrier.await(1L, TimeUnit.SECONDS);
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                    body.run();
                }))
                .peek(Thread::start)
                .collect(Collectors.toList());

        for (Thread thread : threads) {
            thread.join();
        }
    }

    @ParameterizedTest
    @MethodSource("databaseAccessStream")
    void should_throw_exception_when_task_with_the_same_name_already_scheduled(DatabaseAccess databaseAccess) {
        // given
        Scheduler scheduler = createScheduler(databaseAccess);
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task" + uniqueCounter.incrementAndGet(),
                context -> ScheduledTaskExecutionResult.success()
        );

        // when
        scheduler.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(1L)))
                        .build()
        );

        // then
        assertThrows(RuntimeException.class, () -> scheduler.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(1L)))
                        .build()));
    }

    @ParameterizedTest
    @MethodSource("databaseAccessStream")
    void should_get_tasks_info(DatabaseAccess databaseAccess) {
        // given
        Scheduler scheduler = createScheduler(databaseAccess);
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task" + uniqueCounter.incrementAndGet(),
                context -> ScheduledTaskExecutionResult.success()
        );

        // when
        scheduler.start();
        scheduler.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(1L)))
                        .build()
        );
        ScheduledTaskInfo scheduledTaskStatistic = scheduler.getScheduledTaskInfo().stream()
                .filter(it -> Objects.equals(it.getIdentity(), scheduledTask.getIdentity()))
                .findFirst()
                .orElse(null);

        // then
        assertThat(scheduledTaskStatistic.getIdentity(), equalTo(scheduledTask.getIdentity()));
        assertThat(scheduledTaskStatistic.getNextExecutionTime(), notNullValue());
    }

    @ParameterizedTest
    @MethodSource("databaseAccessStream")
    void should_reschedule_task(DatabaseAccess databaseAccess) {
        // given
        Scheduler scheduler = createScheduler(databaseAccess);
        AtomicBoolean executed = new AtomicBoolean(false);
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task" + uniqueCounter.incrementAndGet(),
                context -> {
                    executed.set(true);
                    return ScheduledTaskExecutionResult.success();
                }
        );

        // when
        scheduler.start();
        scheduler.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofDays(1L)))
                        .build()
        );
        scheduler.reschedule(scheduledTask.getIdentity(), Instant.now());

        // then
        await().atMost(Duration.ofSeconds(5L)).until(executed::get);
    }

    @ParameterizedTest
    @MethodSource("databaseAccessStream")
    void should_retry_failed_task(DatabaseAccess databaseAccess) {
        // given
        Scheduler scheduler = createScheduler(databaseAccess);
        AtomicBoolean executed = new AtomicBoolean(false);
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task" + uniqueCounter.incrementAndGet(),
                context -> {
                    executed.set(true);
                    return ScheduledTaskExecutionResult.error();
                }
        );

        // when
        scheduler.start();
        scheduler.schedule(
                scheduledTask,
                ScheduledTaskSettings.builder()
                        .withFailureSettings(FailureSettings.linearBackoff(Duration.ofMinutes(1L)))
                        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofHours(1L)))
                        .build()
        );
        scheduler.reschedule(scheduledTask.getIdentity(), Instant.now());

        // then
        await().atMost(Duration.ofSeconds(5L)).until(executed::get);

        Instant nextExecutionDate = databaseAccess.getJdbcTemplate().queryForObject(
                "select next_process_at from scheduled_tasks where queue_name = ?",
                Timestamp.class,
                scheduledTask.getIdentity().asString()
        ).toInstant();
        assertTrue(nextExecutionDate.minus(Duration.ofMinutes(1L)).isBefore(Instant.now().plus(Duration.ofMinutes(1L))));
        assertTrue(nextExecutionDate.plus(Duration.ofMinutes(1L)).isAfter(Instant.now().plus(Duration.ofMinutes(1L))));
    }

    private Scheduler createScheduler(DatabaseAccess databaseAccess) {
        return new SpringSchedulerConfigurator()
                .withDatabaseDialect(databaseAccess.getDatabaseDialect())
                .withTableName("scheduled_tasks")
                .withIdSequenceName(databaseAccess.getDatabaseDialect() == DatabaseDialect.ORACLE_11G
                        ? "scheduled_tasks_seq"
                        : null)
                .withJdbcOperations(databaseAccess.getJdbcTemplate())
                .withTransactionOperations(databaseAccess.getTransactionTemplate())
                .configure();
    }
}

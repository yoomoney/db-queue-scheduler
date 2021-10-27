package ru.yoomoney.tech.dbqueue.scheduler.internal.queue;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import ru.yoomoney.tech.dbqueue.api.Task;
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.scheduler.config.impl.NoopScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.config.ScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskDefinition;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FixedRateNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;
import ru.yoomoney.tech.dbqueue.settings.ExtSettings;
import ru.yoomoney.tech.dbqueue.settings.FailRetryType;
import ru.yoomoney.tech.dbqueue.settings.FailureSettings;
import ru.yoomoney.tech.dbqueue.settings.PollSettings;
import ru.yoomoney.tech.dbqueue.settings.ProcessingMode;
import ru.yoomoney.tech.dbqueue.settings.ProcessingSettings;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueRetryType;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueSettings;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
class ScheduledTaskQueueConsumerTest {

    @Test
    public void should_execute_scheduledTask() {
        // given
        boolean[] executed = { false };
        ScheduledTaskDefinition scheduledTaskDefinition = ScheduledTaskDefinition.builder()
                .withScheduledTask(() -> {
                    executed[0] = true;
                    return ScheduledTaskExecutionResult.success();
                })
                .withScheduledTaskIdentity(ScheduledTaskIdentity.of("scheduled-task"))
                .withMaxExecutionLockInterval(Duration.ofHours(1L))
                .withNextExecutionTimeProvider(new FixedRateNextExecutionTimeProvider(Duration.ZERO))
                .build();
        ScheduledTaskQueueConsumer scheduledTaskQueueConsumer = new ScheduledTaskQueueConsumer(
                dummyQueueConfig(),
                scheduledTaskDefinition,
                NoopScheduledTaskLifecycleListener.getInstance()
        );

        // when
        scheduledTaskQueueConsumer.execute(dummyTask());

        // then
        assertThat(executed[0], equalTo(true));
    }

    @Test
    public void should_handle_RuntimeException_during_executing_scheduledTask() {
        // given
        ScheduledTaskDefinition scheduledTaskDefinition = ScheduledTaskDefinition.builder()
                .withScheduledTask(() -> {
                    throw new RuntimeException("test exception");
                })
                .withScheduledTaskIdentity(ScheduledTaskIdentity.of("scheduled-task"))
                .withMaxExecutionLockInterval(Duration.ofHours(1L))
                .withNextExecutionTimeProvider(new FixedRateNextExecutionTimeProvider(Duration.ZERO))
                .build();
        ScheduledTaskQueueConsumer scheduledTaskQueueConsumer = new ScheduledTaskQueueConsumer(
                dummyQueueConfig(),
                scheduledTaskDefinition,
                NoopScheduledTaskLifecycleListener.getInstance()
        );

        // when
        TaskExecutionResult taskExecutionResult = scheduledTaskQueueConsumer.execute(dummyTask());

        // then
        assertThat(taskExecutionResult.getActionType(), equalTo(TaskExecutionResult.Type.REENQUEUE));
    }

    @Test
    public void should_postpone_execution_according_to_nextExectutionTimeProvider() {
        // given
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        ScheduledTaskDefinition scheduledTaskDefinition = ScheduledTaskDefinition.builder()
                .withScheduledTask(ScheduledTaskExecutionResult::success)
                .withScheduledTaskIdentity(ScheduledTaskIdentity.of("scheduled-task"))
                .withMaxExecutionLockInterval(Duration.ofHours(1L))
                .withNextExecutionTimeProvider(new FixedRateNextExecutionTimeProvider(Duration.ofDays(1L), clock))
                .build();
        ScheduledTaskQueueConsumer scheduledTaskQueueConsumer = new ScheduledTaskQueueConsumer(
                dummyQueueConfig(),
                scheduledTaskDefinition,
                NoopScheduledTaskLifecycleListener.getInstance(),
                clock
        );

        // when
        TaskExecutionResult taskExecutionResult = scheduledTaskQueueConsumer.execute(dummyTask());

        // then
        assertThat(taskExecutionResult.getActionType(), equalTo(TaskExecutionResult.Type.REENQUEUE));
        assertThat(taskExecutionResult.getExecutionDelay().get(), equalTo(Duration.ofDays(1L)));
    }

    @Test
    public void should_postpone_execution_according_to_overridden_next_execution_time() {
        // given
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        ScheduledTaskDefinition scheduledTaskDefinition = ScheduledTaskDefinition.builder()
                .withScheduledTask(() ->
                        ScheduledTaskExecutionResult.success().shiftExecutionTime(clock.instant().plus(Duration.ofDays(10L))))
                .withScheduledTaskIdentity(ScheduledTaskIdentity.of("scheduled-task"))
                .withMaxExecutionLockInterval(Duration.ofHours(1L))
                .withNextExecutionTimeProvider(new FixedRateNextExecutionTimeProvider(Duration.ofDays(1L), clock))
                .build();
        ScheduledTaskQueueConsumer scheduledTaskQueueConsumer = new ScheduledTaskQueueConsumer(
                dummyQueueConfig(),
                scheduledTaskDefinition,
                NoopScheduledTaskLifecycleListener.getInstance(),
                clock
        );

        // when
        TaskExecutionResult taskExecutionResult = scheduledTaskQueueConsumer.execute(dummyTask());

        // then
        assertThat(taskExecutionResult.getActionType(), equalTo(TaskExecutionResult.Type.REENQUEUE));
        assertThat(taskExecutionResult.getExecutionDelay().get(), equalTo(Duration.ofDays(10L)));
    }

    @Test
    @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
    public void should_receive_lifecycle_events() {
        // given
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        ScheduledTaskDefinition scheduledTaskDefinition = ScheduledTaskDefinition.builder()
                .withScheduledTask(ScheduledTaskExecutionResult::success)
                .withScheduledTaskIdentity(ScheduledTaskIdentity.of("scheduled-task"))
                .withMaxExecutionLockInterval(Duration.ofHours(1L))
                .withNextExecutionTimeProvider(new FixedRateNextExecutionTimeProvider(Duration.ofDays(1L), clock))
                .build();
        DummyScheduledTaskLifecycleListener listener = new DummyScheduledTaskLifecycleListener();
        ScheduledTaskQueueConsumer scheduledTaskQueueConsumer = new ScheduledTaskQueueConsumer(
                dummyQueueConfig(),
                scheduledTaskDefinition,
                listener,
                clock
        );

        // when
        scheduledTaskQueueConsumer.execute(dummyTask());

        // then
        assertThat(listener.startedTaskIdentity, equalTo(ScheduledTaskIdentity.of("scheduled-task")));
        assertThat(listener.finishedTaskIdentity, equalTo(ScheduledTaskIdentity.of("scheduled-task")));
        assertThat(listener.executionResult, equalTo(ScheduledTaskExecutionResult.success()));
        assertThat(listener.processTaskTimeInMills, equalTo(0L));
        assertThat(listener.nextExecutionTime, equalTo(clock.instant().plus(Duration.ofDays(1L))));
    }

    private Task<String> dummyTask() {
        return Task.<String>builder(new QueueShardId("shardId"))
                .withPayload("")
                .build();
    }

    private QueueConfig dummyQueueConfig() {
        return new QueueConfig(
                QueueLocation.builder()
                        .withTableName("tableName")
                        .withQueueId(new QueueId("queueId"))
                        .build(),
                QueueSettings.builder()
                        .withProcessingSettings(ProcessingSettings.builder()
                                .withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS)
                                .withThreadCount(1)
                                .build()
                        )
                        .withPollSettings(PollSettings.builder()
                                .withBetweenTaskTimeout(Duration.ZERO)
                                .withNoTaskTimeout(Duration.ZERO)
                                .withFatalCrashTimeout(Duration.ZERO)
                                .build()
                        )
                        .withFailureSettings(FailureSettings.builder()
                                .withRetryType(FailRetryType.GEOMETRIC_BACKOFF)
                                .withRetryInterval(Duration.ZERO)
                                .build()
                        )
                        .withReenqueueSettings(ReenqueueSettings.builder()
                                .withRetryType(ReenqueueRetryType.MANUAL)
                                .build()
                        )
                        .withExtSettings(ExtSettings.builder().withSettings(Collections.emptyMap()).build())
                        .build()
        );
    }

    private static class DummyScheduledTaskLifecycleListener implements ScheduledTaskLifecycleListener {
        private ScheduledTaskIdentity startedTaskIdentity;
        private ScheduledTaskIdentity finishedTaskIdentity;
        private ScheduledTaskExecutionResult executionResult;
        private Instant nextExecutionTime;
        private Long processTaskTimeInMills;

        @Override
        public void started(@NotNull ScheduledTaskIdentity taskIdentity) {
            this.startedTaskIdentity = taskIdentity;
        }

        @Override
        public void finished(@NotNull ScheduledTaskIdentity taskIdentity,
                             @NotNull ScheduledTaskExecutionResult executionResult,
                             @NotNull Instant nextExecutionTime,
                             long processTaskTimeInMills) {
            this.finishedTaskIdentity = taskIdentity;
            this.executionResult = executionResult;
            this.nextExecutionTime = nextExecutionTime;
            this.processTaskTimeInMills = processTaskTimeInMills;
        }
    }
}
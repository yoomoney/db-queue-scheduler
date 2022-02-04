package ru.yoomoney.tech.dbqueue.scheduler.internal.queue;

import org.junit.jupiter.api.Test;
import ru.yoomoney.tech.dbqueue.api.Task;
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.scheduler.config.ScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.config.impl.NoopScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskDefinition;
import ru.yoomoney.tech.dbqueue.scheduler.internal.db.ScheduledTaskQueueDao;
import ru.yoomoney.tech.dbqueue.scheduler.internal.db.ScheduledTaskRecord;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FixedRateNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskContext;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;
import ru.yoomoney.tech.dbqueue.scheduler.models.SimpleScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.settings.FailureSettings;
import ru.yoomoney.tech.dbqueue.settings.ExtSettings;
import ru.yoomoney.tech.dbqueue.settings.FailRetryType;
import ru.yoomoney.tech.dbqueue.settings.PollSettings;
import ru.yoomoney.tech.dbqueue.settings.ProcessingMode;
import ru.yoomoney.tech.dbqueue.settings.ProcessingSettings;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueRetryType;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueSettings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
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
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task",
                context -> {
                    executed[0] = true;
                    return ScheduledTaskExecutionResult.success();
                }
        );
        ScheduledTaskDefinition scheduledTaskDefinition = ScheduledTaskDefinition.builder()
                .withScheduledTask(scheduledTask)
                .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
                .withNextExecutionTimeProvider(new FixedRateNextExecutionTimeProvider(Duration.ZERO))
                .build();
        ScheduledTaskQueueConsumer scheduledTaskQueueConsumer = new ScheduledTaskQueueConsumer(
                dummyQueueConfig(),
                scheduledTaskDefinition,
                NoopScheduledTaskLifecycleListener.getInstance(),
                new DummyScheduledTaskQueueDao()
        );

        // when
        scheduledTaskQueueConsumer.execute(dummyTask());

        // then
        assertThat(executed[0], equalTo(true));
    }

    @Test
    public void should_map_scheduledTaskContext() {
        // given
        AtomicReference<ScheduledTaskContext> contextRef = new AtomicReference<>();
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task",
                context -> {
                    contextRef.set(context);
                    return ScheduledTaskExecutionResult.success();
                }
        );
        ScheduledTaskDefinition scheduledTaskDefinition = ScheduledTaskDefinition.builder()
                .withScheduledTask(scheduledTask)
                .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
                .withNextExecutionTimeProvider(new FixedRateNextExecutionTimeProvider(Duration.ZERO))
                .build();
        ScheduledTaskQueueConsumer scheduledTaskQueueConsumer = new ScheduledTaskQueueConsumer(
                dummyQueueConfig(),
                scheduledTaskDefinition,
                NoopScheduledTaskLifecycleListener.getInstance(),
                new DummyScheduledTaskQueueDao()
        );
        Task<String> task = Task.<String>builder(new QueueShardId("shardId"))
                .withPayload("state")
                .withAttemptsCount(1L)
                .withReenqueueAttemptsCount(2L)
                .withTotalAttemptsCount(3L)
                .withCreatedAt(LocalDateTime.of(2010, 1, 1, 0, 0, 0).atZone(ZoneOffset.UTC))
                .build();

        // when
        scheduledTaskQueueConsumer.execute(task);

        // then
        assertThat(contextRef.get().getState().orElseThrow(), equalTo("state"));
        assertThat(contextRef.get().getAttemptsCount(), equalTo(1L));
        assertThat(contextRef.get().getSuccessfulAttemptsCount(), equalTo(2L));
        assertThat(contextRef.get().getTotalAttemptsCount(), equalTo(3L));
        assertThat(contextRef.get().getCreatedAt(), equalTo(LocalDateTime.of(2010, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC)));
    }

    @Test
    public void should_handle_RuntimeException_during_executing_scheduledTask() {
        // given
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task",
                context -> {
                    throw new RuntimeException("test exception");
                }
        );
        ScheduledTaskDefinition scheduledTaskDefinition = ScheduledTaskDefinition.builder()
                .withScheduledTask(scheduledTask)
                .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
                .withNextExecutionTimeProvider(new FixedRateNextExecutionTimeProvider(Duration.ZERO))
                .build();
        ScheduledTaskQueueConsumer scheduledTaskQueueConsumer = new ScheduledTaskQueueConsumer(
                dummyQueueConfig(),
                scheduledTaskDefinition,
                NoopScheduledTaskLifecycleListener.getInstance(),
                new DummyScheduledTaskQueueDao()
        );

        // when
        TaskExecutionResult taskExecutionResult = scheduledTaskQueueConsumer.execute(dummyTask());

        // then
        assertThat(taskExecutionResult.getActionType(), equalTo(TaskExecutionResult.Type.FAIL));
    }

    @Test
    public void should_postpone_execution_according_to_nextExectutionTimeProvider() {
        // given
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task",
                context -> ScheduledTaskExecutionResult.success()
        );
        ScheduledTaskDefinition scheduledTaskDefinition = ScheduledTaskDefinition.builder()
                .withScheduledTask(scheduledTask)
                .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
                .withNextExecutionTimeProvider(new FixedRateNextExecutionTimeProvider(Duration.ofDays(1L), clock))
                .build();
        ScheduledTaskQueueConsumer scheduledTaskQueueConsumer = new ScheduledTaskQueueConsumer(
                dummyQueueConfig(),
                scheduledTaskDefinition,
                NoopScheduledTaskLifecycleListener.getInstance(),
                new DummyScheduledTaskQueueDao(),
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
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task",
                context -> ScheduledTaskExecutionResult.success().shiftExecutionTime(clock.instant().plus(Duration.ofDays(10L)))
        );
        ScheduledTaskDefinition scheduledTaskDefinition = ScheduledTaskDefinition.builder()
                .withScheduledTask(scheduledTask)
                .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
                .withNextExecutionTimeProvider(new FixedRateNextExecutionTimeProvider(Duration.ofDays(1L), clock))
                .build();
        ScheduledTaskQueueConsumer scheduledTaskQueueConsumer = new ScheduledTaskQueueConsumer(
                dummyQueueConfig(),
                scheduledTaskDefinition,
                NoopScheduledTaskLifecycleListener.getInstance(),
                new DummyScheduledTaskQueueDao(),
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
        RuntimeException exception = new RuntimeException("test exception");
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task",
                context -> {
                    throw exception;
                }
        );
        ScheduledTaskDefinition scheduledTaskDefinition = ScheduledTaskDefinition.builder()
                .withScheduledTask(scheduledTask)
                .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
                .withNextExecutionTimeProvider(new FixedRateNextExecutionTimeProvider(Duration.ofDays(1L), clock))
                .build();
        DummyScheduledTaskLifecycleListener listener = new DummyScheduledTaskLifecycleListener();
        ScheduledTaskQueueConsumer scheduledTaskQueueConsumer = new ScheduledTaskQueueConsumer(
                dummyQueueConfig(),
                scheduledTaskDefinition,
                listener,
                new DummyScheduledTaskQueueDao(),
                clock
        );

        // when
        scheduledTaskQueueConsumer.execute(dummyTask());

        // then
        assertThat(listener.startedTaskIdentity, equalTo(ScheduledTaskIdentity.of("scheduled-task")));
        assertThat(listener.finishedTaskIdentity, equalTo(ScheduledTaskIdentity.of("scheduled-task")));
        assertThat(listener.crashedTaskIdentity, equalTo(ScheduledTaskIdentity.of("scheduled-task")));
        assertThat(listener.executionResult, equalTo(ScheduledTaskExecutionResult.error()));
        assertThat(listener.processTaskTimeInMills, equalTo(0L));
        assertThat(listener.nextExecutionTime, notNullValue());
        assertThat(listener.throwable, equalTo(exception));
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
                        .withFailureSettings(ru.yoomoney.tech.dbqueue.settings.FailureSettings.builder()
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

    private static class DummyScheduledTaskQueueDao implements ScheduledTaskQueueDao {
        @Override
        public Optional<ScheduledTaskRecord> findQueueTask(@Nonnull QueueId queueId) {
            return Optional.of(ScheduledTaskRecord.builder()
                    .withId(1L)
                    .withQueueName(queueId.asString())
                    .withNextProcessAt(Instant.now())
                    .build());
        }

        @Override
        public int updateNextProcessDate(@Nonnull QueueId queueId, @Nonnull Duration executionDelay) {
            return 0;
        }

        @Override
        public int updatePayload(@Nonnull QueueId queueId, @Nullable String payload) {
            return 0;
        }

        @Override
        public List<ScheduledTaskRecord> findAll() {
            return Collections.emptyList();
        }
    }

    private static class DummyScheduledTaskLifecycleListener implements ScheduledTaskLifecycleListener {
        private ScheduledTaskIdentity startedTaskIdentity;
        private ScheduledTaskIdentity finishedTaskIdentity;
        private ScheduledTaskIdentity crashedTaskIdentity;
        private ScheduledTaskExecutionResult executionResult;
        private Instant nextExecutionTime;
        private Long processTaskTimeInMills;
        private Throwable throwable;

        @Override
        public void started(@Nonnull ScheduledTaskIdentity taskIdentity, @Nonnull ScheduledTaskContext taskContext) {
            this.startedTaskIdentity = taskIdentity;
        }

        @Override
        public void finished(@Nonnull ScheduledTaskIdentity taskIdentity,
                             @Nonnull ScheduledTaskContext taskContext,
                             @Nonnull ScheduledTaskExecutionResult executionResult,
                             @Nonnull Instant nextExecutionTime,
                             long processTaskTimeInMills) {
            this.finishedTaskIdentity = taskIdentity;
            this.executionResult = executionResult;
            this.nextExecutionTime = nextExecutionTime;
            this.processTaskTimeInMills = processTaskTimeInMills;
        }

        @Override
        public void crashed(@Nonnull ScheduledTaskIdentity taskIdentity,
                            @Nonnull ScheduledTaskContext taskContext,
                            @Nullable Throwable exc) {
            this.crashedTaskIdentity = taskIdentity;
            this.throwable = exc;
        }
    }
}
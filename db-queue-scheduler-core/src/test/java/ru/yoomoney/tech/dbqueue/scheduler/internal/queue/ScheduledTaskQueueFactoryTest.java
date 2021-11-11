package ru.yoomoney.tech.dbqueue.scheduler.internal.queue;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import ru.yoomoney.tech.dbqueue.api.impl.SingleQueueShardRouter;
import ru.yoomoney.tech.dbqueue.scheduler.config.impl.NoopScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskDefinition;
import ru.yoomoney.tech.dbqueue.scheduler.internal.db.ScheduledTaskQueueDao;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FixedRateNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.SimpleScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.settings.FailureSettings;
import ru.yoomoney.tech.dbqueue.scheduler.settings.FailRetryType;
import ru.yoomoney.tech.dbqueue.settings.ExtSettings;
import ru.yoomoney.tech.dbqueue.settings.PollSettings;
import ru.yoomoney.tech.dbqueue.settings.ProcessingMode;
import ru.yoomoney.tech.dbqueue.settings.ProcessingSettings;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueRetryType;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueSettings;

import java.time.Duration;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 08.11.2021
 */
class ScheduledTaskQueueFactoryTest {
    private ScheduledTaskQueueFactory scheduledTaskQueueFactory = new ScheduledTaskQueueFactory(
            "scheduled_tasks_table",
            new QueueIdMapper(),
            dummyQueueSettings(),
            mock(ScheduledTaskQueueDao.class),
            mock(SingleQueueShardRouter.class),
            NoopScheduledTaskLifecycleListener.getInstance()
    );

    @ParameterizedTest
    @EnumSource(FailRetryType.class)
    void should_create_scheduled_tasks_queue(FailRetryType type) {
        // given
        ScheduledTask scheduledTask = SimpleScheduledTask.create(
                "scheduled-task-id",
                ScheduledTaskExecutionResult::success
        );
        ScheduledTaskDefinition scheduledTaskDefinition = ScheduledTaskDefinition.builder()
                .withScheduledTask(scheduledTask)
                .withFailureSettings(FailureSettings.builder()
                        .withRetryType(type)
                        .withRetryInterval(Duration.ofHours(1L))
                        .build()
                )
                .withNextExecutionTimeProvider(new FixedRateNextExecutionTimeProvider(Duration.ZERO))
                .build();

        // when
        ScheduledTaskQueue scheduledTasksQueue = scheduledTaskQueueFactory.createScheduledTasksQueue(scheduledTaskDefinition);

        // then
        QueueLocation location = scheduledTasksQueue.getQueueConsumer().getQueueConfig().getLocation();
        assertThat(location.getTableName(), equalTo("scheduled_tasks_table"));
        assertThat(location.getQueueId().asString(), equalTo("scheduled-task-id"));

        QueueSettings settings = scheduledTasksQueue.getQueueConsumer().getQueueConfig().getSettings();
        assertThat(settings.getFailureSettings().getRetryType().name(), equalTo(type.name()));
        assertThat(settings.getFailureSettings().getRetryInterval(), equalTo(Duration.ofHours(1L)));
    }

    private QueueSettings dummyQueueSettings() {
        return QueueSettings.builder()
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
                        .withRetryType(ru.yoomoney.tech.dbqueue.settings.FailRetryType.GEOMETRIC_BACKOFF)
                        .withRetryInterval(Duration.ZERO)
                        .build()
                )
                .withReenqueueSettings(ReenqueueSettings.builder()
                        .withRetryType(ReenqueueRetryType.MANUAL)
                        .build()
                )
                .withExtSettings(ExtSettings.builder().withSettings(Collections.emptyMap()).build())
                .build();
    }
}
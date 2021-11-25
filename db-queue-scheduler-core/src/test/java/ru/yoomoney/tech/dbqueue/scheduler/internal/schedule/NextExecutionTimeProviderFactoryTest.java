package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule;

import org.junit.jupiter.api.Test;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.CronNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FailureAwareNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FixedDelayNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FixedRateNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.settings.FailureSettings;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduleSettings;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduledTaskSettings;

import java.time.Duration;
import java.time.ZoneId;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 26.10.2021
 */
class NextExecutionTimeProviderFactoryTest {
    private final NextExecutionTimeProviderFactory factory = new NextExecutionTimeProviderFactory();

    @Test
    public void should_create_CronNextExecutionTimeProvider() {
        // given
        ScheduledTaskSettings scheduledTaskSettings = ScheduledTaskSettings.builder()
                .withScheduleSettings(ScheduleSettings.cron("0 * * * * MON-FRI", ZoneId.systemDefault()))
                .withFailureSettings(FailureSettings.none())
                .build();

        // when
        NextExecutionTimeProvider executionTimeProvider = factory.createExecutionTimeProvider(scheduledTaskSettings);

        // then
        assertThat(executionTimeProvider, instanceOf(CronNextExecutionTimeProvider.class));
    }

    @Test
    public void should_create_FixedDelayNextExecutionTimeProvider() {
        // given
        ScheduledTaskSettings scheduledTaskSettings = ScheduledTaskSettings.builder()
                .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ZERO))
                .withFailureSettings(FailureSettings.none())
                .build();

        // when
        NextExecutionTimeProvider executionTimeProvider = factory.createExecutionTimeProvider(scheduledTaskSettings);

        // then
        assertThat(executionTimeProvider, instanceOf(FixedDelayNextExecutionTimeProvider.class));
    }

    @Test
    public void should_create_FixedRateNextExecutionTimeProvider() {
        // given
        ScheduledTaskSettings scheduledTaskSettings = ScheduledTaskSettings.builder()
                .withScheduleSettings(ScheduleSettings.fixedRate(Duration.ZERO))
                .withFailureSettings(FailureSettings.none())
                .build();

        // when
        NextExecutionTimeProvider executionTimeProvider = factory.createExecutionTimeProvider(scheduledTaskSettings);

        // then
        assertThat(executionTimeProvider, instanceOf(FixedRateNextExecutionTimeProvider.class));
    }

    @Test
    public void should_create_FailureAwareNextExecutionTimeProvider() {
        // given
        ScheduledTaskSettings scheduledTaskSettings = ScheduledTaskSettings.builder()
                .withScheduleSettings(ScheduleSettings.fixedRate(Duration.ZERO))
                .withFailureSettings(FailureSettings.linearBackoff(Duration.ZERO))
                .build();

        // when
        NextExecutionTimeProvider executionTimeProvider = factory.createExecutionTimeProvider(scheduledTaskSettings);

        // then
        assertThat(executionTimeProvider, instanceOf(FailureAwareNextExecutionTimeProvider.class));
    }
}
package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule;

import org.junit.jupiter.api.Test;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.CronNextExecutionDelayProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FailureAwareNextExecutionDelayProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FixedDelayNextExecutionDelayProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FixedRateNextExecutionDelayProvider;
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
class NextExecutionDelayProviderFactoryTest {
    private final NextExecutionDelayProviderFactory factory = new NextExecutionDelayProviderFactory();

    @Test
    void should_create_CronNextExecutionDelayProvider() {
        // given
        ScheduledTaskSettings scheduledTaskSettings = ScheduledTaskSettings.builder()
                .withScheduleSettings(ScheduleSettings.cron("0 * * * * MON-FRI", ZoneId.systemDefault()))
                .withFailureSettings(FailureSettings.none())
                .build();

        // when
        NextExecutionDelayProvider executionTimeProvider = factory.createExecutionDelayProvider(scheduledTaskSettings);

        // then
        assertThat(executionTimeProvider, instanceOf(CronNextExecutionDelayProvider.class));
    }

    @Test
    void should_create_FixedDelayNextExecutionDelayProvider() {
        // given
        ScheduledTaskSettings scheduledTaskSettings = ScheduledTaskSettings.builder()
                .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ZERO))
                .withFailureSettings(FailureSettings.none())
                .build();

        // when
        NextExecutionDelayProvider executionTimeProvider = factory.createExecutionDelayProvider(scheduledTaskSettings);

        // then
        assertThat(executionTimeProvider, instanceOf(FixedDelayNextExecutionDelayProvider.class));
    }

    @Test
    void should_create_FixedRateNextExecutionDelayProvider() {
        // given
        ScheduledTaskSettings scheduledTaskSettings = ScheduledTaskSettings.builder()
                .withScheduleSettings(ScheduleSettings.fixedRate(Duration.ZERO))
                .withFailureSettings(FailureSettings.none())
                .build();

        // when
        NextExecutionDelayProvider executionTimeProvider = factory.createExecutionDelayProvider(scheduledTaskSettings);

        // then
        assertThat(executionTimeProvider, instanceOf(FixedRateNextExecutionDelayProvider.class));
    }

    @Test
    void should_create_FailureAwareNextExecutionDelayProvider() {
        // given
        ScheduledTaskSettings scheduledTaskSettings = ScheduledTaskSettings.builder()
                .withScheduleSettings(ScheduleSettings.fixedRate(Duration.ZERO))
                .withFailureSettings(FailureSettings.linearBackoff(Duration.ZERO))
                .build();

        // when
        NextExecutionDelayProvider executionTimeProvider = factory.createExecutionDelayProvider(scheduledTaskSettings);

        // then
        assertThat(executionTimeProvider, instanceOf(FailureAwareNextExecutionDelayProvider.class));
    }
}
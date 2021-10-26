package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule;

import org.junit.jupiter.api.Test;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.CronNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FixedDelayNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FixedRateNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduleSettings;

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
        ScheduleSettings fixedDelaySchedule = ScheduleSettings.cron("0 * * * * MON-FRI", ZoneId.systemDefault());

        // when
        NextExecutionTimeProvider executionTimeProvider = factory.createExecutionTimeProvider(fixedDelaySchedule);

        // then
        assertThat(executionTimeProvider, instanceOf(CronNextExecutionTimeProvider.class));
    }

    @Test
    public void should_create_FixedDelayNextExecutionTimeProvider() {
        // given
        ScheduleSettings fixedDelaySchedule = ScheduleSettings.fixedDelay(Duration.ZERO);

        // when
        NextExecutionTimeProvider executionTimeProvider = factory.createExecutionTimeProvider(fixedDelaySchedule);

        // then
        assertThat(executionTimeProvider, instanceOf(FixedDelayNextExecutionTimeProvider.class));
    }

    @Test
    public void should_create_FixedRateNextExecutionTimeProvider() {
        // given
        ScheduleSettings fixedDelaySchedule = ScheduleSettings.fixedRate(Duration.ZERO);

        // when
        NextExecutionTimeProvider executionTimeProvider = factory.createExecutionTimeProvider(fixedDelaySchedule);

        // then
        assertThat(executionTimeProvider, instanceOf(FixedRateNextExecutionTimeProvider.class));
    }
}
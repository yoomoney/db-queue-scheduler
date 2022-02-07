package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl;

import org.junit.jupiter.api.Test;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
class FixedRateNextExecutionDelayProviderTest {
    private final FixedRateNextExecutionDelayProvider fixedRateNextExecutionTimeProvider =
            new FixedRateNextExecutionDelayProvider(Duration.ofDays(1L));

    @Test
    void should_resolve_next_delay_based_on_executionTime() {
        // given
        ScheduledTaskExecutionContext context = new ScheduledTaskExecutionContext();
        context.setProcessingTime(Duration.ofHours(1L));

        // when
        Duration nextExecutionDelay = fixedRateNextExecutionTimeProvider.getNextExecutionDelay(context);

        // then
        assertThat(nextExecutionDelay, equalTo(Duration.ofHours(23L)));
    }

    @Test
    void should_resolve_next_delay_when_execution_time_not_set() {
        // given
        ScheduledTaskExecutionContext context = new ScheduledTaskExecutionContext();

        // when
        Duration nextExecutionDelay = fixedRateNextExecutionTimeProvider.getNextExecutionDelay(context);

        // then
        assertThat(nextExecutionDelay, equalTo(Duration.ofDays(1L)));
    }
}
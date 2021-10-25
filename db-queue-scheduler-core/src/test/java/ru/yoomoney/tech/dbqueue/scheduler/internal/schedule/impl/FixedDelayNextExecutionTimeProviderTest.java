package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl;

import org.junit.jupiter.api.Test;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
class FixedDelayNextExecutionTimeProviderTest {
    private final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
    private final FixedDelayNextExecutionTimeProvider fixedDelayNextExecutionTimeProvider =
            new FixedDelayNextExecutionTimeProvider(Duration.ofDays(1L), clock);

    @Test
    public void should_resolve_next_time_based_on_LastExecutionFinishTime() {
        // given
        Instant lastExecutionTime = LocalDateTime.of(2010, 1, 1, 0, 0).toInstant(ZoneOffset.UTC);
        ScheduledTaskExecutionContext context = new ScheduledTaskExecutionContext();
        context.setLastExecutionFinishTime(lastExecutionTime);

        // when
        Instant nextExecutionTime = fixedDelayNextExecutionTimeProvider.getNextExecutionTime(context);

        // then
        assertThat(nextExecutionTime, equalTo(lastExecutionTime.plus(Duration.ofDays(1L))));
    }

    @Test
    public void should_resolve_next_time_based_on_current_time_when_LastExecutionFinishTime_null() {
        // given
        ScheduledTaskExecutionContext context = new ScheduledTaskExecutionContext();

        // when
        Instant nextExecutionTime = fixedDelayNextExecutionTimeProvider.getNextExecutionTime(context);

        // then
        assertThat(nextExecutionTime, equalTo(clock.instant().plus(Duration.ofDays(1L))));
    }
}
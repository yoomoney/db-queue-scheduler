package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl;

import org.junit.jupiter.api.Test;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;

import java.time.Clock;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjusters;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 26.10.2021
 */
class CronNextExecutionTimeProviderTest {
    @Test
    public void should_resolve_next_time_based_on_configured_zoneId() {
        // given
        ZoneId moscowZone = ZoneId.of("Europe/Moscow");
        ZonedDateTime monday = ZonedDateTime.of(2010, 1, 1, 0, 0, 0, 0, moscowZone)
                .with(TemporalAdjusters.next(DayOfWeek.MONDAY));
        NextExecutionTimeProvider cronNextExecutionTimeProvider = new CronNextExecutionTimeProvider(
                "0 * * * * FRI",
                moscowZone,
                Clock.fixed(monday.toInstant(), ZoneId.systemDefault())
        );

        // when
        Instant nextExecutionTime = cronNextExecutionTimeProvider.getNextExecutionTime(new ScheduledTaskExecutionContext());

        // then
        assertThat(nextExecutionTime, equalTo(monday.with(TemporalAdjusters.next(DayOfWeek.FRIDAY)).toInstant()));
    }
    @Test
    public void should_throw_exception_when_cron_expression_not_valid() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new CronNextExecutionTimeProvider("not valid", ZoneId.of("Europe/Moscow"))
        );
    }
}
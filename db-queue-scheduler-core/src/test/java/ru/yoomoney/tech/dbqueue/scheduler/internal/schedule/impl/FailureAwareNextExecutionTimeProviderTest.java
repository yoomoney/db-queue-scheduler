package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.settings.FailureSettings;

import java.time.Clock;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjusters;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 09.11.2021
 */
class FailureAwareNextExecutionTimeProviderTest {
    private final ZoneId zoneId =  ZoneId.of("Europe/Moscow");
    private final ZonedDateTime now = ZonedDateTime.of(2010, 1, 1, 0, 0, 0, 0, zoneId)
            .with(TemporalAdjusters.next(DayOfWeek.MONDAY));
    private final Clock clock = Clock.fixed(now.toInstant(), zoneId);

    private static Stream<Arguments> backoffData() {
        return Stream.of(
                Arguments.of(FailureSettings.geometricBackoff(Duration.ofSeconds(1L)), 0, Duration.ofSeconds(1L)),
                Arguments.of(FailureSettings.geometricBackoff(Duration.ofSeconds(1L)), 1, Duration.ofSeconds(2L)),
                Arguments.of(FailureSettings.geometricBackoff(Duration.ofSeconds(1L)), 2, Duration.ofSeconds(4L)),
                Arguments.of(FailureSettings.geometricBackoff(Duration.ofSeconds(1L)), 3, Duration.ofSeconds(8L)),
                Arguments.of(FailureSettings.geometricBackoff(Duration.ofSeconds(1L)), 4, Duration.ofSeconds(16L)),

                Arguments.of(FailureSettings.arithmeticBackoff(Duration.ofSeconds(1L)), 0, Duration.ofSeconds(1L)),
                Arguments.of(FailureSettings.arithmeticBackoff(Duration.ofSeconds(1L)), 1, Duration.ofSeconds(3L)),
                Arguments.of(FailureSettings.arithmeticBackoff(Duration.ofSeconds(1L)), 2, Duration.ofSeconds(5L)),
                Arguments.of(FailureSettings.arithmeticBackoff(Duration.ofSeconds(1L)), 3, Duration.ofSeconds(7L)),
                Arguments.of(FailureSettings.arithmeticBackoff(Duration.ofSeconds(1L)), 4, Duration.ofSeconds(9L)),

                Arguments.of(FailureSettings.linearBackoff(Duration.ofSeconds(1L)), 0, Duration.ofSeconds(1L)),
                Arguments.of(FailureSettings.linearBackoff(Duration.ofSeconds(1L)), 1, Duration.ofSeconds(1L)),
                Arguments.of(FailureSettings.linearBackoff(Duration.ofSeconds(1L)), 2, Duration.ofSeconds(1L))
        );
    }

    @ParameterizedTest
    @MethodSource("backoffData")
    void should_calculate_backoff(FailureSettings failureSettings, long attemptNumber, Duration calculatedInterval) {
        // given
        FailureAwareNextExecutionTimeProvider nextExecutionTimeProvider = new FailureAwareNextExecutionTimeProvider(
                new FixedRateNextExecutionTimeProvider(Duration.ofDays(1L), clock),
                failureSettings,
                clock
        );
        ScheduledTaskExecutionContext context = new ScheduledTaskExecutionContext();
        context.setLastExecutionStartTime(clock.instant());
        context.setExecutionResultType(ScheduledTaskExecutionResult.Type.ERROR);
        context.setAttemptsCount(attemptNumber);

        // when
        Instant nextExecutionTime = nextExecutionTimeProvider.getNextExecutionTime(context);

        // then
        assertThat(Duration.between(clock.instant(), nextExecutionTime), equalTo(calculatedInterval));
    }

    @Test
    void should_not_calculate_backoff_if_execution_result_not_error() {
        // given
        FailureAwareNextExecutionTimeProvider nextExecutionTimeProvider = new FailureAwareNextExecutionTimeProvider(
                new FixedRateNextExecutionTimeProvider(Duration.ofDays(1L), clock),
                FailureSettings.linearBackoff(Duration.ofSeconds(1L)),
                clock
        );
        ScheduledTaskExecutionContext context = new ScheduledTaskExecutionContext();
        context.setLastExecutionStartTime(clock.instant());
        context.setExecutionResultType(ScheduledTaskExecutionResult.Type.SUCCESS);

        // when
        Instant nextExecutionTime = nextExecutionTimeProvider.getNextExecutionTime(context);

        // then
        assertThat(Duration.between(clock.instant(), nextExecutionTime), equalTo(Duration.ofDays(1L)));
    }

    @Test
    void should_not_calculate_backoff_if_attempts_limit_exceeded() {
        // given
        FailureAwareNextExecutionTimeProvider nextExecutionTimeProvider = new FailureAwareNextExecutionTimeProvider(
                new FixedRateNextExecutionTimeProvider(Duration.ofDays(1L), clock),
                FailureSettings.linearBackoff(Duration.ofSeconds(1L)).withMaxAttempts(10),
                clock
        );
        ScheduledTaskExecutionContext context1 = new ScheduledTaskExecutionContext();
        context1.setLastExecutionStartTime(clock.instant());
        context1.setExecutionResultType(ScheduledTaskExecutionResult.Type.ERROR);
        context1.setAttemptsCount(10L);

        ScheduledTaskExecutionContext context2 = new ScheduledTaskExecutionContext();
        context2.setLastExecutionStartTime(clock.instant());
        context2.setExecutionResultType(ScheduledTaskExecutionResult.Type.ERROR);
        context2.setAttemptsCount(11L);

        // when
        Instant nextExecutionTime1 = nextExecutionTimeProvider.getNextExecutionTime(context1);
        Instant nextExecutionTime2 = nextExecutionTimeProvider.getNextExecutionTime(context2);

        // then
        assertThat(Duration.between(clock.instant(), nextExecutionTime1), equalTo(Duration.ofSeconds(1L)));
        assertThat(Duration.between(clock.instant(), nextExecutionTime2), equalTo(Duration.ofDays(1L)));
    }

    @Test
    void should_override_fail_settings_when_cron_next_execution_time_earlier() {
        // given
        FailureAwareNextExecutionTimeProvider nextExecutionTimeProvider1 = new FailureAwareNextExecutionTimeProvider(
                new CronNextExecutionTimeProvider("0 0 3 * * *", zoneId, clock),
                FailureSettings.linearBackoff(Duration.ofSeconds(1L)),
                clock
        );
        FailureAwareNextExecutionTimeProvider nextExecutionTimeProvider2 = new FailureAwareNextExecutionTimeProvider(
                new CronNextExecutionTimeProvider("0 0 3 * * *", zoneId, clock),
                FailureSettings.linearBackoff(Duration.ofDays(1L)),
                clock
        );
        ScheduledTaskExecutionContext context = new ScheduledTaskExecutionContext();
        context.setExecutionResultType(ScheduledTaskExecutionResult.Type.ERROR);

        // when
        Instant nextExecutionTime1 = nextExecutionTimeProvider1.getNextExecutionTime(context);
        Instant nextExecutionTime2 = nextExecutionTimeProvider2.getNextExecutionTime(context);

        // then
        assertThat(nextExecutionTime1, equalTo(now.plus(Duration.ofSeconds(1L)).toInstant()));
        assertThat(nextExecutionTime2, equalTo(now.withHour(3).toInstant()));
    }
}
package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.settings.FailureSettings;

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
class FailureAwareNextExecutionDelayProviderTest {
    private final ZoneId zoneId =  ZoneId.of("Europe/Moscow");
    private final ZonedDateTime now = ZonedDateTime.of(2010, 1, 1, 0, 0, 0, 0, zoneId)
            .with(TemporalAdjusters.next(DayOfWeek.MONDAY));

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
        FailureAwareNextExecutionDelayProvider nextExecutionTimeProvider = new FailureAwareNextExecutionDelayProvider(
                new FixedRateNextExecutionDelayProvider(Duration.ofDays(1L)),
                failureSettings
        );
        ScheduledTaskExecutionContext context = new ScheduledTaskExecutionContext();
        context.setExecutionStartTime(Instant.now());
        context.setExecutionResultType(ScheduledTaskExecutionResult.Type.ERROR);
        context.setAttemptsCount(attemptNumber);

        // when
        Duration nextExecutionDelay = nextExecutionTimeProvider.getNextExecutionDelay(context);

        // then
        assertThat(nextExecutionDelay, equalTo(calculatedInterval));
    }

    @Test
    void should_not_calculate_backoff_if_execution_result_not_error() {
        // given
        FailureAwareNextExecutionDelayProvider nextExecutionTimeProvider = new FailureAwareNextExecutionDelayProvider(
                new FixedRateNextExecutionDelayProvider(Duration.ofDays(1L)),
                FailureSettings.linearBackoff(Duration.ofSeconds(1L))
        );
        ScheduledTaskExecutionContext context = new ScheduledTaskExecutionContext();
        context.setExecutionStartTime(Instant.now());
        context.setExecutionResultType(ScheduledTaskExecutionResult.Type.SUCCESS);

        // when
        Duration nextExecutionDelay = nextExecutionTimeProvider.getNextExecutionDelay(context);

        // then
        assertThat(nextExecutionDelay, equalTo(Duration.ofDays(1L)));
    }

    @Test
    void should_not_calculate_backoff_if_attempts_limit_exceeded() {
        // given
        FailureAwareNextExecutionDelayProvider nextExecutionTimeProvider = new FailureAwareNextExecutionDelayProvider(
                new FixedRateNextExecutionDelayProvider(Duration.ofDays(1L)),
                FailureSettings.linearBackoff(Duration.ofSeconds(1L)).withMaxAttempts(10)
        );
        ScheduledTaskExecutionContext context1 = new ScheduledTaskExecutionContext();
        context1.setExecutionStartTime(Instant.now());
        context1.setExecutionResultType(ScheduledTaskExecutionResult.Type.ERROR);
        context1.setAttemptsCount(10L);

        ScheduledTaskExecutionContext context2 = new ScheduledTaskExecutionContext();
        context2.setExecutionStartTime(Instant.now());
        context2.setExecutionResultType(ScheduledTaskExecutionResult.Type.ERROR);
        context2.setAttemptsCount(11L);

        // when
        Duration nextExecutionDelay1 = nextExecutionTimeProvider.getNextExecutionDelay(context1);
        Duration nextExecutionDelay2 = nextExecutionTimeProvider.getNextExecutionDelay(context2);

        // then
        assertThat(nextExecutionDelay1, equalTo(Duration.ofSeconds(1L)));
        assertThat(nextExecutionDelay2, equalTo(Duration.ofDays(1L)));
    }

    @Test
    void should_override_fail_settings_when_cron_next_execution_time_earlier() {
        // given
        FailureAwareNextExecutionDelayProvider nextExecutionTimeProvider1 = new FailureAwareNextExecutionDelayProvider(
                new CronNextExecutionDelayProvider("0 0 3 * * *", zoneId),
                FailureSettings.linearBackoff(Duration.ofSeconds(1L))
        );
        FailureAwareNextExecutionDelayProvider nextExecutionTimeProvider2 = new FailureAwareNextExecutionDelayProvider(
                new CronNextExecutionDelayProvider("0 0 3 * * *", zoneId),
                FailureSettings.linearBackoff(Duration.ofDays(1L))
        );
        ScheduledTaskExecutionContext context = new ScheduledTaskExecutionContext();
        context.setExecutionStartTime(now.toInstant());
        context.setExecutionResultType(ScheduledTaskExecutionResult.Type.ERROR);

        // when
        Duration nextExecutionDelay1 = nextExecutionTimeProvider1.getNextExecutionDelay(context);
        Duration nextExecutionDelay2 = nextExecutionTimeProvider2.getNextExecutionDelay(context);

        // then
        assertThat(nextExecutionDelay1, equalTo(Duration.ofSeconds(1L)));
        assertThat(nextExecutionDelay2, equalTo(Duration.between(now, now.withHour(3))));
    }
}
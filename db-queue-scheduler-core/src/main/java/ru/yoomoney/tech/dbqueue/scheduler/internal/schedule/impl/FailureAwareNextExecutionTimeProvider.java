package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl;

import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.settings.FailureSettings;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Composite provider that handles execution failures and calculates next execution time
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 24.11.2021
 */
public class FailureAwareNextExecutionTimeProvider implements NextExecutionTimeProvider {
    private final FailureSettings failureSettings;
    private final NextExecutionTimeProvider delegate;
    private final Clock clock;

    public FailureAwareNextExecutionTimeProvider(@Nonnull NextExecutionTimeProvider delegate,
                                                 @Nonnull FailureSettings failureSettings) {
        this(delegate, failureSettings, Clock.systemDefaultZone());
    }

    FailureAwareNextExecutionTimeProvider(@Nonnull NextExecutionTimeProvider delegate,
                                          @Nonnull FailureSettings failureSettings,
                                          @Nonnull Clock clock) {
        this.failureSettings = requireNonNull(failureSettings, "failureSettings");
        this.delegate = requireNonNull(delegate, "delegate");
        this.clock = requireNonNull(clock, "clock");
    }

    @Override
    public Instant getNextExecutionTime(@Nonnull ScheduledTaskExecutionContext executionContext) {
        requireNonNull(executionContext, "executionContext");
        if (isFailRetryIntervalNeeded(executionContext)) {
            Instant failRetryInterval = resolveFailRetryInterval(executionContext);
            if (delegate instanceof CronNextExecutionTimeProvider) {
                return earliest(delegate.getNextExecutionTime(executionContext), failRetryInterval);
            }
            return failRetryInterval;
        }
        return delegate.getNextExecutionTime(executionContext);
    }

    private Instant earliest(Instant left, Instant right) {
        return left.isBefore(right) ? left : right;
    }

    private boolean isFailRetryIntervalNeeded(ScheduledTaskExecutionContext executionContext) {
        boolean isResultError = executionContext.getExecutionResultType()
                .filter(ScheduledTaskExecutionResult.Type.ERROR::equals)
                .isPresent();
        boolean maxRetryAttemptsNotExceeded = failureSettings.getMaxAttempts().isEmpty() ||
                executionContext.getAttemptsCount().orElse(0L) <= failureSettings.getMaxAttempts().orElseThrow();

        return isResultError && maxRetryAttemptsNotExceeded;
    }

    private Instant resolveFailRetryInterval(@Nonnull ScheduledTaskExecutionContext executionContext) {
        long attemptsCount = executionContext.getAttemptsCount().orElse(0L);
        switch (failureSettings.getRetryType()) {
            case GEOMETRIC_BACKOFF:
                return clock.instant().plus(failureSettings.getRetryInterval().multipliedBy(1L << attemptsCount));
            case ARITHMETIC_BACKOFF:
                return clock.instant().plus(failureSettings.getRetryInterval().multipliedBy(1L + attemptsCount * 2L));
            case LINEAR_BACKOFF:
                return clock.instant().plus(failureSettings.getRetryInterval());
            default:
                throw new RuntimeException("got unexpected retryType: retryType=" + failureSettings.getRetryType());
        }
    }
}

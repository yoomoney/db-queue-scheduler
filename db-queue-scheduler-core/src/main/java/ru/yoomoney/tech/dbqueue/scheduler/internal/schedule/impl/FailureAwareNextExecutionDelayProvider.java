package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl;

import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionDelayProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.settings.FailureSettings;

import javax.annotation.Nonnull;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Composite provider that handles execution failures and calculates delays before next executions
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 24.11.2021
 */
public class FailureAwareNextExecutionDelayProvider implements NextExecutionDelayProvider {
    private final FailureSettings failureSettings;
    private final NextExecutionDelayProvider delegate;

    public FailureAwareNextExecutionDelayProvider(@Nonnull NextExecutionDelayProvider delegate,
                                                  @Nonnull FailureSettings failureSettings) {
        this.failureSettings = requireNonNull(failureSettings, "failureSettings");
        this.delegate = requireNonNull(delegate, "delegate");
    }

    @Override
    public Duration getNextExecutionDelay(@Nonnull ScheduledTaskExecutionContext executionContext) {
        requireNonNull(executionContext, "executionContext");
        if (isFailRetryIntervalNeeded(executionContext)) {
            Duration failRetryDelay = resolveFailRetryInterval(executionContext);
            if (delegate instanceof CronNextExecutionDelayProvider) {
                return min(delegate.getNextExecutionDelay(executionContext), failRetryDelay);
            }
            return failRetryDelay;
        }
        return delegate.getNextExecutionDelay(executionContext);
    }

    private Duration min(Duration left, Duration right) {
        return left.compareTo(right) < 0 ? left : right;
    }

    private boolean isFailRetryIntervalNeeded(ScheduledTaskExecutionContext executionContext) {
        boolean isResultError = executionContext.getExecutionResultType()
                .filter(ScheduledTaskExecutionResult.Type.ERROR::equals)
                .isPresent();
        boolean maxRetryAttemptsNotExceeded = failureSettings.getMaxAttempts().isEmpty() ||
                executionContext.getAttemptsCount().orElse(0L) <= failureSettings.getMaxAttempts().orElseThrow();

        return isResultError && maxRetryAttemptsNotExceeded;
    }

    private Duration resolveFailRetryInterval(@Nonnull ScheduledTaskExecutionContext executionContext) {
        long attemptsCount = executionContext.getAttemptsCount().orElse(0L);
        switch (failureSettings.getRetryType()) {
            case GEOMETRIC_BACKOFF:
                return failureSettings.getRetryInterval().multipliedBy(1L << attemptsCount);
            case ARITHMETIC_BACKOFF:
                return failureSettings.getRetryInterval().multipliedBy(1L + attemptsCount * 2L);
            case LINEAR_BACKOFF:
                return failureSettings.getRetryInterval();
            default:
                throw new RuntimeException("got unexpected retryType: retryType=" + failureSettings.getRetryType());
        }
    }
}

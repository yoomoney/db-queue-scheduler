package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl;

import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Provides next execution time according to the preconfigured fixed interval which is counted from the
 * last execution start time. If last execution start time is empty, execution time is counted from current time.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 22.10.2021
 */
public class FixedRateNextExecutionTimeProvider implements NextExecutionTimeProvider {
    private final Duration fixedRate;
    private final Clock clock;

    public FixedRateNextExecutionTimeProvider(@Nonnull Duration fixedRate) {
        this(fixedRate, Clock.systemDefaultZone());
    }

    public FixedRateNextExecutionTimeProvider(@Nonnull Duration fixedRate, @Nonnull Clock clock) {
        this.fixedRate = requireNonNull(fixedRate, "fixedRate");
        this.clock = requireNonNull(clock, "clock");
    }

    @Override
    public Instant getNextExecutionTime(@Nonnull ScheduledTaskExecutionContext executionContext) {
        requireNonNull(executionContext, "executionContext");
        return executionContext.getLastExecutionStartTime().orElseGet(clock::instant).plus(fixedRate);
    }
}

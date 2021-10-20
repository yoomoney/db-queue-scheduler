package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl;

import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProvider;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Provides next execution time according to the preconfigured fixed interval which is counted from the
 * last execution finish time. If last execution finish time is empty, execution time is counted from current time.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 22.10.2021
 */
public class FixedDelayNextExecutionTimeProvider implements NextExecutionTimeProvider {
    private final Duration fixedDelay;

    public FixedDelayNextExecutionTimeProvider(@Nonnull Duration fixedDelay) {
        this.fixedDelay = requireNonNull(fixedDelay, "fixedDelay");
    }

    @Override
    public Instant getNextExecutionTime(@Nonnull ScheduledTaskExecutionContext executionContext) {
        requireNonNull(executionContext, "executionContext");
        return executionContext.getLastExecutionStartTime().orElseGet(Instant::now).plus(fixedDelay);
    }
}

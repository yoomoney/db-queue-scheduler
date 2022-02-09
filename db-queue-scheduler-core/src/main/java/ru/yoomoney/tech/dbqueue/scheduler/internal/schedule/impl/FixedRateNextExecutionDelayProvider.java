package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl;

import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionDelayProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;

import javax.annotation.Nonnull;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Provider of delays of next executions according to the preconfigured fixed interval which is counted from the
 * last execution start time.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 22.10.2021
 */
public class FixedRateNextExecutionDelayProvider implements NextExecutionDelayProvider {
    private final Duration fixedRate;

    public FixedRateNextExecutionDelayProvider(@Nonnull Duration fixedRate) {
        this.fixedRate = requireNonNull(fixedRate, "fixedRate");
    }

    @Override
    public Duration getNextExecutionDelay(@Nonnull ScheduledTaskExecutionContext executionContext) {
        requireNonNull(executionContext, "executionContext");
        return fixedRate.minus(executionContext.getProcessingTime().orElse(Duration.ZERO));
    }
}

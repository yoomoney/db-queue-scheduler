package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl;

import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionDelayProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;

import javax.annotation.Nonnull;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Provider of delays of next executions according to the preconfigured fixed interval which is counted from the
 * last execution finish time. If last execution finish time is empty, execution time is counted from current time.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 22.10.2021
 */
public class FixedDelayNextExecutionDelayProvider implements NextExecutionDelayProvider {
    private final Duration fixedDelay;

    public FixedDelayNextExecutionDelayProvider(@Nonnull Duration fixedDelay) {
        this.fixedDelay = requireNonNull(fixedDelay, "fixedDelay");
    }

    @Override
    public Duration getNextExecutionDelay(@Nonnull ScheduledTaskExecutionContext executionContext) {
        requireNonNull(executionContext, "executionContext");
        return fixedDelay;
    }
}

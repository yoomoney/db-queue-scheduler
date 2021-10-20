package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl;

import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProvider;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.time.ZoneId;

import static java.util.Objects.requireNonNull;

/**
 * Provider next execution time according to predefined cron expression.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 22.10.2021
 */
public class CronNextExecutionTimeProvider implements NextExecutionTimeProvider {

    public CronNextExecutionTimeProvider(@Nonnull String cronExpression, @Nonnull ZoneId zoneId) {
        requireNonNull(cronExpression, "cronExpression");
        requireNonNull(zoneId, "zoneId");
    }

    @Override
    public Instant getNextExecutionTime(@Nonnull ScheduledTaskExecutionContext executionContext) {
        requireNonNull(executionContext, "executionContext");
        // todo implement
        return Instant.now();
    }
}

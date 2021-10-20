package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule;

import javax.annotation.Nonnull;
import java.time.Instant;

/**
 * Next execution time provider calculates next execution time according to the execution context and implemented logic
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 20.10.2021
 */
public interface NextExecutionTimeProvider {
    /**
     * Calculates next execution time for a scheduled task
     *
     * @param executionContext context of the scheduled task executions
     * @return calculated next execution time
     */
    Instant getNextExecutionTime(@Nonnull ScheduledTaskExecutionContext executionContext);
}

package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule;

import javax.annotation.Nonnull;
import java.time.Duration;

/**
 * Next execution delay provider calculates delay before next executions according to the execution context and implemented logic
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 20.10.2021
 */
public interface NextExecutionDelayProvider {
    /**
     * Calculates delay before next execution for a scheduled task
     *
     * @param executionContext context of the scheduled task executions
     * @return calculated delay next execution time
     */
    Duration getNextExecutionDelay(@Nonnull ScheduledTaskExecutionContext executionContext);
}

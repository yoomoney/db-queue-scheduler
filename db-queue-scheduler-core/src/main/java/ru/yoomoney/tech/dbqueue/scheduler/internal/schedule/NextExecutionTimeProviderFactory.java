package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule;

import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.CronNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FailureAwareNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FixedDelayNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FixedRateNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.settings.FailRetryType;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduleSettings;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduledTaskSettings;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Factory for {@link NextExecutionTimeProvider}
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 20.10.2021
 */
public class NextExecutionTimeProviderFactory {
    /**
     * Creates a next execution time provider according to passed schedule settings
     *
     * @param scheduledTaskSettings scheduled task settings
     * @return created next execution time provider
     */
    public NextExecutionTimeProvider createExecutionTimeProvider(@Nonnull ScheduledTaskSettings scheduledTaskSettings) {
        requireNonNull(scheduledTaskSettings, "scheduledTaskSettings");
        NextExecutionTimeProvider executionTimeProvider =
                createExecutionTimeProvider(scheduledTaskSettings.getScheduleSettings());

        return scheduledTaskSettings.getFailureSettings().getRetryType() == FailRetryType.NONE
                ? executionTimeProvider
                : new FailureAwareNextExecutionTimeProvider(executionTimeProvider, scheduledTaskSettings.getFailureSettings());
    }

    private NextExecutionTimeProvider createExecutionTimeProvider(@Nonnull ScheduleSettings scheduleSettings) {
        requireNonNull(scheduleSettings, "scheduleSettings");
        if (scheduleSettings.getCronSettings().isPresent()) {
            return new CronNextExecutionTimeProvider(
                    scheduleSettings.getCronSettings().orElseThrow().getCronExpression(),
                    scheduleSettings.getCronSettings().orElseThrow().getZoneId()
            );
        }
        if (scheduleSettings.getFixedDelay().isPresent()) {
            return new FixedDelayNextExecutionTimeProvider(scheduleSettings.getFixedDelay().orElseThrow());
        }
        if (scheduleSettings.getFixedRate().isPresent()) {
            return new FixedRateNextExecutionTimeProvider(scheduleSettings.getFixedRate().orElseThrow());
        }

        throw new IllegalStateException("not found settings for provider initializing. scheduledSettings=" + scheduleSettings);
    }
}

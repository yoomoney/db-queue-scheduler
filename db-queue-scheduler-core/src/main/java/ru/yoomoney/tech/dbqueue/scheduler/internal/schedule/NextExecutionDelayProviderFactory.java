package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule;

import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.CronNextExecutionDelayProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FailureAwareNextExecutionDelayProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FixedDelayNextExecutionDelayProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FixedRateNextExecutionDelayProvider;
import ru.yoomoney.tech.dbqueue.scheduler.settings.FailRetryType;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduleSettings;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduledTaskSettings;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Factory for {@link NextExecutionDelayProvider}
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 20.10.2021
 */
public class NextExecutionDelayProviderFactory {
    /**
     * Creates a next execution delay provider according to passed schedule settings
     *
     * @param scheduledTaskSettings scheduled task settings
     * @return created next execution time provider
     */
    public NextExecutionDelayProvider createExecutionDelayProvider(@Nonnull ScheduledTaskSettings scheduledTaskSettings) {
        requireNonNull(scheduledTaskSettings, "scheduledTaskSettings");
        NextExecutionDelayProvider executionDelayProvider =
                createExecutionDelayProvider(scheduledTaskSettings.getScheduleSettings());

        return scheduledTaskSettings.getFailureSettings().getRetryType() == FailRetryType.NONE
                ? executionDelayProvider
                : new FailureAwareNextExecutionDelayProvider(executionDelayProvider, scheduledTaskSettings.getFailureSettings());
    }

    private NextExecutionDelayProvider createExecutionDelayProvider(@Nonnull ScheduleSettings scheduleSettings) {
        requireNonNull(scheduleSettings, "scheduleSettings");
        if (scheduleSettings.getCronSettings().isPresent()) {
            return new CronNextExecutionDelayProvider(
                    scheduleSettings.getCronSettings().orElseThrow().getCronExpression(),
                    scheduleSettings.getCronSettings().orElseThrow().getZoneId()
            );
        }
        if (scheduleSettings.getFixedDelay().isPresent()) {
            return new FixedDelayNextExecutionDelayProvider(scheduleSettings.getFixedDelay().orElseThrow());
        }
        if (scheduleSettings.getFixedRate().isPresent()) {
            return new FixedRateNextExecutionDelayProvider(scheduleSettings.getFixedRate().orElseThrow());
        }

        throw new IllegalStateException("not found settings for provider initializing. scheduledSettings=" + scheduleSettings);
    }
}

package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule;

import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.CronNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FixedDelayNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl.FixedRateNextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduleSettings;

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
     * @param scheduleSettings schedule settings
     * @return created next execution time provider
     */
    public NextExecutionTimeProvider createExecutionTimeProvider(@Nonnull ScheduleSettings scheduleSettings) {
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

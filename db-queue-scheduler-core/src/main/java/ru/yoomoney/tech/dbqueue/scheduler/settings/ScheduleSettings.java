package ru.yoomoney.tech.dbqueue.scheduler.settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Schedule settings
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 20.10.2021
 */
public class ScheduleSettings {
    /**
     * A cron-like expression settings
     */
    @Nullable
    private final CronSettings cronSettings;

    /**
     * Fixed execution interval between start of the last execution and start of the next one
     */
    @Nullable
    private final Duration fixedRate;

    /**
     * Fixed execution interval between end of the last execution and start of the next one
     */
    @Nullable
    private final Duration fixedDelay;

    private ScheduleSettings(@Nullable CronSettings cronSettings, @Nullable Duration fixedRate, @Nullable Duration fixedDelay) {
        this.cronSettings = cronSettings;
        this.fixedRate = fixedRate;
        this.fixedDelay = fixedDelay;
    }

    @Nonnull
    public Optional<CronSettings> getCronSettings() {
        return Optional.ofNullable(cronSettings);
    }

    @Nonnull
    public Optional<Duration> getFixedRate() {
        return Optional.ofNullable(fixedRate);
    }

    @Nonnull
    public Optional<Duration> getFixedDelay() {
        return Optional.ofNullable(fixedDelay);
    }

    /**
     * A cron-like expression.
     *
     *  <p>The fields read from left to right are interpreted as follows.
     *  <ul>
     *  <li>second</li>
     *  <li>minute</li>
     *  <li>hour</li>
     *  <li>day of month</li>
     *  <li>month</li>
     *  <li>day of week</li>
     *  </ul>
     *
     *  <p>For example, {@code "0 * * * * MON-FRI"} means once per minute on weekdays
     *
     * @param cron expression describes rules of periodic task execution
     * @param zoneId time-zone in which cron expression should be evaluated
     * @return configured instance of {@link ScheduleSettings}
     */
    @Nonnull
    public static ScheduleSettings cron(@Nonnull String cron, @Nonnull ZoneId zoneId) {
        requireNonNull(cron, "cron");
        requireNonNull(zoneId, "zoneId");
        return new ScheduleSettings(new CronSettings(cron, zoneId), null, null);
    }

    /**
     * Fixed execution interval between start of the last execution and start of the next one
     *
     * @param fixedRate fixed execution interval between start of the last execution and start of the next one
     * @return configured instance of {@link ScheduleSettings}
     */
    @Nonnull
    public static ScheduleSettings fixedRate(@Nonnull Duration fixedRate) {
        requireNonNull(fixedRate, "fixedRate");
        return new ScheduleSettings( null, fixedRate, null);
    }

    /**
     * Fixed execution interval between end of the last execution and start of the next one
     * @param fixedDelay fixed execution interval between end of the last execution and start of the next one
     * @return configured instance of {@link ScheduleSettings}
     */
    @Nonnull
    public static ScheduleSettings fixedDelay(@Nonnull Duration fixedDelay) {
        requireNonNull(fixedDelay, "fixedDelay");
        return new ScheduleSettings( null, null, fixedDelay);
    }

    @Override
    public String toString() {
        return "ScheduleSettings{" +
                "fixedRate=" + fixedRate +
                ", fixedDelay=" + fixedDelay +
                ", cronSettings=" + cronSettings +
                '}';
    }

    /**
     * A cron-like expression settings
     */
    public static class CronSettings {
        /**
         * A cron-like expression
         */
        @Nonnull
        private final String cronExpression;

        /**
         * Time zone for which the cron expression should be computed
         */
        @Nonnull
        private final ZoneId zoneId;

        private CronSettings(@Nonnull String cronExpression, @Nonnull ZoneId zoneId) {
            this.cronExpression = requireNonNull(cronExpression, "cronExpression");
            this.zoneId = requireNonNull(zoneId, "zoneId");
        }

        public String getCronExpression() {
            return cronExpression;
        }

        public ZoneId getZoneId() {
            return zoneId;
        }

        @Override
        public String toString() {
            return "CronSettings{" +
                    "cronExpression='" + cronExpression + '\'' +
                    ", zoneId=" + zoneId +
                    '}';
        }
    }
}

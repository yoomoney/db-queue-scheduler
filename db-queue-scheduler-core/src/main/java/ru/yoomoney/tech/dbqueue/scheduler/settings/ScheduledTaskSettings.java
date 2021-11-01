package ru.yoomoney.tech.dbqueue.scheduler.settings;

import javax.annotation.Nonnull;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Scheduled task settings
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 20.10.2021
 */
public class ScheduledTaskSettings {
    /**
     * Flag that enables/disables scheduled task
     */
    private final boolean enabled;

    /**
     * Max interval during which task is not executed again unless task is rescheduled or the interval exceeded.
     *
     * <p>Normally, the second condition happens - tasks rescheduled according to its execution result. The interval
     * prevents unexpected conditions - an application crashed, some dead-lock happened, etc.
     *
     * <p>Pay attention, small interval may lead to a simultaneous execution of the same task by different nodes.
     */
    @Nonnull
    private final Duration maxExecutionLockInterval;

    /**
     * Scheduled settings
     */
    @Nonnull
    private final ScheduleSettings scheduleSettings;

    private ScheduledTaskSettings(boolean enabled,
                                  @Nonnull Duration executionLock,
                                  @Nonnull ScheduleSettings scheduleSettings) {
        this.enabled = enabled;
        this.maxExecutionLockInterval = requireNonNull(executionLock, "executionLock");
        this.scheduleSettings = requireNonNull(scheduleSettings, "scheduleSettings");
    }

    /**
     * Creates an object builder
     *
     * @return a new instance of {@link Builder}
     */
    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Nonnull
    public Duration getMaxExecutionLockInterval() {
        return maxExecutionLockInterval;
    }

    @Nonnull
    public ScheduleSettings getScheduleSettings() {
        return scheduleSettings;
    }

    @Override
    public String toString() {
        return "ScheduledTaskSettings{" +
                "enabled=" + enabled +
                ", maxExecutionLockInterval=" + maxExecutionLockInterval +
                ", scheduleSettings=" + scheduleSettings +
                '}';
    }

    /**
     * Builder for {@link ScheduledTaskSettings}
     */
    public static final class Builder {
        private boolean enabled = true;
        private Duration maxExecutionLockInterval;
        private ScheduleSettings scheduleSettings;

        private Builder() {
        }

        public Builder withEnabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder withMaxExecutionLockInterval(@Nonnull Duration maxExecutionLockInterval) {
            this.maxExecutionLockInterval = maxExecutionLockInterval;
            return this;
        }

        public Builder withScheduleSettings(@Nonnull ScheduleSettings scheduleSettings) {
            this.scheduleSettings = scheduleSettings;
            return this;
        }

        /**
         * Creates an object
         *
         * @return configured instance of {@link ScheduledTaskSettings}
         */
        @Nonnull
        public ScheduledTaskSettings build() {
            return new ScheduledTaskSettings(enabled, maxExecutionLockInterval, scheduleSettings);
        }
    }
}

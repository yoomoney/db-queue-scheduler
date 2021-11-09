package ru.yoomoney.tech.dbqueue.scheduler.settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Settings for task execution strategy in case of failure
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
     * Settings of retrying scheduled tasks in case of failure or freezing.
     */
    @Nonnull
    private final FailureSettings failureSettings;

    /**
     * Scheduled settings
     */
    @Nonnull
    private final ScheduleSettings scheduleSettings;

    private ScheduledTaskSettings(boolean enabled,
                                  @Nonnull ScheduleSettings scheduleSettings,
                                  @Nonnull FailureSettings failureSettings) {
        this.enabled = enabled;
        this.scheduleSettings = requireNonNull(scheduleSettings, "scheduleSettings");
        this.failureSettings = requireNonNull(failureSettings, "failureSettings");
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
    public ScheduleSettings getScheduleSettings() {
        return scheduleSettings;
    }

    @Nonnull
    public FailureSettings getFailureSettings() {
        return failureSettings;
    }

    @Override
    public String toString() {
        return "ScheduledTaskSettings{" +
                "enabled=" + enabled +
                ", failureSettings=" + failureSettings +
                ", scheduleSettings=" + scheduleSettings +
                '}';
    }

    /**
     * Builder for {@link ScheduledTaskSettings}
     */
    public static final class Builder {
        private boolean enabled = true;
        private ScheduleSettings scheduleSettings;
        private FailureSettings failureSettings;

        private Builder() {
        }

        public Builder withEnabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder withScheduleSettings(@Nonnull ScheduleSettings scheduleSettings) {
            this.scheduleSettings = scheduleSettings;
            return this;
        }

        public Builder withFailureSettings(@Nullable FailureSettings failureSettings) {
            this.failureSettings = failureSettings;
            return this;
        }

        /**
         * Creates an object
         *
         * @return configured instance of {@link ScheduledTaskSettings}
         */
        @Nonnull
        public ScheduledTaskSettings build() {
            return new ScheduledTaskSettings(enabled, scheduleSettings, failureSettings);
        }
    }
}

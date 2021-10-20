package ru.yoomoney.tech.dbqueue.scheduler.settings;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Scheduled task settings
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 20.10.2021
 */
public class ScheduledTaskSettings {
    /**
     * Task name
     */
    @Nonnull
    private final String name;
    /**
     * Scheduled settings
     */
    @Nonnull
    private final ScheduleSettings scheduleSettings;

    private ScheduledTaskSettings(@Nonnull String name, @Nonnull ScheduleSettings scheduleSettings) {
        this.name = requireNonNull(name, "name");
        this.scheduleSettings = requireNonNull(scheduleSettings, "scheduleSettings");
    }

    /**
     * Creates an object builder
     */
    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public ScheduleSettings getScheduleSettings() {
        return scheduleSettings;
    }

    @Override
    public String toString() {
        return "ScheduledTaskSettings{" +
                "name='" + name + '\'' +
                ", scheduleSettings=" + scheduleSettings +
                '}';
    }

    /**
     * Builder for {@link ScheduledTaskSettings}
     */
    public static final class Builder {
        private String name;
        private ScheduleSettings scheduleSettings;

        private Builder() {
        }

        public Builder withName(@Nonnull String name) {
            this.name = name;
            return this;
        }

        public Builder withScheduleSettings(@Nonnull ScheduleSettings scheduleSettings) {
            this.scheduleSettings = scheduleSettings;
            return this;
        }

        /**
         * Creates an object
         */
        @Nonnull
        public ScheduledTaskSettings build() {
            return new ScheduledTaskSettings(name, scheduleSettings);
        }
    }
}

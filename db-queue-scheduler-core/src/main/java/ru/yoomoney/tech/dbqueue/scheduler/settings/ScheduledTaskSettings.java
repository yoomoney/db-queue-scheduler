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

    private ScheduledTaskSettings(@Nonnull Duration executionLock, @Nonnull ScheduleSettings scheduleSettings) {
        this.maxExecutionLockInterval = requireNonNull(executionLock, "executionLock");
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
                "executionLock=" + maxExecutionLockInterval +
                ", scheduleSettings=" + scheduleSettings +
                '}';
    }

    /**
     * Builder for {@link ScheduledTaskSettings}
     */
    public static final class Builder {
        private Duration executionLock;
        private ScheduleSettings scheduleSettings;

        private Builder() {
        }

        public Builder withExecutionLock(@Nonnull Duration executionLock) {
            this.executionLock = executionLock;
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
            return new ScheduledTaskSettings(executionLock, scheduleSettings);
        }
    }
}

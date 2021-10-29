package ru.yoomoney.tech.dbqueue.scheduler.internal;

import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;

import javax.annotation.Nonnull;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Definition of a scheduled task - contains meta information, eg: identity, schedule, etc
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 19.10.2021
 */
public class ScheduledTaskDefinition {
    /**
     * Flag that enables/disables scheduled task
     */
    private final boolean enabled;
    /**
     * Unique identity of a scheduled task
     */
    @Nonnull
    private final ScheduledTaskIdentity identity;

    /**
     * Max interval during which task is not executed again unless task is rescheduled or the interval exceeded.
     */
    private final Duration maxExecutionLockInterval;

    /**
     * Next execution time provider
     */
    @Nonnull
    private final NextExecutionTimeProvider nextExecutionTimeProvider;

    /**
     * Scheduled task
     */
    @Nonnull
    private final ScheduledTask scheduledTask;

    private ScheduledTaskDefinition(boolean enabled,
                                    @Nonnull Duration maxExecutionLockInterval,
                                    @Nonnull NextExecutionTimeProvider nextExecutionTimeProvider,
                                    @Nonnull ScheduledTask scheduledTask) {
        this.enabled = enabled;
        this.maxExecutionLockInterval = requireNonNull(maxExecutionLockInterval, "maxExecutionLockInterval");
        this.nextExecutionTimeProvider = requireNonNull(nextExecutionTimeProvider, "nextExecutionTimeProvider");
        this.scheduledTask = requireNonNull(scheduledTask, "scheduledTask");
        this.identity = scheduledTask.getIdentity();
    }

    /**
     * Creates an object builder
     */
    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Nonnull
    public ScheduledTaskIdentity getIdentity() {
        return identity;
    }

    public Duration getMaxExecutionLockInterval() {
        return maxExecutionLockInterval;
    }

    @Nonnull
    public NextExecutionTimeProvider getNextExecutionTimeProvider() {
        return nextExecutionTimeProvider;
    }

    @Nonnull
    public ScheduledTask getScheduledTask() {
        return scheduledTask;
    }

    @Override
    public String toString() {
        return "ScheduledTaskDefinition{" +
                "enabled=" + enabled +
                ", identity=" + identity +
                ", maxExecutionLockInterval=" + maxExecutionLockInterval +
                ", nextExecutionTimeProvider=" + nextExecutionTimeProvider +
                ", scheduledTask=" + scheduledTask +
                '}';
    }

    /**
     * Builder for {@link ScheduledTaskDefinition}
     */
    public static final class Builder {
        private boolean enabled;
        private Duration maxExecutionLockInterval;
        private NextExecutionTimeProvider nextExecutionTimeProvider;
        private ScheduledTask scheduledTask;

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

        public Builder withNextExecutionTimeProvider(@Nonnull NextExecutionTimeProvider nextExecutionTimeProvider) {
            this.nextExecutionTimeProvider = nextExecutionTimeProvider;
            return this;
        }

        public Builder withScheduledTask(@Nonnull ScheduledTask scheduledTask) {
            this.scheduledTask = scheduledTask;
            return this;
        }

        /**
         * Creates an object
         */
        @Nonnull
        public ScheduledTaskDefinition build() {
            return new ScheduledTaskDefinition(enabled, maxExecutionLockInterval, nextExecutionTimeProvider, scheduledTask);
        }
    }
}

package ru.yoomoney.tech.dbqueue.scheduler.internal;

import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;
import ru.yoomoney.tech.dbqueue.scheduler.models.StatefulScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.settings.FailureSettings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

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
     * Scheduled task retry settings
     */
    private final FailureSettings failureSettings;

    /**
     * Next execution time provider
     */
    @Nonnull
    private final NextExecutionTimeProvider nextExecutionTimeProvider;

    /**
     * Stateless scheduled task
     */
    @Nullable
    private final ScheduledTask scheduledTask;

    /**
     * Stateful scheduled task
     */
    @Nullable
    private final StatefulScheduledTask statefulScheduledTask;

    private ScheduledTaskDefinition(boolean enabled,
                                    @Nonnull ScheduledTaskIdentity identity,
                                    @Nonnull FailureSettings failureSettings,
                                    @Nonnull NextExecutionTimeProvider nextExecutionTimeProvider,
                                    @Nullable ScheduledTask scheduledTask,
                                    @Nullable StatefulScheduledTask statefulScheduledTask) {
        this.enabled = enabled;
        this.identity = requireNonNull(identity, "identity");
        this.failureSettings = requireNonNull(failureSettings, "failureSettings");
        this.nextExecutionTimeProvider = requireNonNull(nextExecutionTimeProvider, "nextExecutionTimeProvider");
        this.scheduledTask = scheduledTask;
        this.statefulScheduledTask = statefulScheduledTask;
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
    public ScheduledTaskIdentity getIdentity() {
        return identity;
    }

    @Nonnull
    public FailureSettings getFailureSettings() {
        return failureSettings;
    }

    @Nonnull
    public NextExecutionTimeProvider getNextExecutionTimeProvider() {
        return nextExecutionTimeProvider;
    }

    @Nonnull
    public Optional<ScheduledTask> getScheduledTask() {
        return Optional.ofNullable(scheduledTask);
    }

    @Nonnull
    public Optional<StatefulScheduledTask> getStatefulScheduledTask() {
        return Optional.ofNullable(statefulScheduledTask);
    }

    @Override
    public String toString() {
        return "ScheduledTaskDefinition{" +
                "enabled=" + enabled +
                ", identity=" + identity +
                ", failureSettings=" + failureSettings +
                ", nextExecutionTimeProvider=" + nextExecutionTimeProvider +
                ", scheduledTask=" + scheduledTask +
                ", statefulScheduledTask=" + statefulScheduledTask +
                '}';
    }

    /**
     * Builder for {@link ScheduledTaskDefinition}
     */
    public static final class Builder {
        private boolean enabled;
        private FailureSettings failureSettings;
        private NextExecutionTimeProvider nextExecutionTimeProvider;
        private ScheduledTask scheduledTask;
        private StatefulScheduledTask statefulScheduledTask;

        private Builder() {
        }

        public Builder withEnabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder withFailureSettings(@Nonnull FailureSettings failureSettings) {
            this.failureSettings = failureSettings;
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

        public Builder withStatefulScheduledTask(@Nonnull StatefulScheduledTask statefulScheduledTask) {
            this.statefulScheduledTask = statefulScheduledTask;
            return this;
        }

        /**
         * Creates an object
         *
         * @return configured instance of {@link ScheduledTaskDefinition}
         */
        @Nonnull
        public ScheduledTaskDefinition build() {
            ScheduledTaskIdentity scheduledTaskIdentity;
            if (scheduledTask != null) {
                scheduledTaskIdentity = scheduledTask.getIdentity();
            } else if (statefulScheduledTask != null) {
                scheduledTaskIdentity = statefulScheduledTask.getIdentity();
            } else {
                throw new RuntimeException("scheduledTask or statefulScheduledTask must be set");
            }
            return new ScheduledTaskDefinition(enabled, scheduledTaskIdentity, failureSettings, nextExecutionTimeProvider,
                    scheduledTask, statefulScheduledTask);
        }
    }
}

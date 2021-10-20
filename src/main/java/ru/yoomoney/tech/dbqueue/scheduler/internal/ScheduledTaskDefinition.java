package ru.yoomoney.tech.dbqueue.scheduler.internal;

import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Definition of a scheduled task - contains meta information, eg: name, schedule, etc
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 19.10.2021
 */
public class ScheduledTaskDefinition {
    /**
     * Unique task name
     */
    @Nonnull
    private final String name;

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

    private ScheduledTaskDefinition(@Nonnull String name,
                                    @Nonnull NextExecutionTimeProvider nextExecutionTimeProvider,
                                    @Nonnull ScheduledTask scheduledTask) {
        this.name = requireNonNull(name, "name");
        this.nextExecutionTimeProvider = requireNonNull(nextExecutionTimeProvider, "nextExecutionTimeProvider");
        this.scheduledTask = requireNonNull(scheduledTask, "scheduledTask");
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
                "name='" + name + '\'' +
                ", nextExecutionTimeProvider=" + nextExecutionTimeProvider +
                ", scheduledTask=" + scheduledTask +
                '}';
    }

    /**
     * Builder for {@link ScheduledTaskDefinition}
     */
    public static final class Builder {
        private String name;
        private NextExecutionTimeProvider nextExecutionTimeProvider;
        private ScheduledTask scheduledTask;

        private Builder() {
        }

        public Builder withName(@Nonnull String name) {
            this.name = name;
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
            return new ScheduledTaskDefinition(name, nextExecutionTimeProvider, scheduledTask);
        }
    }
}

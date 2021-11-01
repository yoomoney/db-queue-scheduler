package ru.yoomoney.tech.dbqueue.scheduler.models.info;

import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;

import javax.annotation.Nonnull;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Scheduler task statistics model
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 01.11.2021
 */
public class ScheduledTaskInfo {
    /**
     * Identity of the task
     */
    @Nonnull
    private final ScheduledTaskIdentity identity;

    /**
     * Next execution time
     */
    @Nonnull
    private final Instant nextExecutionTime;


    private ScheduledTaskInfo(@Nonnull ScheduledTaskIdentity identity,
                              @Nonnull Instant nextExecutionTime) {
        this.identity = requireNonNull(identity, "identity");
        this.nextExecutionTime = requireNonNull(nextExecutionTime, "nextExecutionTime");
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

    @Nonnull
    public ScheduledTaskIdentity getIdentity() {
        return identity;
    }

    @Nonnull
    public Instant getNextExecutionTime() {
        return nextExecutionTime;
    }

    @Override
    public String toString() {
        return "ScheduledTaskStatistic{" +
                "identity=" + identity +
                ", nextExecutionTime=" + nextExecutionTime +
                '}';
    }

    /**
     * Builder for {@link ScheduledTaskInfo}
     */
    public static final class Builder {
        private ScheduledTaskIdentity identity;
        private Instant nextExecutionTime;

        private Builder() {
        }

        public Builder withIdentity(@Nonnull ScheduledTaskIdentity identity) {
            this.identity = identity;
            return this;
        }

        public Builder with(@Nonnull ScheduledTaskIdentity identity) {
            this.identity = identity;
            return this;
        }

        public Builder withNextExecutionTime(@Nonnull Instant nextExecutionTime) {
            this.nextExecutionTime = nextExecutionTime;
            return this;
        }

        /**
         * Creates an object
         *
         * @return configured instance of {@link ScheduledTaskInfo}
         */
        @Nonnull
        public ScheduledTaskInfo build() {
            return new ScheduledTaskInfo(identity, nextExecutionTime);
        }
    }
}

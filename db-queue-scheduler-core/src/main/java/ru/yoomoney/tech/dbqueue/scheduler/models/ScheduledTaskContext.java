package ru.yoomoney.tech.dbqueue.scheduler.models;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Context of a scheduled task
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 18.11.2021
 */
public class ScheduledTaskContext {
    /**
     * Current scheduled task state
     */
    @Nullable
    private final String state;
    /**
     * Date and time when the task was scheduled for the first time
     */
    @Nonnull
    private final Instant createdAt;
    /**
     * Number of attempts to execute the task including the current one since the last successful execution
     */
    private final long attemptsCount;
    /**
     * Number of successful attempts to execute the task
     */
    private final long successfulAttemptsCount;
    /**
     * Sum of all attempts to execute the task, including all failed attempts
     */
    private final long totalAttemptsCount;

    private ScheduledTaskContext(@Nullable String state,
                                 @Nonnull Instant createdAt,
                                 long attemptsCount,
                                 long successfulAttemptsCount,
                                 long totalAttemptsCount) {
        this.state = state;
        this.createdAt = requireNonNull(createdAt, "createdAt");
        this.attemptsCount = attemptsCount;
        this.successfulAttemptsCount = successfulAttemptsCount;
        this.totalAttemptsCount = totalAttemptsCount;
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
    public Optional<String> getState() {
        return Optional.ofNullable(state);
    }

    @Nonnull
    public Instant getCreatedAt() {
        return createdAt;
    }

    public long getAttemptsCount() {
        return attemptsCount;
    }

    public long getSuccessfulAttemptsCount() {
        return successfulAttemptsCount;
    }

    public long getTotalAttemptsCount() {
        return totalAttemptsCount;
    }

    @Override
    public String toString() {
        return "ScheduledTaskContext{" +
                "state='" + state + '\'' +
                ", createdAt=" + createdAt +
                ", attemptsCount=" + attemptsCount +
                ", successfulAttemptsCount=" + successfulAttemptsCount +
                ", totalAttemptsCount=" + totalAttemptsCount +
                '}';
    }

    /**
     * Builder for {@link ScheduledTaskContext}
     */
    public static final class Builder {
        private String state;
        private Instant createdAt;
        private long attemptsCount;
        private long successfulAttemptsCount;
        private long totalAttemptsCount;

        private Builder() {
        }

        public Builder withState(@Nullable String state) {
            this.state = state;
            return this;
        }

        public Builder withCreatedAt(@Nonnull Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder withAttemptsCount(long attemptsCount) {
            this.attemptsCount = attemptsCount;
            return this;
        }

        public Builder withSuccessfulAttemptsCount(long successfulAttemptsCount) {
            this.successfulAttemptsCount = successfulAttemptsCount;
            return this;
        }

        public Builder withTotalAttemptsCount(long totalAttemptsCount) {
            this.totalAttemptsCount = totalAttemptsCount;
            return this;
        }

        /**
         * Creates an object
         *
         * @return configured instance of {@link ScheduledTaskContext}
         */
        @Nonnull
        public ScheduledTaskContext build() {
            return new ScheduledTaskContext(state, createdAt, attemptsCount, successfulAttemptsCount, totalAttemptsCount);
        }
    }
}

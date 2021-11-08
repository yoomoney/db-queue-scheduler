package ru.yoomoney.tech.dbqueue.scheduler.internal.db;

import javax.annotation.Nonnull;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Database scheduled task record
 *
 * <p>NOTICE: Model does not represent a related table schema completely.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 01.11.2021
 */
public class ScheduledTaskRecord {
    /**
     * Unique identifier of the task
     */
    private final long id;

    /**
     * Name of the queue which the task is related to
     */
    @Nonnull
    private final String queueName;

    /**
     * Date and time of the next task execution
     */
    @Nonnull
    private final Instant nextProcessAt;


    private ScheduledTaskRecord(long id,
                                @Nonnull String queueName,
                                @Nonnull Instant nextProcessAt) {
        this.id = id;
        this.queueName = requireNonNull(queueName, "queueName");
        this.nextProcessAt = requireNonNull(nextProcessAt, "nextProcessAt");
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

    public long getId() {
        return id;
    }

    @Nonnull
    public String getQueueName() {
        return queueName;
    }

    @Nonnull
    public Instant getNextProcessAt() {
        return nextProcessAt;
    }

    @Override
    public String toString() {
        return "ScheduledTaskRecord{" +
                "id=" + id +
                ", queueName='" + queueName + '\'' +
                ", nextProcessAt=" + nextProcessAt +
                '}';
    }

    /**
     * Builder for {@link ScheduledTaskRecord}
     */
    public static final class Builder {
        private long id;
        private String queueName;
        private Instant nextProcessAt;

        private Builder() {
        }

        public Builder withId(long id) {
            this.id = id;
            return this;
        }

        public Builder withQueueName(@Nonnull String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder withNextProcessAt(@Nonnull Instant nextProcessAt) {
            this.nextProcessAt = nextProcessAt;
            return this;
        }

        /**
         * Creates an object
         *
         * @return configured instance of {@link ScheduledTaskRecord}
         */
        @Nonnull
        public ScheduledTaskRecord build() {
            return new ScheduledTaskRecord(id, queueName, nextProcessAt);
        }
    }
}

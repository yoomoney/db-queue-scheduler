package ru.yoomoney.tech.dbqueue.scheduler.settings;

import javax.annotation.Nonnull;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Settings for task retry in case of failure
 *
 * <p>Origin - {@link ru.yoomoney.tech.dbqueue.settings.FailureSettings}
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 08.11.2021
 */
public class RetrySettings {
    /**
     * Task execution retry strategy in case of failure.
     */
    @Nonnull
    private final RetryType retryType;

    /**
     * Retry interval for task execution in case of failure or freezing.
     *
     * During that interval task is not executed again unless task is rescheduled or the interval exceeded.
     *
     * <p>PAY ATTENTION, small interval may lead to a concurrent execution of the same task by different nodes.
     */
    @Nonnull
    private final Duration retryInterval;

    private RetrySettings(@Nonnull RetryType retryType, @Nonnull Duration retryInterval) {
        this.retryType = requireNonNull(retryType, "retryType");
        this.retryInterval = requireNonNull(retryInterval, "retryInterval");
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

    /**
     * Creates linear retries strategy settings
     *
     * @param retryInterval interval between executions in case of failure
     * @return new instance of {@link RetrySettings}
     */
    public static RetrySettings linear(@Nonnull Duration retryInterval) {
        return new RetrySettings(RetryType.LINEAR_BACKOFF, retryInterval);
    }

    /**
     * Creates arithmetic retries strategy settings
     *
     * @param initialInterval interval between executions in case of failure
     * @return new instance of {@link RetrySettings}
     */
    public static RetrySettings arithmetic(@Nonnull Duration initialInterval) {
        return new RetrySettings(RetryType.ARITHMETIC_BACKOFF, initialInterval);
    }

    /**
     * Creates geometric retries strategy settings
     *
     * @param initialInterval interval between executions in case of failure
     * @return new instance of {@link RetrySettings}
     */
    public static RetrySettings geometric(@Nonnull Duration initialInterval) {
        return new RetrySettings(RetryType.GEOMETRIC_BACKOFF, initialInterval);
    }

    @Nonnull
    public RetryType getRetryType() {
        return retryType;
    }

    @Nonnull
    public Duration getRetryInterval() {
        return retryInterval;
    }

    @Override
    public String toString() {
        return "FailureSettings{" +
                "retryType=" + retryType +
                ", retryInterval=" + retryInterval +
                '}';
    }

    /**
     * Builder for {@link RetrySettings}
     */
    public static final class Builder {
        private RetryType retryType;
        private Duration retryInterval;

        private Builder() {
        }

        public Builder withRetryType(@Nonnull RetryType retryType) {
            this.retryType = retryType;
            return this;
        }

        public Builder withRetryInterval(@Nonnull Duration retryInterval) {
            this.retryInterval = retryInterval;
            return this;
        }

        /**
         * Creates an object
         *
         * @return configured instance of {@link RetrySettings}
         */
        @Nonnull
        public RetrySettings build() {
            return new RetrySettings(retryType, retryInterval);
        }
    }
}

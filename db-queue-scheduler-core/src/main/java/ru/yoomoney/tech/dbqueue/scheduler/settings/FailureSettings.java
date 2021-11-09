package ru.yoomoney.tech.dbqueue.scheduler.settings;

import javax.annotation.Nonnull;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Settings for task execution strategy in case of failure
 *
 * <p>Origin - {@link ru.yoomoney.tech.dbqueue.settings.FailureSettings}
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 08.11.2021
 */
public class FailureSettings {
    /**
     * Task execution retry strategy in case of failure.
     */
    @Nonnull
    private final FailRetryType failRetryType;

    /**
     * Retry interval for task execution in case of failure or freezing.
     *
     * During that interval task is not executed again unless task is rescheduled or the interval exceeded.
     *
     * <p>PAY ATTENTION, small interval may lead to a concurrent execution of the same task by different nodes.
     */
    @Nonnull
    private final Duration retryInterval;

    private FailureSettings(@Nonnull FailRetryType failRetryType, @Nonnull Duration retryInterval) {
        this.failRetryType = requireNonNull(failRetryType, "retryType");
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
     * @return new instance of {@link FailureSettings}
     */
    public static FailureSettings linear(@Nonnull Duration retryInterval) {
        return new FailureSettings(FailRetryType.LINEAR_BACKOFF, retryInterval);
    }

    /**
     * Creates arithmetic retries strategy settings
     *
     * @param initialInterval interval between executions in case of failure
     * @return new instance of {@link FailureSettings}
     */
    public static FailureSettings arithmetic(@Nonnull Duration initialInterval) {
        return new FailureSettings(FailRetryType.ARITHMETIC_BACKOFF, initialInterval);
    }

    /**
     * Creates geometric retries strategy settings
     *
     * @param initialInterval interval between executions in case of failure
     * @return new instance of {@link FailureSettings}
     */
    public static FailureSettings geometric(@Nonnull Duration initialInterval) {
        return new FailureSettings(FailRetryType.GEOMETRIC_BACKOFF, initialInterval);
    }

    @Nonnull
    public FailRetryType getRetryType() {
        return failRetryType;
    }

    @Nonnull
    public Duration getRetryInterval() {
        return retryInterval;
    }

    @Override
    public String toString() {
        return "FailureSettings{" +
                "failRetryType=" + failRetryType +
                ", retryInterval=" + retryInterval +
                '}';
    }

    /**
     * Builder for {@link FailureSettings}
     */
    public static final class Builder {
        private FailRetryType failRetryType;
        private Duration retryInterval;

        private Builder() {
        }

        public Builder withRetryType(@Nonnull FailRetryType failRetryType) {
            this.failRetryType = failRetryType;
            return this;
        }

        public Builder withRetryInterval(@Nonnull Duration retryInterval) {
            this.retryInterval = retryInterval;
            return this;
        }

        /**
         * Creates an object
         *
         * @return configured instance of {@link FailureSettings}
         */
        @Nonnull
        public FailureSettings build() {
            return new FailureSettings(failRetryType, retryInterval);
        }
    }
}

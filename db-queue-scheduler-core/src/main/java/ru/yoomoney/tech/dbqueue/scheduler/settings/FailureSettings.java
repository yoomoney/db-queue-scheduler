package ru.yoomoney.tech.dbqueue.scheduler.settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Optional;

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
     * Retry interval for task execution in case of failure.
     */
    @Nonnull
    private final Duration retryInterval;

    /**
     * Max count of retry attempts
     *
     * <p>If the counter is exceeded, the next execution time is calculated according to a related task schedule
     */
    @Nullable
    private final Integer maxAttempts;

    private FailureSettings(@Nonnull FailRetryType failRetryType,
                            @Nonnull Duration retryInterval,
                            @Nullable Integer maxAttempts) {
        this.failRetryType = requireNonNull(failRetryType, "retryType");
        this.retryInterval = requireNonNull(retryInterval, "retryInterval");
        this.maxAttempts = maxAttempts;
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
     * Creates linear backoff failure strategy settings
     *
     * @param retryInterval interval between executions in case of failure
     * @return new instance of {@link FailureSettings}
     */
    public static FailureSettings linearBackoff(@Nonnull Duration retryInterval) {
        return new FailureSettings(FailRetryType.LINEAR_BACKOFF, retryInterval, null);
    }

    /**
     * Creates arithmetic backoff failure strategy settings
     *
     * @param initialInterval interval between executions in case of failure
     * @return new instance of {@link FailureSettings}
     */
    public static FailureSettings arithmeticBackoff(@Nonnull Duration initialInterval) {
        return new FailureSettings(FailRetryType.ARITHMETIC_BACKOFF, initialInterval, null);
    }

    /**
     * Creates geometric backoff failure strategy settings
     *
     * @param initialInterval interval between executions in case of failure
     * @return new instance of {@link FailureSettings}
     */
    public static FailureSettings geometricBackoff(@Nonnull Duration initialInterval) {
        return new FailureSettings(FailRetryType.GEOMETRIC_BACKOFF, initialInterval, null);
    }

    /**
     * Creates empty failure strategy that defers task execution according to its schedule algorithm
     * in spite of any execution result
     *
     * @return new instance of {@link FailureSettings}
     */
    public static FailureSettings none() {
        return new FailureSettings(FailRetryType.NONE, Duration.ZERO, null);
    }

    /**
     * Creates a new FailureSettings with configured max count of retry attempts
     *
     * @param maxAttempts max count of retry attempts
     * @return new instance of {@link FailureSettings}
     */
    public FailureSettings withMaxAttempts(@Nonnull Integer maxAttempts) {
        return new FailureSettings(failRetryType, retryInterval, maxAttempts);
    }

    @Nonnull
    public FailRetryType getRetryType() {
        return failRetryType;
    }

    @Nonnull
    public Duration getRetryInterval() {
        return retryInterval;
    }

    @Nonnull
    public Optional<Integer> getMaxAttempts() {
        return Optional.ofNullable(maxAttempts);
    }

    @Override
    public String toString() {
        return "FailureSettings{" +
                "failRetryType=" + failRetryType +
                ", retryInterval=" + retryInterval +
                ", maxAttempts=" + maxAttempts +
                '}';
    }

    /**
     * Builder for {@link FailureSettings}
     */
    public static final class Builder {
        private FailRetryType failRetryType;
        private Duration retryInterval;
        private Integer maxAttempts;

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

        public Builder withMaxAttempts(@Nullable Integer maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        /**
         * Creates an object
         *
         * @return configured instance of {@link FailureSettings}
         */
        @Nonnull
        public FailureSettings build() {
            return new FailureSettings(failRetryType, retryInterval, maxAttempts);
        }
    }
}

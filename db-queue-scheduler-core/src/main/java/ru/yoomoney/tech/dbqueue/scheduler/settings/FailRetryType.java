package ru.yoomoney.tech.dbqueue.scheduler.settings;

/**
 * Strategy type for the task deferring in case of retry.
 *
 * <p>Origin - {@link ru.yoomoney.tech.dbqueue.settings.FailRetryType}
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 08.11.2021
 */
public enum FailRetryType {
    /**
     * The task is deferred exponentially relative to the interval
     * {@link FailureSettings#getRetryInterval()}
     *
     * <p>The denominator of the progression equals 2.
     *
     * <p>First 6 terms: 1 2 4 8 16 32
     */
    GEOMETRIC_BACKOFF,

    /**
     * The task is deferred by an arithmetic progression relative to the interval
     * {@link FailureSettings#getRetryInterval()}.
     *
     * <p>The difference of progression equals 2.
     *
     * <p>First 6 terms: 1 3 5 7 9 11
     */
    ARITHMETIC_BACKOFF,

    /**
     * <p> The task is deferred with fixed delay.
     *
     * <p>Fixed delay value is set through {@link FailureSettings#getRetryInterval()}
     */
    LINEAR_BACKOFF
}

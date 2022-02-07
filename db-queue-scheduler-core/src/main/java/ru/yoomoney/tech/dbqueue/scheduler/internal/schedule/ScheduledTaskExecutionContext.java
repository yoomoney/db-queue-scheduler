package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule;

import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Context of a scheduled task execution.
 *
 * <p>Mutable and not thread-safe.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 20.10.2021
 */
@NotThreadSafe
public class ScheduledTaskExecutionContext {
    @Nullable
    private Instant executionStartTime;
    @Nullable
    private Duration processingTime;
    @Nullable
    private ScheduledTaskExecutionResult.Type executionResultType;
    @Nullable
    private Long attemptsCount;

    public ScheduledTaskExecutionContext() {
    }

    private ScheduledTaskExecutionContext(@Nullable Instant executionStartTime,
                                          @Nullable Duration processingTime,
                                          @Nullable ScheduledTaskExecutionResult.Type executionResultType,
                                          @Nullable Long attemptsCount) {
        this.executionStartTime = executionStartTime;
        this.processingTime = processingTime;
        this.executionResultType = executionResultType;
        this.attemptsCount = attemptsCount;
    }

    public void setExecutionStartTime(@Nonnull Instant executionStartTime) {
        this.executionStartTime = requireNonNull(executionStartTime, "executionStartTime");
    }

    public void setProcessingTime(@Nonnull Duration processingTime) {
        this.processingTime = requireNonNull(processingTime, "processingTime");
    }

    public void setExecutionResultType(@Nullable ScheduledTaskExecutionResult.Type executionResultType) {
        this.executionResultType = executionResultType;
    }

    public void setAttemptsCount(@Nullable Long attemptsCount) {
        this.attemptsCount = attemptsCount;
    }

    @Nonnull
    public Optional<Instant> getExecutionStartTime() {
        return Optional.ofNullable(executionStartTime);
    }

    @Nonnull
    public Optional<Duration> getProcessingTime() {
        return Optional.ofNullable(processingTime);
    }

    @Nonnull
    public Optional<ScheduledTaskExecutionResult.Type> getExecutionResultType() {
        return Optional.ofNullable(executionResultType);
    }

    @Nonnull
    public Optional<Long> getAttemptsCount() {
        return Optional.ofNullable(attemptsCount);
    }

    /**
     * Copy current {@link ScheduledTaskExecutionContext}
     * @return new instance of the context
     */
    public ScheduledTaskExecutionContext copy() {
        return new ScheduledTaskExecutionContext(executionStartTime, processingTime, executionResultType, attemptsCount);
    }

    @Override
    public String toString() {
        return "ScheduledTaskExecutionContext{" +
                "executionStartTime=" + executionStartTime +
                ", processingTime=" + processingTime +
                ", executionResultType=" + executionResultType +
                ", attemptsCount=" + attemptsCount +
                '}';
    }
}

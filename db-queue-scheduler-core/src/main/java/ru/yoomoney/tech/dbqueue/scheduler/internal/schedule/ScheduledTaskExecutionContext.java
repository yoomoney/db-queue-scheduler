package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule;

import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
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
    private Instant lastExecutionStartTime;
    @Nullable
    private Instant lastExecutionFinishTime;
    @Nullable
    private ScheduledTaskExecutionResult.Type executionResultType;
    @Nullable
    private Long attemptsCount;

    public ScheduledTaskExecutionContext() {
    }

    private ScheduledTaskExecutionContext(@Nullable Instant lastExecutionStartTime,
                                          @Nullable Instant lastExecutionFinishTime,
                                          @Nullable ScheduledTaskExecutionResult.Type executionResultType,
                                          @Nullable Long attemptsCount) {
        this.lastExecutionStartTime = lastExecutionStartTime;
        this.lastExecutionFinishTime = lastExecutionFinishTime;
        this.executionResultType = executionResultType;
        this.attemptsCount = attemptsCount;
    }

    public void setLastExecutionStartTime(@Nonnull Instant lastExecutionStartTime) {
        this.lastExecutionStartTime = requireNonNull(lastExecutionStartTime, "lastExecutionStartTime");
    }

    public void setLastExecutionFinishTime(@Nonnull Instant lastExecutionFinishTime) {
        this.lastExecutionFinishTime = requireNonNull(lastExecutionFinishTime, "lastExecutionFinishTime");
    }

    public void setExecutionResultType(@Nonnull ScheduledTaskExecutionResult.Type executionResultType) {
        this.executionResultType = requireNonNull(executionResultType, "executionResultType");
    }

    public void setAttemptsCount(@Nullable Long attemptsCount) {
        this.attemptsCount = attemptsCount;
    }

    @Nonnull
    public Optional<Instant> getLastExecutionStartTime() {
        return Optional.ofNullable(lastExecutionStartTime);
    }

    @Nonnull
    public Optional<Instant> getLastExecutionFinishTime() {
        return Optional.ofNullable(lastExecutionFinishTime);
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
        return new ScheduledTaskExecutionContext(lastExecutionStartTime, lastExecutionFinishTime, executionResultType,
                attemptsCount);
    }

    @Override
    public String toString() {
        return "ScheduledTaskExecutionContext{" +
                "lastExecutionStartTime=" + lastExecutionStartTime +
                ", lastExecutionFinishTime=" + lastExecutionFinishTime +
                ", executionResultType=" + executionResultType +
                ", attemptsCount=" + attemptsCount +
                '}';
    }
}

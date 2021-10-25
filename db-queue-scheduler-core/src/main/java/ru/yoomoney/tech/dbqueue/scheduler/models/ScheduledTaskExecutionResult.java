package ru.yoomoney.tech.dbqueue.scheduler.models;

import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduleSettings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Result of a scheduled task execution
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 19.10.2021
 */
public class ScheduledTaskExecutionResult {
    private static final ScheduledTaskExecutionResult SUCCESS_RESULT = new ScheduledTaskExecutionResult(Type.SUCCESS, null);
    private static final ScheduledTaskExecutionResult ERROR_RESULT = new ScheduledTaskExecutionResult(Type.ERROR, null);

    /**
     * Execution result type
     */
    @Nonnull
    private final Type type;

    /**
     * Next execution time overrides next scheduled time calculated according to its predefined schedule rules.
     *
     * <p> If field is null, related scheduled task`s predefined schedule works ({@link ScheduleSettings}).
     */
    @Nullable
    private final Instant nextExecutionTime;

    private ScheduledTaskExecutionResult(@Nonnull Type type, @Nullable Instant nextExecutionTime) {
        this.type = requireNonNull(type, "type");
        this.nextExecutionTime = nextExecutionTime;
    }

    /**
     * Task execution succeeded
     */
    @Nonnull
    public static ScheduledTaskExecutionResult success() {
        return SUCCESS_RESULT;
    }

    /**
     * Task execution failed
     */
    @Nonnull
    public static ScheduledTaskExecutionResult error() {
        return ERROR_RESULT;
    }

    /**
     * Shift next execution time
     */
    @Nonnull
    public ScheduledTaskExecutionResult shiftExecutionTime(@Nonnull Instant nextExecutionTime) {
        requireNonNull(nextExecutionTime, "nextExecutionTime");
        return new ScheduledTaskExecutionResult(type, nextExecutionTime);
    }

    @Nonnull
    public Type getType() {
        return type;
    }

    @Nonnull
    public Optional<Instant> getNextExecutionTime() {
        return Optional.ofNullable(nextExecutionTime);
    }

    @Override
    public String toString() {
        return "ScheduledTaskExecutionResult{" +
                "type=" + type +
                ", nextExecutionTime=" + nextExecutionTime +
                '}';
    }

    /**
     * Type of execution result
     */
    public enum Type {
        /**
         * Task execution succeeded
         */
        SUCCESS,
        /**
         * Task execution failed
         */
        ERROR
        ;
    }
}

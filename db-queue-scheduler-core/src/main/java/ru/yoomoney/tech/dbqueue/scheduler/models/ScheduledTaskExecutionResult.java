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
    private static final ScheduledTaskExecutionResult SUCCESS_RESULT = new ScheduledTaskExecutionResult(Type.SUCCESS, null, null);
    private static final ScheduledTaskExecutionResult ERROR_RESULT = new ScheduledTaskExecutionResult(Type.ERROR, null, null);

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

    /**
     * Scheduled task state
     */
    @Nullable
    private final String state;

    private ScheduledTaskExecutionResult(@Nonnull Type type, @Nullable Instant nextExecutionTime, @Nullable String state) {
        this.type = requireNonNull(type, "type");
        this.nextExecutionTime = nextExecutionTime;
        this.state = state;
    }

    /**
     * Task execution succeeded
     *
     * @return flyweight instance of a simple succeed execution result
     */
    @Nonnull
    public static ScheduledTaskExecutionResult success() {
        return SUCCESS_RESULT;
    }

    /**
     * Task execution failed
     *
     * @return flyweight instance of a simple failed execution result
     */
    @Nonnull
    public static ScheduledTaskExecutionResult error() {
        return ERROR_RESULT;
    }

    /**
     * Set new state of a scheduled task
     *
     * @param state state of the scheduled task
     * @return new instance of {@link ScheduledTaskExecutionResult} with the new state
     */
    @Nonnull
    public ScheduledTaskExecutionResult withState(@Nonnull String state) {
        requireNonNull(state, "state");
        return new ScheduledTaskExecutionResult(type, nextExecutionTime, state);
    }

    /**
     * Shift next execution time
     *
     * @param nextExecutionTime date time at which related scheduled task should be executed again
     * @return new instance of {@link ScheduledTaskExecutionResult} that overrides scheduled task next execution time
     */
    @Nonnull
    public ScheduledTaskExecutionResult shiftExecutionTime(@Nonnull Instant nextExecutionTime) {
        requireNonNull(nextExecutionTime, "nextExecutionTime");
        return new ScheduledTaskExecutionResult(type, nextExecutionTime, state);
    }

    @Nonnull
    public Type getType() {
        return type;
    }

    @Nonnull
    public Optional<String> getState() {
        return Optional.ofNullable(state);
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
                ", state='" + state + '\'' +
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

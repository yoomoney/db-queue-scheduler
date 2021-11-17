package ru.yoomoney.tech.dbqueue.scheduler.models;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Result of a stateful scheduled task execution
 *
 * @param <StateT> type of the state
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 17.11.2021
 */
public class StatefulScheduledTaskExecutionResult<StateT> {
    /**
     * Result of a scheduled task execution
     */
    @Nonnull
    private final ScheduledTaskExecutionResult executionResult;

    /**
     * State of a scheduled task
     */
    @Nullable
    private final StateT state;

    private StatefulScheduledTaskExecutionResult(@Nonnull ScheduledTaskExecutionResult executionResult,
                                                 @Nullable StateT state) {
        this.executionResult = requireNonNull(executionResult, "executionResult");
        this.state = state;
    }

    /**
     * Task execution succeeded
     *
     * @param state current scehduled task state
     * @param <StateT> type of the state
     * @return instance of succeed execution result
     */
    @Nonnull
    public static <StateT> StatefulScheduledTaskExecutionResult<StateT> success(@Nullable StateT state) {
        return new StatefulScheduledTaskExecutionResult<>(ScheduledTaskExecutionResult.success(), state);
    }

    /**
     * Task execution failed
     *
     * @param state current scehduled task state
     * @param <StateT> type of the state
     * @return instance of failed execution result
     */
    @Nonnull
    public static <StateT> StatefulScheduledTaskExecutionResult<StateT> error(@Nullable StateT state) {
        return new StatefulScheduledTaskExecutionResult<>(ScheduledTaskExecutionResult.error(), state);
    }

    /**
     * Shift next execution time
     *
     * @param nextExecutionTime date time at which related scheduled task should be executed again
     * @return new instance of {@link StatefulScheduledTaskExecutionResult} that overrides scheduled task next execution time
     */
    @Nonnull
    public StatefulScheduledTaskExecutionResult<StateT> shiftExecutionTime(@Nonnull Instant nextExecutionTime) {
        requireNonNull(nextExecutionTime, "nextExecutionTime");
        return new StatefulScheduledTaskExecutionResult<>(executionResult.shiftExecutionTime(nextExecutionTime), state);
    }

    @Nonnull
    public ScheduledTaskExecutionResult getExecutionResult() {
        return executionResult;
    }

    @Nonnull
    public Optional<StateT> getState() {
        return Optional.ofNullable(state);
    }

    @Override
    public String toString() {
        return "StatefulScheduledTaskExecutionResult{" +
                "executionResult=" + executionResult +
                ", state=" + state +
                '}';
    }
}

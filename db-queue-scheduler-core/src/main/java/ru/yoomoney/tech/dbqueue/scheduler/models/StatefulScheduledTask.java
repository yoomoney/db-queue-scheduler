package ru.yoomoney.tech.dbqueue.scheduler.models;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Stateful task that executed periodically at a scheduled time
 *
 * @param <StateT> type of the state
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 17.11.2021
 */
public interface StatefulScheduledTask<StateT> {
    /**
     * Get unique identity of a scheduled task
     *
     * @return identity of a scheduled task
     */
    @Nonnull
    ScheduledTaskIdentity getIdentity();

    /**
     * Executes scheduled task
     *
     * @param state current scheduled task state
     * @return execution result
     */
    @Nonnull
    StatefulScheduledTaskExecutionResult<StateT> execute(@Nullable StateT state);

    /**
     * Get scheduled task state transformer which serializes state object into a string and deserializes the latter as well.
     *
     * @return state transformer
     */
    @Nonnull
    StatefulScheduledTaskStateTransformer<StateT> getTransformer();

    /**
     * Returns state which task received on the first execution
     * @return initial state value
     */
    @Nullable
    default StateT getInitialState() {
        return null;
    }
}

package ru.yoomoney.tech.dbqueue.scheduler.models;

import javax.annotation.Nullable;

/**
 * Marshaller and unmarshaller for the state in the task
 *
 * @param <StateT> type of the state in the scheduled task
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 17.11.2021
 */
public interface StatefulScheduledTaskStateTransformer<StateT> {
    /**
     * Unmarshall the string into a typed object.
     *
     * @param state task state
     * @return Object with task data
     */
    @Nullable
    StateT toObject(@Nullable String state);

    /**
     * Marshall the typed object with task state into a string.
     *
     * @param state task state
     * @return string with the task payload.
     */
    @Nullable
    String fromObject(@Nullable StateT state);
}

package ru.yoomoney.tech.dbqueue.scheduler.models;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Simple implementation of a stateful scheduled task.
 *
 * <p>Introduced to provide simple to use library API
 *
 * @param <StateT> type of the state
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 29.10.2021
 */
public class SimpleStatefulScheduledTask<StateT> implements StatefulScheduledTask<StateT> {
    /**
     * Scheduled task identity
     */
    private final ScheduledTaskIdentity identity;

    /**
     * Task body that should be executed periodically
     */
    private final Function<StateT, StatefulScheduledTaskExecutionResult<StateT>> body;

    /**
     * Task state transformer which serializes state object into a string and deserializes the latter as well
     */
    private final StatefulScheduledTaskStateTransformer<StateT> transformer;

    private SimpleStatefulScheduledTask(@Nonnull ScheduledTaskIdentity identity,
                                        @Nonnull Function<StateT, StatefulScheduledTaskExecutionResult<StateT>> body,
                                        @Nonnull StatefulScheduledTaskStateTransformer<StateT> transformer) {
        this.identity = requireNonNull(identity, "identity");
        this.body = requireNonNull(body, "body");
        this.transformer = requireNonNull(transformer, "transformer");
    }

    /**
     * Creates stateful scheduled task by name and execution body
     * 
     * @param taskName scheduled task name
     * @param body job that should be executed
     * @return prepared scheduled task
     */
    public static SimpleStatefulScheduledTask<String> create(
            @Nonnull String taskName,
            @Nonnull Function<String, StatefulScheduledTaskExecutionResult<String>> body
    ) {
        requireNonNull(taskName, "taskName");
        requireNonNull(body, "body");

        return new SimpleStatefulScheduledTask<String>(
                ScheduledTaskIdentity.of(taskName),
                body,
                NoopStatefulScheduledTaskStateTransformer.getInstance()
        );
    }

    @Nonnull
    @Override
    public ScheduledTaskIdentity getIdentity() {
        return identity;
    }

    @Nonnull
    @Override
    public StatefulScheduledTaskExecutionResult<StateT> execute(@Nullable StateT state) {
        return body.apply(state);
    }

    @Nonnull
    @Override
    public StatefulScheduledTaskStateTransformer<StateT> getTransformer() {
        return transformer;
    }
}

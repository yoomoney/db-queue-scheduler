package ru.yoomoney.tech.dbqueue.scheduler.models;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Simple implementation of a scheduled task.
 *
 * <p>Introduced to provide simple to use library API
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 29.10.2021
 */
public class SimpleScheduledTask implements ScheduledTask {
    /**
     * Scheduled task identity
     */
    private final ScheduledTaskIdentity identity;

    /**
     * Task body that should be executed periodically
     */
    private final Supplier<ScheduledTaskExecutionResult> body;

    private SimpleScheduledTask(@Nonnull ScheduledTaskIdentity identity, 
                                @Nonnull Supplier<ScheduledTaskExecutionResult> body) {
        this.identity = requireNonNull(identity, "identity");
        this.body = requireNonNull(body, "body");
    }

    /**
     * Creates scheduled task by name and execution body
     * 
     * @param taskName scheduled task name
     * @param body job that should be executed
     * @return prepared scheduled task
     */
    public static SimpleScheduledTask create(@Nonnull String taskName, @Nonnull Supplier<ScheduledTaskExecutionResult> body) {
        requireNonNull(taskName, "taskName");
        requireNonNull(body, "body");
        return new SimpleScheduledTask(ScheduledTaskIdentity.of(taskName), body);
    }

    @Nonnull
    @Override
    public ScheduledTaskIdentity getIdentity() {
        return identity;
    }

    @Nonnull
    @Override
    public ScheduledTaskExecutionResult execute() {
        return body.get();
    }
}

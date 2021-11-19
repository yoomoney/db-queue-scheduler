package ru.yoomoney.tech.dbqueue.scheduler.config.impl;

import ru.yoomoney.tech.dbqueue.scheduler.config.ScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskContext;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Composite listener. It allows combining several listeners into one.
 *
 * Listeners for started events are executed in straight order.
 * Listeners for finished and crashed events are executed in reverse order.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 27.10.2021
 */
public class CompositeScheduledTaskLifecycleListener implements ScheduledTaskLifecycleListener {
    private final List<ScheduledTaskLifecycleListener> listeners;
    private final List<ScheduledTaskLifecycleListener> reverseListeners;

    public CompositeScheduledTaskLifecycleListener(@Nonnull List<ScheduledTaskLifecycleListener> listeners) {
        this.listeners = Collections.unmodifiableList(requireNonNull(listeners, "listeners"));
        this.reverseListeners = new ArrayList<>(listeners);
        Collections.reverse(reverseListeners);
    }

    @Override
    public void started(@Nonnull ScheduledTaskIdentity taskIdentity, @Nonnull ScheduledTaskContext taskContext) {
        listeners.forEach(listener -> listener.started(taskIdentity, taskContext));
    }

    @Override
    public void finished(@Nonnull ScheduledTaskIdentity taskIdentity,
                         @Nonnull ScheduledTaskContext taskContext,
                         @Nonnull ScheduledTaskExecutionResult executionResult,
                         @Nonnull Instant nextExecutionTime,
                         long processTaskTimeInMills) {
        reverseListeners.forEach(listener -> listener.finished(taskIdentity, taskContext, executionResult, nextExecutionTime,
                processTaskTimeInMills));
    }

    @Override
    public void crashed(@Nonnull ScheduledTaskIdentity taskIdentity,
                        @Nonnull ScheduledTaskContext taskContext,
                        @Nullable Throwable exc) {
        reverseListeners.forEach(listener -> listener.crashed(taskIdentity, taskContext, exc));
    }
}

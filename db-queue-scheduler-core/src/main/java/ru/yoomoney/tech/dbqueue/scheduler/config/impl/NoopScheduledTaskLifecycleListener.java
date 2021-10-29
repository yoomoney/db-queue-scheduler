package ru.yoomoney.tech.dbqueue.scheduler.config.impl;

import ru.yoomoney.tech.dbqueue.scheduler.config.ScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;

/**
 * Empty listener for task processing lifecycle.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 27.10.2021
 */
public class NoopScheduledTaskLifecycleListener implements ScheduledTaskLifecycleListener {
    private static final NoopScheduledTaskLifecycleListener INSTANCE = new NoopScheduledTaskLifecycleListener();

    @Override
    public void started(@Nonnull ScheduledTaskIdentity taskIdentity) {
        // do nothing
    }

    @Override
    public void finished(@Nonnull ScheduledTaskIdentity taskIdentity,
                         @Nonnull ScheduledTaskExecutionResult executionResult,
                         @Nonnull Instant nextExecutionTime,
                         long processTaskTimeInMills) {
        // do nothing
    }

    @Override
    public void crashed(@Nonnull ScheduledTaskIdentity taskIdentity, @Nullable Throwable exc) {
        // do nothing
    }

    /**
     * Get empty listener for task processing lifecycle
     *
     * @return flyweight object of empty listener
     */
    public static NoopScheduledTaskLifecycleListener getInstance() {
        return INSTANCE;
    }
}

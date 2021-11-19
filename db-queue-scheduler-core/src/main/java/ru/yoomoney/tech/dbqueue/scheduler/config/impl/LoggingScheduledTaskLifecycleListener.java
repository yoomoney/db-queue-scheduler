package ru.yoomoney.tech.dbqueue.scheduler.config.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.scheduler.config.ScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskContext;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;

/**
 * Listener that writes logs
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 27.10.2021
 */
public class LoggingScheduledTaskLifecycleListener implements ScheduledTaskLifecycleListener {
    private static final Logger log = LoggerFactory.getLogger(LoggingScheduledTaskLifecycleListener.class);

    @Override
    public void started(@Nonnull ScheduledTaskIdentity taskIdentity, @Nonnull ScheduledTaskContext taskContext) {
        log.info("task started: identity={}", taskIdentity.asString());
    }

    @Override
    public void finished(@Nonnull ScheduledTaskIdentity taskIdentity,
                         @Nonnull ScheduledTaskContext taskContext,
                         @Nonnull ScheduledTaskExecutionResult executionResult,
                         @Nonnull Instant nextExecutionTime,
                         long processTaskTimeInMills) {
        if (executionResult.getType() == ScheduledTaskExecutionResult.Type.ERROR) {
            log.error("task failed: identity={}, executionResult={}, nextExecutionTime={}, time={}",
                    taskIdentity.asString(), executionResult.getType(), nextExecutionTime, processTaskTimeInMills);
        } else {
            log.info("task finished: identity={}, executionResult={}, nextExecutionTime={}, time={}",
                    taskIdentity.asString(), executionResult.getType(), nextExecutionTime, processTaskTimeInMills);
        }
    }

    @Override
    public void crashed(@Nonnull ScheduledTaskIdentity taskIdentity,
                        @Nonnull ScheduledTaskContext taskContext,
                        @Nullable Throwable exc) {
        log.error("task crashed: identity={}", taskIdentity.asString(), exc);
    }
}

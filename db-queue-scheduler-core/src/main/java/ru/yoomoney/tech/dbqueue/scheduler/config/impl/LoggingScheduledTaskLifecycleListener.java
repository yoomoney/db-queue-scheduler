package ru.yoomoney.tech.dbqueue.scheduler.config.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.scheduler.config.ScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;

import javax.annotation.Nonnull;
import java.time.Instant;

/**
 * Listener that writes INFO logs
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 27.10.2021
 */
public class LoggingScheduledTaskLifecycleListener implements ScheduledTaskLifecycleListener {
    private final Logger log = LoggerFactory.getLogger(LoggingScheduledTaskLifecycleListener.class);

    @Override
    public void started(@Nonnull ScheduledTaskIdentity taskIdentity) {
        log.info("task started: identity={}", taskIdentity.getTaskName());
    }

    @Override
    public void finished(@Nonnull ScheduledTaskIdentity taskIdentity,
                         @Nonnull ScheduledTaskExecutionResult executionResult,
                         @Nonnull Instant nextExecutionTime,
                         long processTaskTimeInMills) {
        log.info("task finished: identity={}, executionResult={}, processTaskTime={}, nextExecutionTime={}",
                taskIdentity.getTaskName(), executionResult.getType(), processTaskTimeInMills, nextExecutionTime);
    }
}

package ru.yoomoney.tech.dbqueue.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskDefinition;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskManager;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProviderFactory;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;
import ru.yoomoney.tech.dbqueue.scheduler.models.info.ScheduledTaskInfo;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduledTaskSettings;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Standard scheduler implementation
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 19.10.2021
 */
class DefaultScheduler implements Scheduler {
    private static final Logger log = LoggerFactory.getLogger(DefaultScheduler.class);

    private final ScheduledTaskManager scheduledTaskManager;
    private final NextExecutionTimeProviderFactory nextExecutionTimeProviderFactory;

    DefaultScheduler(@Nonnull ScheduledTaskManager scheduledTaskManager,
                     @Nonnull NextExecutionTimeProviderFactory nextExecutionTimeProviderFactory) {
        this.scheduledTaskManager = requireNonNull(scheduledTaskManager, "scheduledTaskManager");
        this.nextExecutionTimeProviderFactory = requireNonNull(nextExecutionTimeProviderFactory, "nextExecutionTimeProviderFactory");
    }

    @Override
    public void schedule(@Nonnull ScheduledTask scheduledTask,
                         @Nonnull ScheduledTaskSettings scheduledTaskSettings) {
        requireNonNull(scheduledTaskSettings, "scheduledTaskSettings");
        requireNonNull(scheduledTask, "scheduledTask");

        NextExecutionTimeProvider executionTimeProvider = nextExecutionTimeProviderFactory
                .createExecutionTimeProvider(scheduledTaskSettings);

        ScheduledTaskDefinition scheduledTaskDefinition = ScheduledTaskDefinition.builder()
                .withEnabled(scheduledTaskSettings.isEnabled())
                .withFailureSettings(scheduledTaskSettings.getFailureSettings())
                .withScheduledTask(scheduledTask)
                .withNextExecutionTimeProvider(executionTimeProvider)
                .build();

        scheduledTaskManager.schedule(scheduledTaskDefinition);

        log.info("task scheduled: identity={}, settings={}", scheduledTask.getIdentity(), scheduledTaskSettings);
    }

    @Override
    public void reschedule(@Nonnull ScheduledTaskIdentity taskIdentity, @Nonnull Instant nextExecutionTime) {
        requireNonNull(taskIdentity, "taskIdentity");
        requireNonNull(nextExecutionTime, "nextExecutionTime");

        scheduledTaskManager.reschedule(taskIdentity, nextExecutionTime);
        log.info("task rescheduled: identity={}, nextExecutionTime={}", taskIdentity, nextExecutionTime);
    }

    @Override
    public void start() {
        scheduledTaskManager.start();
        log.info("scheduler started");
    }

    @Override
    public void unpause() {
        scheduledTaskManager.unpause();
        log.info("scheduler unpaused");
    }

    @Override
    public void pause() {
        scheduledTaskManager.pause();
        log.info("scheduler paused");
    }

    @Override
    public void shutdown() {
        scheduledTaskManager.shutdown();
        log.info("scheduler shutdown");
    }

    @Override
    public List<ScheduledTaskIdentity> awaitTermination(@Nonnull Duration timeout) {
        return scheduledTaskManager.awaitTermination(timeout);
    }

    @Override
    public List<ScheduledTaskInfo> getScheduledTaskInfo() {
        return scheduledTaskManager.getScheduledTaskInfo();
    }
}

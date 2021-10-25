package ru.yoomoney.tech.dbqueue.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskDefinition;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskManager;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProviderFactory;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduledTaskSettings;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Scheduler manages {@link ScheduledTask}s for periodic execution - configures, registers, starts and pauses.
 *
 * <p>Scheduler guarantees exactly-once task execution that means that any registered tasks is executed exactly once per each
 * scheduled time in spite of any numbers of working application nodes.
 *
 * <p>Scheduler uses RDBMS for persisting registered tasks. Currently, scheduled backed on {@code db-queue} library.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 19.10.2021
 */
public class Scheduler {
    private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

    private final ScheduledTaskManager scheduledTaskManager;
    private final NextExecutionTimeProviderFactory nextExecutionTimeProviderFactory;

    Scheduler(@Nonnull ScheduledTaskManager scheduledTaskManager,
              @Nonnull NextExecutionTimeProviderFactory nextExecutionTimeProviderFactory) {
        this.scheduledTaskManager = requireNonNull(scheduledTaskManager, "scheduledTaskRegistry");
        this.nextExecutionTimeProviderFactory = requireNonNull(nextExecutionTimeProviderFactory, "nextExecutionTimeProviderFactory");
    }

    /**
     * Register a task for periodic executions
     *
     * @param scheduledTaskIdentity unique identity of a scheduled task
     * @param scheduledTaskSettings settings of the scheduled task
     * @param scheduledTask task for periodic executions
     * @throws RuntimeException if any scheduled task with the same identity already registered
     */
    public void schedule(@Nonnull ScheduledTaskIdentity scheduledTaskIdentity,
                         @Nonnull ScheduledTaskSettings scheduledTaskSettings,
                         @Nonnull ScheduledTask scheduledTask) {
        requireNonNull(scheduledTaskIdentity, "scheduledTaskIdentity");
        requireNonNull(scheduledTaskSettings, "scheduledTaskSettings");
        requireNonNull(scheduledTask, "scheduledTask");

        NextExecutionTimeProvider executionTimeProvider = nextExecutionTimeProviderFactory
                .createExecutionTimeProvider(scheduledTaskSettings.getScheduleSettings());

        ScheduledTaskDefinition scheduledTaskDefinition = ScheduledTaskDefinition.builder()
                .withScheduledTaskIdentity(scheduledTaskIdentity)
                .withScheduledTask(scheduledTask)
                .withNextExecutionTimeProvider(executionTimeProvider)
                .build();

        scheduledTaskManager.register(scheduledTaskDefinition);

        log.debug("task scheduled: scheduledTaskIdentity={}, scheduledTaskSettings={}, scheduledTaskDefinition={}",
                scheduledTaskIdentity, scheduledTaskSettings, scheduledTaskDefinition);
    }
}

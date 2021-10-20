package ru.yoomoney.tech.dbqueue.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskDefinition;
import ru.yoomoney.tech.dbqueue.scheduler.internal.ScheduledTaskRegistry;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionTimeProviderFactory;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduledTaskSettings;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Scheduler manages {@link ScheduledTask}s for periodic execution - configures, registers, starts and pauses.
 *
 * <p>Scheduler guarantees exactly-once task execution that means any registered tasks is executed exactly once per each
 * scheduled time in spite of any numbers of working application nodes.
 *
 * <p>Scheduler uses RDBMS for persisting registered tasks. Currently, scheduled backed on {@code db-queue} library.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 19.10.2021
 */
public class Scheduler {
    private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

    private final ScheduledTaskRegistry scheduledTaskRegistry;
    private final NextExecutionTimeProviderFactory nextExecutionTimeProviderFactory;

    Scheduler(@Nonnull ScheduledTaskRegistry scheduledTaskRegistry,
              @Nonnull NextExecutionTimeProviderFactory nextExecutionTimeProviderFactory) {
        this.scheduledTaskRegistry = requireNonNull(scheduledTaskRegistry, "scheduledTaskRegistry");
        this.nextExecutionTimeProviderFactory = requireNonNull(nextExecutionTimeProviderFactory, "nextExecutionTimeProviderFactory");
    }

    /**
     * Register a task for periodic executions
     *
     * @param scheduledTask task for periodic executions
     * @param scheduledTaskSettings settings of the scheduled task
     * @throws RuntimeException if any scheduled task with the same name already registered
     */
    public void scheduleTask(@Nonnull ScheduledTask scheduledTask,
                             @Nonnull ScheduledTaskSettings scheduledTaskSettings) {
        requireNonNull(scheduledTask, "scheduledTask");
        requireNonNull(scheduledTaskSettings, "scheduledTaskSettings");

        NextExecutionTimeProvider executionTimeProvider = nextExecutionTimeProviderFactory
                .createExecutionTimeProvider(scheduledTaskSettings.getScheduleSettings());

        ScheduledTaskDefinition scheduledTaskDefinition = ScheduledTaskDefinition.builder()
                .withName(scheduledTaskSettings.getName())
                .withScheduledTask(scheduledTask)
                .withNextExecutionTimeProvider(executionTimeProvider)
                .build();

        scheduledTaskRegistry.register(scheduledTaskDefinition);

        log.debug("task scheduled: scheduledTaskSettings={}, scheduledTaskDefinition={}", scheduledTaskSettings,
                scheduledTaskDefinition);
    }
}

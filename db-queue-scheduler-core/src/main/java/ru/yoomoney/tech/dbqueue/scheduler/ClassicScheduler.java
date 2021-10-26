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
 * Standard scheduler implementation
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 19.10.2021
 */
class ClassicScheduler implements Scheduler {
    private static final Logger log = LoggerFactory.getLogger(ClassicScheduler.class);

    private final ScheduledTaskManager scheduledTaskManager;
    private final NextExecutionTimeProviderFactory nextExecutionTimeProviderFactory;

    ClassicScheduler(@Nonnull ScheduledTaskManager scheduledTaskManager,
                     @Nonnull NextExecutionTimeProviderFactory nextExecutionTimeProviderFactory) {
        this.scheduledTaskManager = requireNonNull(scheduledTaskManager, "scheduledTaskRegistry");
        this.nextExecutionTimeProviderFactory = requireNonNull(nextExecutionTimeProviderFactory, "nextExecutionTimeProviderFactory");
    }

    @Override
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
                .withMaxExecutionLockInterval(scheduledTaskSettings.getMaxExecutionLockInterval())
                .withScheduledTask(scheduledTask)
                .withNextExecutionTimeProvider(executionTimeProvider)
                .build();

        scheduledTaskManager.register(scheduledTaskDefinition);

        log.debug("task scheduled: scheduledTaskIdentity={}, scheduledTaskSettings={}, scheduledTaskDefinition={}",
                scheduledTaskIdentity, scheduledTaskSettings, scheduledTaskDefinition);
    }

    @Override
    public void start() {
        scheduledTaskManager.start();
    }

    @Override
    public void unpause() {
        scheduledTaskManager.unpause();
    }

    @Override
    public void pause() {
        scheduledTaskManager.pause();
    }
}

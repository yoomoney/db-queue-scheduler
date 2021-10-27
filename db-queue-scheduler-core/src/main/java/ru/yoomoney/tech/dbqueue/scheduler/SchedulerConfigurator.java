package ru.yoomoney.tech.dbqueue.scheduler;

/**
 * Entry point for the library configuration.
 *
 * <p>Core library does not contain any implementations of the configurator. Use specific modules
 * such as {@code db-queue-scheduler-spring}, please.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
public interface SchedulerConfigurator {
    /**
     * Configures {@link Scheduler}.
     * @return configured scheduler
     */
    Scheduler configure();
}

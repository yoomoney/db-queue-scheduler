package ru.yoomoney.tech.dbqueue.scheduler.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yoomoney.tech.dbqueue.scheduler.Scheduler;
import ru.yoomoney.tech.dbqueue.scheduler.SpringSchedulerConfigurator;
import ru.yoomoney.tech.dbqueue.scheduler.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.scheduler.config.impl.LoggingScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.SimpleScheduledTask;
import ru.yoomoney.tech.dbqueue.scheduler.settings.RetrySettings;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduleSettings;
import ru.yoomoney.tech.dbqueue.scheduler.settings.ScheduledTaskSettings;

import java.time.Duration;
import java.time.ZoneId;

/**
 * Simple example of the library configuration via {@code spring framework}.
 *
 * <p>NOTE: the library has not had a preconfigured spring boot configuration yet. That's why the example shows a pure
 * library configuration.
 */
@Configuration
@SpringBootApplication
@EnableConfigurationProperties
public class ExampleApplication {
    private static final Logger log = LoggerFactory.getLogger(ExampleApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(ExampleApplication.class, args);
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    Scheduler scheduler(JdbcOperations jdbcOperations,
                        TransactionOperations transactionOperations) {
        Scheduler scheduler = new SpringSchedulerConfigurator()
                .withTableName("scheduled_tasks")
                .withDatabaseDialect(DatabaseDialect.H2)
                .withJdbcOperations(jdbcOperations)
                .withTransactionOperations(transactionOperations)
                .withScheduledTaskLifecycleListener(new LoggingScheduledTaskLifecycleListener())
                .configure();

        // Scheduled tasks might be spring beans. To make the example short, the tasks are created here.

        // cron example
        scheduler.schedule(
                createScheduledTaskExample("cron-example"),
                ScheduledTaskSettings.builder()
                        .withFailureSettings(RetrySettings.linear(Duration.ofHours(1L)))
                        .withScheduleSettings(ScheduleSettings.cron("*/30 * * * * *", ZoneId.systemDefault()))
                        .build()
        );

        // fixed-delay example
        scheduler.schedule(
                createScheduledTaskExample("fixed-delay-example"),
                ScheduledTaskSettings.builder()
                        .withFailureSettings(RetrySettings.linear(Duration.ofHours(1L)))
                        .withScheduleSettings(ScheduleSettings.fixedRate(Duration.ofMinutes(1L)))
                        .build()
        );

        // fixed-rate example
        scheduler.schedule(
                createScheduledTaskExample("fixed-rate-example"),
                ScheduledTaskSettings.builder()
                        .withFailureSettings(RetrySettings.linear(Duration.ofHours(1L)))
                        .withScheduleSettings(ScheduleSettings.fixedRate(Duration.ofMinutes(1L)))
                        .build()
        );

        return scheduler;
    }

    private ScheduledTask createScheduledTaskExample(String taskName) {
        return SimpleScheduledTask.create(taskName, () -> {
            log.info("execute(): taskName={}", taskName);
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                throw new RuntimeException("thread interrupted: taskName=" + taskName, e);
            }
            return ScheduledTaskExecutionResult.success();
        });
    }
}

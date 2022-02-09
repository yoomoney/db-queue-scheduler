package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionDelayProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.chrono.ChronoZonedDateTime;

import static java.util.Objects.requireNonNull;

/**
 * Provider of delays of next executions according to predefined cron expression.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 22.10.2021
 */
public class CronNextExecutionDelayProvider implements NextExecutionDelayProvider {
    private final ExecutionTime executionTime;
    private final ZoneId zoneId;
    private final Clock clock;

    /**
     * Constructor
     *
     * @param cronExpression cron expression
     * @param zoneId time zone for evaluations of passed cron expression
     * @throws IllegalArgumentException if cron expression invalid
     */
    public CronNextExecutionDelayProvider(@Nonnull String cronExpression, @Nonnull ZoneId zoneId) {
        this(cronExpression, zoneId, Clock.systemDefaultZone());
    }

    public CronNextExecutionDelayProvider(@Nonnull String cronExpression, @Nonnull ZoneId zoneId, @Nonnull Clock clock) {
        this.executionTime = buildCronExecutionTime(requireNonNull(cronExpression, "cronExpression"));
        this.zoneId = requireNonNull(zoneId, "zoneId");
        this.clock = requireNonNull(clock, "clock");
    }

    private ExecutionTime buildCronExecutionTime(String cronExpression) {
        CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING);
        CronParser cronParser = new CronParser(cronDefinition);
        Cron cron = cronParser.parse(cronExpression);
        cron.validate();
        return ExecutionTime.forCron(cron);
    }

    @Override
    public Duration getNextExecutionDelay(@Nonnull ScheduledTaskExecutionContext executionContext) {
        requireNonNull(executionContext, "executionContext");

        Instant baseTime = executionContext.getExecutionStartTime().orElseGet(clock::instant)
                .plus(executionContext.getProcessingTime().orElse(Duration.ZERO));

        Instant nextExecutionTime = executionTime.nextExecution(baseTime.atZone(zoneId))
                .map(ChronoZonedDateTime::toInstant)
                .orElse(Instant.MAX);
        return Duration.between(baseTime, nextExecutionTime);
    }
}

package ru.yoomoney.tech.dbqueue.scheduler.config.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 28.10.2021
 */
class LoggingScheduledTaskLifecycleListenerTest {
    public static final Path LOG_PATH = Paths.get("target/scheduled-task-listener.log");

    private final LoggingScheduledTaskLifecycleListener listener = new LoggingScheduledTaskLifecycleListener();
    private final  ScheduledTaskIdentity identity = ScheduledTaskIdentity.of("taskIdentity");

    @BeforeEach
    public void setUp() throws IOException {
        if (Files.exists(LOG_PATH)) {
            Files.write(LOG_PATH, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        }
    }

    @Test
    public void should_log_start_event() throws IOException {
        // when
        listener.started(identity);

        // then
        assertThat(
                Files.readAllLines(LOG_PATH),
                equalTo(List.of("INFO  [LoggingScheduledTaskLifecycleListener] task started: identity=taskIdentity"))
        );
    }

    @Test
    public void should_log_finish_event_when_execution_result_success() throws IOException {
        // given
        Instant nextExecutionTime = LocalDateTime.of(2010, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC);

        // when
        listener.finished(identity, ScheduledTaskExecutionResult.success(), nextExecutionTime, 800L);

        // then
        assertThat(
                Files.readAllLines(LOG_PATH),
                equalTo(List.of("INFO  [LoggingScheduledTaskLifecycleListener] task finished: identity=taskIdentity, " +
                        "executionResult=SUCCESS, nextExecutionTime=2010-01-01T00:00:00Z, time=800"))
        );
    }

    @Test
    public void should_log_finish_event_when_execution_result_error() throws IOException {
        // given
        Instant nextExecutionTime = LocalDateTime.of(2010, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC);

        // when
        listener.finished(identity, ScheduledTaskExecutionResult.error(), nextExecutionTime, 800L);

        // then
        assertThat(
                Files.readAllLines(LOG_PATH),
                equalTo(List.of("ERROR [LoggingScheduledTaskLifecycleListener] task failed: identity=taskIdentity, " +
                        "executionResult=ERROR, nextExecutionTime=2010-01-01T00:00:00Z, time=800"))
        );
    }

    @Test
    public void should_log_crash_event() throws IOException {
        // when
        listener.crashed(identity, null);

        // then
        assertThat(
                Files.readAllLines(LOG_PATH),
                equalTo(List.of("ERROR [LoggingScheduledTaskLifecycleListener] task crashed: identity=taskIdentity"))
        );
    }
}
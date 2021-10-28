package ru.yoomoney.tech.dbqueue.scheduler.brave;

import brave.Tracing;
import brave.propagation.TraceContext;
import org.junit.jupiter.api.Test;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;

import java.time.Instant;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 28.10.2021
 */
class TracingScheduledTaskLifecycleListenerTest {
    private final Tracing tracing = Tracing.newBuilder().build();

    @Test
    public void should_create_new_span() {
        // given
        TracingScheduledTaskLifecycleListener listener = new TracingScheduledTaskLifecycleListener(tracing);
        ScheduledTaskIdentity scheduledTaskIdentity = ScheduledTaskIdentity.of("scheduled_task");

        // when
        listener.started(scheduledTaskIdentity);

        // then
        TraceContext traceContext = tracing.tracer().currentSpan().context();
        assertThat(traceContext.traceId(), notNullValue());
        assertThat(traceContext.parentIdString(), equalTo(null));

        listener.finished(scheduledTaskIdentity, ScheduledTaskExecutionResult.success(), Instant.now(), 0L);
    }
}
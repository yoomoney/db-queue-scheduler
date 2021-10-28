package ru.yoomoney.tech.dbqueue.scheduler.brave;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.TraceContext;
import ru.yoomoney.tech.dbqueue.scheduler.config.ScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Task lifecycle listener with brave tracing support
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 27.10.2021
 */
public class TracingScheduledTaskLifecycleListener implements ScheduledTaskLifecycleListener {
    private static final ThreadLocal<SpanAndScope> threadLocalSpan = new ThreadLocal<>();

    private final Tracing tracing;

    public TracingScheduledTaskLifecycleListener(@Nonnull Tracing tracing) {
        this.tracing = requireNonNull(tracing, "tracing");
    }

    @Override
    public void started(@Nonnull ScheduledTaskIdentity taskIdentity) {
        Tracer tracer = tracing.tracer();
        TraceContext traceContext = tracer.newTrace().context().toBuilder().build();
        Span span = tracer.toSpan(traceContext)
                .name("scheduler " + taskIdentity.asString())
                .tag("scheduler.task", taskIdentity.asString())
                .kind(Span.Kind.CONSUMER)
                .start();
        threadLocalSpan.set(new SpanAndScope(span, tracer.withSpanInScope(span)));
    }

    @Override
    public void finished(@Nonnull ScheduledTaskIdentity taskIdentity,
                         @Nonnull ScheduledTaskExecutionResult executionResult,
                         @Nonnull Instant nextExecutionTime,
                         long processTaskTimeInMills) {
        SpanAndScope spanAndScope = threadLocalSpan.get();
        if (spanAndScope == null) {
            return;
        }
        threadLocalSpan.remove();
        spanAndScope.getSpanInScope().close();
        spanAndScope.getSpan().finish();
    }

    @Override
    public void crashed(@Nonnull ScheduledTaskIdentity taskIdentity, @Nullable Throwable exc) {
        SpanAndScope spanAndScope = threadLocalSpan.get();
        if (spanAndScope == null) {
            return;
        }
        spanAndScope.getSpan().error(exc);
    }

    private static final class SpanAndScope {
        private final Span span;
        private final Tracer.SpanInScope spanInScope;

        SpanAndScope(@Nonnull Span span, @Nonnull Tracer.SpanInScope spanInScope) {
            this.span = requireNonNull(span);
            this.spanInScope = requireNonNull(spanInScope);
        }

        public Span getSpan() {
            return span;
        }

        public Tracer.SpanInScope getSpanInScope() {
            return spanInScope;
        }
    }
}

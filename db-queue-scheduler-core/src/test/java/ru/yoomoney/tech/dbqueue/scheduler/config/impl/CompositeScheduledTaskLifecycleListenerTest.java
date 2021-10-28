package ru.yoomoney.tech.dbqueue.scheduler.config.impl;

import org.junit.jupiter.api.Test;
import ru.yoomoney.tech.dbqueue.scheduler.config.ScheduledTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskExecutionResult;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 28.10.2021
 */
class CompositeScheduledTaskLifecycleListenerTest {

    @Test
    public void should_handle_started_event_in_order() {
        // given
        List<String> events = new ArrayList<>();
        DummyScheduledTaskLifecycleListener listener1 = new DummyScheduledTaskLifecycleListener("1", events);
        DummyScheduledTaskLifecycleListener listener2 = new DummyScheduledTaskLifecycleListener("2", events);
        CompositeScheduledTaskLifecycleListener listener = new CompositeScheduledTaskLifecycleListener(List.of(
                listener1, listener2));

        // when
        listener.started(ScheduledTaskIdentity.of("task_name"));

        // then
        assertThat(events, equalTo(List.of("1:started", "2:started")));
    }

    @Test
    public void should_handle_finished_event_in_reverse_order() {
        // given
        List<String> events = new ArrayList<>();
        DummyScheduledTaskLifecycleListener listener1 = new DummyScheduledTaskLifecycleListener("1", events);
        DummyScheduledTaskLifecycleListener listener2 = new DummyScheduledTaskLifecycleListener("2", events);
        CompositeScheduledTaskLifecycleListener listener = new CompositeScheduledTaskLifecycleListener(List.of(
                listener1, listener2));

        // when
        listener.finished(ScheduledTaskIdentity.of("task_name"), ScheduledTaskExecutionResult.success(), Instant.now(), 0L);

        // then
        assertThat(events, equalTo(List.of("2:finished", "1:finished")));
    }

    private static class DummyScheduledTaskLifecycleListener implements ScheduledTaskLifecycleListener {
        private final String id;
        private final List<String> events;

        DummyScheduledTaskLifecycleListener(String id, List<String> events) {
            this.id = id;
            this.events = events;
        }

        @Override
        public void started(@Nonnull ScheduledTaskIdentity taskIdentity) {
            events.add(id + ":started");
        }

        @Override
        public void finished(@Nonnull ScheduledTaskIdentity taskIdentity,
                             @Nonnull ScheduledTaskExecutionResult executionResult,
                             @Nonnull Instant nextExecutionTime, long processTaskTimeInMills) {
            events.add(id + ":finished");
        }
    }
}
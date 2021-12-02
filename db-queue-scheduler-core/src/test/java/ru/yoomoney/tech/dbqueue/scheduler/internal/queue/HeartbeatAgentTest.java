package ru.yoomoney.tech.dbqueue.scheduler.internal.queue;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.11.2021
 */
class HeartbeatAgentTest {

    @Test
    void should_do_heartbeat() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        HeartbeatAgent heartbeatAgent = new HeartbeatAgent("name", Duration.ofMillis(100L), () -> {
            counter.incrementAndGet();
            throw new RuntimeException("fail");
        });

        Thread.sleep(200L);
        assertThat(counter.get(), equalTo(0));

        heartbeatAgent.start();
        Thread.sleep(500L);
        heartbeatAgent.stop();

        assertThat(counter.get(), greaterThanOrEqualTo(4));
        assertThat(counter.get(), lessThanOrEqualTo(6));
    }

    @Test
    void should_release_thread() throws InterruptedException {
        AtomicReference<Thread> threadRef = new AtomicReference<>();
        HeartbeatAgent heartbeatAgent = new HeartbeatAgent("name", Duration.ofMinutes(10L), () ->
                threadRef.set(Thread.currentThread()));

        heartbeatAgent.start();
        Thread.sleep(100L);
        heartbeatAgent.stop();

        Thread.sleep(100L);
        assertThat(threadRef.get().isAlive(), equalTo(false));
    }
}
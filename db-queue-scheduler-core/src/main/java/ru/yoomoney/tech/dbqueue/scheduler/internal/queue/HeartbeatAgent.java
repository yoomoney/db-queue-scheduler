package ru.yoomoney.tech.dbqueue.scheduler.internal.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Simple implementation of a heartbeat agent
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.11.2021
 */
class HeartbeatAgent {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatAgent.class);

    private final String name;
    private final Duration heartbeatInterval;
    private final Runnable heartbeatAction;
    private volatile boolean isTaskRunning;

    HeartbeatAgent(@Nonnull String name,
                   @Nonnull Duration heartbeatInterval,
                   @Nonnull Runnable heartbeatAction) {
        this.name = requireNonNull(name, "name");
        this.heartbeatInterval = requireNonNull(heartbeatInterval, "heartbeatInterval");
        this.heartbeatAction = requireNonNull(heartbeatAction, "heartbeatAction");
        this.isTaskRunning = false;
    }

    /**
     * Starts heart beating
     */
    public void start() {
        if (isTaskRunning) {
            throw new RuntimeException("unexpected agent state. the previous execution must be finished: name=" + name);
        }
        isTaskRunning = true;
        // tasks are rarely executed
        Thread thread = new Thread(this::doHeartbeats);
        thread.setName("heartbeat-agent-" + name);
        thread.start();
    }

    private void doHeartbeats() {
        while (isTaskRunning) {
            try {
                heartbeatAction.run();
            } catch (RuntimeException ex) {
                log.warn("failed to run heartbeat action. that might lead to race conditions: name={}", name, ex);
            }
            try {
                Thread.sleep(heartbeatInterval.toMillis());
            } catch (InterruptedException ex) {
                log.info("agent thread interrupted: name={}", name, ex);
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Stop heart beating
     */
    public void stop() {
        isTaskRunning = false;
    }
}

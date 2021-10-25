package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Context of a scheduled task execution.
 *
 * <p>Mutable and not thread-safe.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 20.10.2021
 */
@NotThreadSafe
public class ScheduledTaskExecutionContext {
    @Nullable
    private Instant lastExecutionStartTime;
    @Nullable
    private Instant lastExecutionFinishTime;

    public void setLastExecutionStartTime(@Nonnull Instant lastExecutionStartTime) {
        this.lastExecutionStartTime = requireNonNull(lastExecutionStartTime, "lastExecutionStartTime");
    }

    public void setLastExecutionFinishTime(@Nonnull Instant lastExecutionFinishTime) {
        this.lastExecutionFinishTime = requireNonNull(lastExecutionFinishTime, "lastExecutionFinishTime");
    }

    @Nonnull
    public Optional<Instant> getLastExecutionStartTime() {
        return Optional.ofNullable(lastExecutionStartTime);
    }

    @Nonnull
    public Optional<Instant> getLastExecutionFinishTime() {
        return Optional.ofNullable(lastExecutionFinishTime);
    }

    @Override
    public String toString() {
        return "ScheduledTaskExecutionContext{" +
                "lastExecutionStartTime=" + lastExecutionStartTime +
                ", lastExecutionFinishTime=" + lastExecutionFinishTime +
                '}';
    }
}

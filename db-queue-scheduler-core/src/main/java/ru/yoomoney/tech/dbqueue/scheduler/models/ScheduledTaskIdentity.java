package ru.yoomoney.tech.dbqueue.scheduler.models;

import javax.annotation.Nonnull;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Unique identity of a scheduled task
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
public class ScheduledTaskIdentity {
    /**
     * Scheduled task name
     */
    @Nonnull
    private final String taskName;

    private ScheduledTaskIdentity(@Nonnull String taskName) {
        this.taskName = requireNonNull(taskName, "taskName");
    }

    /**
     * Builds scheduled task identity by taskName
     */
    @Nonnull
    public static ScheduledTaskIdentity of(@Nonnull String taskName) {
        requireNonNull(taskName, "taskName");
        return new ScheduledTaskIdentity(taskName);
    }

    @Nonnull
    public String getTaskName() {
        return taskName;
    }

    /**
     * Get flat string identity representation
     *
     * @return representation of the identity
     */
    @Nonnull
    public String asString() {
        return taskName;
    }

    @Override
    public String toString() {
        return "ScheduledTaskIdentity{" +
                "taskName='" + taskName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ScheduledTaskIdentity)) {
            return false;
        }
        ScheduledTaskIdentity that = (ScheduledTaskIdentity) obj;
        return taskName.equals(that.taskName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskName);
    }
}

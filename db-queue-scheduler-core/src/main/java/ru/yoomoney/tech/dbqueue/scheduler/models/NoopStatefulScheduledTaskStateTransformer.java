package ru.yoomoney.tech.dbqueue.scheduler.models;

import javax.annotation.Nullable;

/**
 * Default scheduled task state transformer which performs no transformation and returns the same string as in the raw payload.
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 17.11.2021
 */
public class NoopStatefulScheduledTaskStateTransformer implements StatefulScheduledTaskStateTransformer<String> {

    private static final NoopStatefulScheduledTaskStateTransformer INSTANCE = new NoopStatefulScheduledTaskStateTransformer();

    /**
     * Get payload scheduled task state transformer.
     *
     * @return Singleton of transformer.
     */
    public static NoopStatefulScheduledTaskStateTransformer getInstance() {
        return INSTANCE;
    }

    @Nullable
    @Override
    public String toObject(@Nullable String state) {
        return state;
    }

    @Nullable
    @Override
    public String fromObject(@Nullable String state) {
        return state;
    }
}

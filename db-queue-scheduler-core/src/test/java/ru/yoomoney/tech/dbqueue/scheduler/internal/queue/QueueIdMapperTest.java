package ru.yoomoney.tech.dbqueue.scheduler.internal.queue;

import org.junit.jupiter.api.Test;
import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;
import ru.yoomoney.tech.dbqueue.settings.QueueId;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 02.11.2021
 */
class QueueIdMapperTest {
    private final QueueIdMapper queueIdMapper = new QueueIdMapper();

    @Test
    void should_map_QueueId_to_ScheduledTaskIdentity() {
        QueueId queueId = new QueueId("queueId");

        assertThat(queueIdMapper.toScheduledTaskIdentity(queueId), equalTo(ScheduledTaskIdentity.of("queueId")));
    }

    @Test
    void should_map_ScheduledTaskIdentity_to_QueueId() {
        ScheduledTaskIdentity scheduledTaskIdentity = ScheduledTaskIdentity.of("queueId");

        assertThat(queueIdMapper.toQueueId(scheduledTaskIdentity), equalTo(new QueueId("queueId")));
    }
}

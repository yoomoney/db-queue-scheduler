CREATE TABLE scheduled_tasks (
   id                BIGSERIAL PRIMARY KEY,
   queue_name        VARCHAR(100) NOT NULL,
   payload           VARCHAR(100),
   created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),
   next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),
   attempt           INTEGER                  DEFAULT 0,
   reenqueue_attempt INTEGER                  DEFAULT 0,
   total_attempt     INTEGER                  DEFAULT 0
);
CREATE INDEX scheduled_tasks_name_time_desc_idx
  ON scheduled_tasks (queue_name, next_process_at, id DESC);


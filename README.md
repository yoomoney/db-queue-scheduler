[![Build Status](https://travis-ci.com/yoomoney/db-queue-scheduler.svg?branch=master)](https://travis-ci.com/github/yoomoney/db-queue-scheduler/branches)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Download](https://img.shields.io/badge/Download-latest)](https://search.maven.org/artifact/ru.yoomoney.tech/db-queue-scheduler)

# Database Scheduler

Library provides periodic task executions on top of [db-queue](https://github.com/yoomoney/db-queue) library.

Project uses [Semantic Versioning](http://semver.org/).

Library is available on [Maven Central](https://search.maven.org/).

```
implementation 'ru.yoomoney.tech:db-queue-scheduler-core:2.0.0',
               'ru.yoomoney.tech:db-queue-scheduler-spring:2.0.0'
```

## Features

* Persisted periodic tasks;
* **At most once task execution at the same time**;
* Different schedule configuration: cron expressions, fixed rates, fixed delays, dynamic calculations;
* Tracing support;
* Task event listeners to build up monitoring;
* Many other features...

## Database configuration

Library uses [dn-queue](https://github.com/yoomoney/db-queue) to work with a database. 

Library requires a single database table where periodic tasks are stored.

### PostgreSQL DDL

```sql
CREATE TABLE scheduled_tasks (
  id                BIGSERIAL PRIMARY KEY,
  queue_name        TEXT NOT NULL,
  payload           TEXT,
  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),
  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),
  attempt           INTEGER                  DEFAULT 0,
  reenqueue_attempt INTEGER                  DEFAULT 0,
  total_attempt     INTEGER                  DEFAULT 0
);
CREATE UNIQUE INDEX scheduled_tasks_name_queue_name_uq ON scheduled_tasks (queue_name);
```

Look at [db-queue documentation](https://github.com/yoomoney/db-queue#database-configuration) to learn more about 
database configuration.

## Example

```java
Scheduler scheduler = new SpringSchedulerConfigurator()
        .withDatabaseDialect(DatabaseDialect.POSTGRESQL)
        .withTableName("scheduled_tasks")
        .withJdbcOperations(jdbcTemplate)
        .withTransactionOperations(transactionTemplate)
        .configure();

         
ScheduledTask task = SimpleScheduledTask.create(
        "scheduled-task-id",
        () -> {
            System.out.println("Hello World!");
            return ScheduledTaskExecutionResult.success();
        });

ScheduledTaskSettings settings = ScheduledTaskSettings.builder()
        .withFailureSettings(RetrySettings.linear(Duration.ofHours(1L)))
        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(0L)))
        .build()

scheduler.schedule(task, settings);

scheduler.start();
```

See also [runnable example]().

## How to contribute?

Just fork the repo and send us a pull request.

Make sure your branch builds without any warnings/issues.
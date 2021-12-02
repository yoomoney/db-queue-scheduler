[![Build Status](https://travis-ci.com/yoomoney/db-queue-scheduler.svg?branch=master)](https://travis-ci.com/github/yoomoney/db-queue-scheduler/branches)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Download](https://img.shields.io/badge/Download-latest)](https://search.maven.org/artifact/ru.yoomoney.tech/db-queue-scheduler)

# Database Scheduler

Library provides periodic task executions on top of [db-queue](https://github.com/yoomoney/db-queue) library.

Project uses [Semantic Versioning](http://semver.org/).

Library is available on [Maven Central](https://search.maven.org/).

```
implementation 'ru.yoomoney.tech:db-queue-scheduler-core:3.0.0',
               'ru.yoomoney.tech:db-queue-scheduler-spring:3.0.0'
```

## Features

* Persisted periodic tasks;
* **At most once task execution at the same time**;
* Different schedule configuration: cron expressions, fixed rates, fixed delays, dynamic calculations;
* Tracing support;
* Task event listeners to build up monitoring;
* Many other features.

The library provides only (recurring tasks)/(periodic tasks)/(scheduled tasks) functionality -
that allows executing tasks periodically. If you need one-time tasks - tasks that are executed once, please, 
look at [db-queue](https://github.com/yoomoney/db-queue) library.

## Database configuration

The project uses [db-queue](https://github.com/yoomoney/db-queue) to work with a database. 

The library requires a single database table where periodic tasks are stored.

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
CREATE UNIQUE INDEX scheduled_tasks_uq ON scheduled_tasks (queue_name);
```

### MSSQL DDL

```sql
CREATE TABLE scheduled_tasks (
  id                INT IDENTITY(1,1) NOT NULL,
  queue_name        VARCHAR(100) NOT NULL,
  payload           TEXT,
  created_at        DATETIMEOFFSET NOT NULL  DEFAULT SYSDATETIMEOFFSET(),
  next_process_at   DATETIMEOFFSET NOT NULL  DEFAULT SYSDATETIMEOFFSET(),
  attempt           INTEGER NOT NULL         DEFAULT 0,
  reenqueue_attempt INTEGER NOT NULL         DEFAULT 0,
  total_attempt     INTEGER NOT NULL         DEFAULT 0,
  PRIMARY KEY (id)
);
CREATE UNIQUE INDEX scheduled_tasks_uq ON scheduled_tasks (queue_name);
```

### Oracle DDL

```sql
CREATE TABLE scheduled_tasks (
  id                NUMBER(38) NOT NULL PRIMARY KEY,
  queue_name        VARCHAR2(128) NOT NULL,
  payload           CLOB,
  created_at        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  attempt           NUMBER(38)                  DEFAULT 0,
  reenqueue_attempt NUMBER(38)                  DEFAULT 0,
  total_attempt     NUMBER(38)                  DEFAULT 0
);
CREATE UNIQUE INDEX scheduled_tasks_uq ON scheduled_tasks (queue_name);

-- Create sequence and specify its name through scheduler configurator.
CREATE SEQUENCE scheduled_tasks_seq;
```

### H2 Database DDL
```sql
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
CREATE UNIQUE INDEX scheduled_tasks_uq ON scheduled_tasks (queue_name);
```

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
        context -> {
            System.out.println("Hello World!");
            return ScheduledTaskExecutionResult.success();
        });

ScheduledTaskSettings settings = ScheduledTaskSettings.builder()
        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofHours(1L)))
        .withFailureSettings(FailureSettings.none())
        .build()

scheduler.schedule(task, settings);

scheduler.start();
```

See also our [runnable example](/examples/spring/src/main/java/ru/yoomoney/tech/dbqueue/scheduler/example/ExampleApplication.java).

## How it works

### Overview

1. When a new [scheduled task](db-queue-scheduler-core/src/main/java/ru/yoomoney/tech/dbqueue/scheduler/models/ScheduledTask.java) 
   is registered, the library creates a new `db-queue` queue that linked exactly to the registered `scheduled task`;
2. If the `db-queue` queue does not have a task, the library creates a new one and postpones it according to the linked 
   [schedule settings](db-queue-scheduler-core/src/main/java/ru/yoomoney/tech/dbqueue/scheduler/settings/ScheduleSettings.java);
3. Each `db-queue` queue have a [consumer](db-queue-scheduler-core/src/main/java/ru/yoomoney/tech/dbqueue/scheduler/internal/queue/ScheduledTaskQueueConsumer.java) 
   that executes its linked `scheduled task`;
4. When the consumer got a `db-queue` task it does the following steps:
   1. Postponing the next execution time of the `db-queue` task according to the linked [schedule settings](db-queue-scheduler-core/src/main/java/ru/yoomoney/tech/dbqueue/scheduler/settings/ScheduleSettings.java)
   and [failure settings](db-queue-scheduler-core/src/main/java/ru/yoomoney/tech/dbqueue/scheduler/settings/FailureSettings.java);
   2. Starting [HeartbeatAgent](db-queue-scheduler-core/src/main/java/ru/yoomoney/tech/dbqueue/scheduler/internal/queue/HeartbeatAgent.java)
   to prevent concurrent execution of the same task by different application nodes in case of a time-consuming execution; 
   3. Executing the linked `scheduled task`;
   4. Postponing `db-queue` task according to the result of the last execution of the linked `scheduled task`. 

### Schedule and failure settings 

The library lets configure [`ScheduleSettings`](db-queue-scheduler-core/src/main/java/ru/yoomoney/tech/dbqueue/scheduler/settings/ScheduleSettings.java)
and [`FailureSettings`](db-queue-scheduler-core/src/main/java/ru/yoomoney/tech/dbqueue/scheduler/settings/FailureSettings.java).

`FailureSettings` is applied each time when an execution result is `ERROR` and the following conditions are true:

1. The execution task attempts since the last successful one is less than [`FailureSettings.maxAttempts`](https://github.com/yoomoney/db-queue-scheduler/blob/feature/none-failure-settings/db-queue-scheduler-core/src/main/java/ru/yoomoney/tech/dbqueue/scheduler/settings/FailureSettings.java#L37);
2. [`ScheduleSettings.CronSettings`](db-queue-scheduler-core/src/main/java/ru/yoomoney/tech/dbqueue/scheduler/settings/ScheduleSettings.java#L137) 
   is not configured, or the next execution time computed via `FailureSettings` is earlier than the one computed via `ScheduleSettings.CronSettings`.

## How to contribute?

Just fork the repo and send us a pull request.

Make sure your branch builds without any warnings/issues.
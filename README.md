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

* Persisted periodic stateful and stateless tasks;
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
        () -> {
            System.out.println("Hello World!");
            return ScheduledTaskExecutionResult.success();
        });

ScheduledTaskSettings settings = ScheduledTaskSettings.builder()
        .withFailureSettings(FailureSettings.linearBackoff(Duration.ofHours(1L)))
        .withScheduleSettings(ScheduleSettings.fixedDelay(Duration.ofSeconds(0L)))
        .build()

scheduler.schedule(task, settings);

scheduler.start();
```

See also our [runnable example](/examples/spring/src/main/java/ru/yoomoney/tech/dbqueue/scheduler/example/ExampleApplication.java).

## How to contribute?

Just fork the repo and send us a pull request.

Make sure your branch builds without any warnings/issues.
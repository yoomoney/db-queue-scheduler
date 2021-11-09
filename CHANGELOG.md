## [1.1.1](https://github.com/yoomoney/db-queue-scheduler/pull/8) (09-11-2021)

* Race condition on scheduling tasks fixed

## [1.1.0](https://github.com/yoomoney/db-queue-scheduler/pull/6) (08-11-2021)

* Scheduled tasks info feature implemented that allows listing all scheduled tasks details;
* Rescheduling tasks feature implemented that allows forcing task execution manually;

## [1.0.0](https://github.com/yoomoney/db-queue-scheduler/pull/5) (29-10-2021)

* Toggling scheduled tasks feature implemented - `ScheduledTaskSettings.enabled`;
* Improved library api usage experience - look at the breaking changes;
* **breaking changes**
* `ru.yoomoney.tech.dbqueue.scheduler.Scheduler.schedule` method argument order changed;
* `ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTask.getIdentity` method added;
* `scheduledTaskIdentity` field pruned from `ScheduledTaskSettings`.

## [0.3.0](https://github.com/yoomoney/db-queue-scheduler/pull/4) (28-10-2021)

* `ScheduledTaskLifecycleListener` introduced - that helps to observe task execution, eg: tracing, logging, monitoring, etc.
* tracing support, logging support implemented;
* brave tracing support implemented in a separate module - `db-queue-scheduler-brave`.

## [0.2.0](https://github.com/yoomoney/db-queue-scheduler/pull/2) (27-10-2021)

* Library split into 2 modules: `db-queue-scheduler-core` and `db-queue-scheduler-spring` that gives spring library
configuration entry-point;
* Implemented core library API: `Scheduler`, `ScheduledTask`, `ScheduledTaskIdentity` and `ScheduledTaskExecutionResult`;
* Implemented core library functionality - `db-queue` library integration, scheduling tasks feature;
* `CronNextExecutionTimeProvider` implemented

## [0.1.0](https://github.com/yoomoney/db-queue-scheduler/pull/1) (26-10-2021)

* Library skeleton set up: gradle and library-project-plugin, travis, git attributes, license.
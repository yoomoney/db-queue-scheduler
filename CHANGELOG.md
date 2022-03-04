## [3.1.2]() (04-03-2022)

* Next execution delay rounding type changed from flooring to ceiling. It prevents one-second early scheduled tasks
execution. It might be crucial for instant cron tasks.
For instance, a schedule is defined by the following cron expression "0 0 3 * * *" which means "every day at 3 AM".
If the task is executed at 2.59 AM and finished at 2.59 AM, it might be scheduled at 3 AM again.

## [3.1.1]() (09-02-2022)

* Next execution time calculation was fixed. Large time difference between database and application or fast executions
of scheduled tasks might have led to repeated execution. That was possible because the execution time calculation
relied on the application clock and the tasks picking mechanism relied on the database clock.

## [3.1.0]() (02-12-2021)

* `FailRetryType.NONE` added that does not add extra execution attempts in case of a failure -
a task is strictly executed according to the schedule;
* `FailureSettings.maxAttempts` added that boards maximum of extra execution attempts in case of a failure -
if limit is exceeded, a task will be executed according to the schedule;
* `fixedRate`, `fixedDelay` and `cron` schedule settings clarified - `FailureSettings` always override `fixedRate`
and `fixedDelay` but it does not override `cron` in case of the calculated date time via `FailureSettings` is later
than the time calculated by `cron`.

## [3.0.0]() (19-11-2021)

* Scheduled task states added that allows implementing stateful scheduled tasks;
* Scheduled task context added that contains its meta-information;
* **breaking changes**
* `ScheduledTask.execute` method contract changed. Method receives `ScheduledTaskContext context` argument.
* `ScheduledTaskLifecycleListener` method contracts changed. Methods receive `ScheduledTaskContext context` arguments.

## [2.0.1](https://github.com/yoomoney/db-queue-scheduler/pull/10) (11-11-2021)

* H2, MSSQL, Oracle databases support added;
* Library example written;
* Project description written in the `README.md` file

## [2.0.0](https://github.com/yoomoney/db-queue-scheduler/pull/9) (11-11-2021)

* Failure settings added that allows configuring scheduled task retry policies in case of failures;
* **breaking changes**
* `maxExecutionLockInterval` field pruned from `ScheduleSettings`. Use `ScheduleSettings.failureSettings` instead.

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
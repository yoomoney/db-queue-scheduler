### NEXT_VERSION_TYPE=MINOR
### NEXT_VERSION_DESCRIPTION_BEGIN
* `ScheduledTaskLifecycleListener` introduced - that helps to observe task execution, eg: tracing, logging, monitoring, etc.
* tracing support, logging support implemented;
* brave tracing support implemented in a separate module - `db-queue-scheduler-brave`.
### NEXT_VERSION_DESCRIPTION_END
## [0.2.0](https://github.com/yoomoney/db-queue-scheduler/pull/2) (27-10-2021)

* Library split into 2 modules: `db-queue-scheduler-core` and `db-queue-scheduler-spring` that gives spring library
configuration entry-point;
* Implemented core library API: `Scheduler`, `ScheduledTask`, `ScheduledTaskIdentity` and `ScheduledTaskExecutionResult`;
* Implemented core library functionality - `db-queue` library integration, scheduling tasks feature;
* `CronNextExecutionTimeProvider` implemented

## [0.1.0](https://github.com/yoomoney/db-queue-scheduler/pull/1) (26-10-2021)

* Library skeleton set up: gradle and library-project-plugin, travis, git attributes, license.
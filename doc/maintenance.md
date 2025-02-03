## SUP-able functions

| Function | Arguments | Description |
| -------- | --------- | ----------- |
| `account_jobs/1` | `(AccountId)` | |
| `account_jobs/2` | `(AccountId,State)` | |
| `account_workers/1` | `(AccountId)` | |
| `active_jobs/0` |  | |
| `faxbox_jobs/1` | `(FaxboxId)` | |
| `faxbox_jobs/2` | `(FaxboxId,State)` | |
| `flush/0` |  | |
| `force_exit_account_stale_workers/1` | `(AccountId)` | |
| `force_exit_account_stale_workers/2` | `(AccountId,Timestamp)` | |
| `force_exit_all_stale_workers/0` |  | |
| `force_exit_all_stale_workers/1` | `(Timestamp)` | |
| `force_exit_single_stale_worker/2` | `(AccountId,JobId)` | |
| `force_remove_account_stale_workers/1` | `(AccountId)` | |
| `force_remove_account_stale_workers/2` | `(AccountId,Timestamp)` | |
| `force_remove_all_stale_workers/0` |  | |
| `force_remove_all_stale_workers/1` | `(Timestamp)` | |
| `force_remove_single_stale_worker/2` | `(AccountId,JobId)` | |
| `load_smtp_attachment/2` | `(DocId,Filename)` | |
| `locked_jobs/0` |  | |
| `migrate/0` |  | |
| `migrate/1` | `(Account) | ([]) | (_)` | |
| `migrate/2` | `(Account,Options) | (Accounts,Option) | (Accounts,_) | ([],_) | (_,Options)` | |
| `migrate_outbound_faxes/0` |  | |
| `migrate_outbound_faxes/1` | `(Number) | (Options)` | |
| `overview/0` |  | |
| `pending_jobs/0` |  | |
| `refresh_views/0` |  | |
| `remove_account_stale_workers/1` | `(AccountId)` | |
| `remove_account_stale_workers/2` | `(AccountId,Timestamp)` | |
| `remove_all_stale_workers/0` |  | |
| `remove_all_stale_workers/1` | `(Timestamp)` | |
| `remove_single_stale_worker/2` | `(AccountId,JobId)` | |
| `restart_job/1` | `(JobID)` | |
| `update_job/2` | `(JobID,State)` | |
| `worker_info/2` | `(AccountId,JobId)` | |


## `account_workers/1`

List all current account workers in the system. The processes may or may not live, or still in start/restart phase.
The workers are running across all Kazoo nodes that is running the Fax application.

```bash
sup fax_maintenance account_workers {ACCOUNT_ID}
+--------------------------------------------+----------------------------------+----------------+
| Node                                       | Job                              | PID
+============================================+==================================+================+
| kazoo_apps@kazoo.test.com                  | 7b38c3f3549918ee96426a60b0ee53f7 | <0.8088.0>
| kazoo_apps@kazoo.test.com                  | f1c5dbf17d76b49daa65fc5f5000f194 | <0.8094.0>
+--------------------------------------------+----------------------------------+----------------+
```

## `remove_account_stale_workers/1`
## `remove_account_stale_workers/2`
## `remove_all_stale_workers/0`
## `remove_all_stale_workers/1`
## `remove_single_stale_worker/2`

This will attempt to find all stale workers that are older than specified timestamp and remove them.

The `Timestamp` is in UTC Gregorian seconds. If `Timestamp` is not provided, anything older than past 24 hours is being considered older.

Stale worker is a worker that doesn't have a Pid (never started or restarting) in Fax worker cluster state machine, or the process Pid is not running in its node.

If the worker has a Pid, and process associated with that Pid is still running the worker will not be removed.

If the worker chosen for removal, it's job object will get remove from Fax cluster state. If this operation is successful then the job document
in `faxes` database will be updated to put the jib state back into `pending` state. This will result the job to be picked up later again by
`fax_ra` Ra machine.

```bash
sup fax_maintenance remove_account_stale_workers {ACCOUNT_ID}
sup fax_maintenance remove_account_stale_workers {ACCOUNT_ID} {TIMESTAMP}
sup fax_maintenance remove_all_stale_workers
sup fax_maintenance remove_all_stale_workers {TIMESTAMP}
sup fax_maintenance remove_single_stale_worker {ACCOUNT_ID} {JOB_ID}
```

## `force_remove_account_stale_workers/1`
## `force_remove_account_stale_workers/2`
## `force_remove_all_stale_workers/0`
## `force_remove_all_stale_workers/1`
## `force_remove_single_stale_worker/2`

Same as their `remove_*` counterpart, but will forcefully removes worker from cluster state even its worker process is alive.

## `force_exit_account_stale_workers/1`
## `force_exit_account_stale_workers/2`
## `force_exit_all_stale_workers/0`
## `force_exit_all_stale_workers/1`
## `force_exit_single_stale_worker/2`

Same as their `force_remove_*` counterpart, but will forcefully exists the worker process if exists.

## `worker_info/2`

Prints Pid and the node the worker is running.

```bash
sup fax_maintenance worker_info {ACCOUNT_ID} {JOB_ID}
JobId: 7b38c3f3549918ee96426a60b0ee53f7
PID: <0.8088.0>
Node: 'kazoo_apps@kazoo.test.com'
```


## `fax_ra_action workers_count/0`

Prints state of the number of workers and the number of accounts that being processes.

```bash
sup fax_ra_action workers_count
{ok,[{workers_count,8},{account_counts,4}]}
```

## `locked_jobs/0`

List all locked jobs in database.

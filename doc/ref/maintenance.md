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

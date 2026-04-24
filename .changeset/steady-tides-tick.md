---
"jazz-tools": patch
---

Prevent TokioScheduler task pileup under contention.

`schedule_batched_tick` used to clear the debounce flag at the start of the spawned task, before acquiring the core lock. When the lock was held, every caller arriving during that window saw `scheduled=false` and spawned another task, piling up behind the same mutex. The flag is now cleared after the lock is acquired, immediately before `batched_tick` runs, capping the pending queue at one tick while preserving the lost-wakeup fix.

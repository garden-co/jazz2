---
"jazz-tools": patch
---

Add `rollback()` to transaction handles. Calling rollback closes the transaction without committing it, so later writes, reads, commits, or rollbacks on the same transaction fail.

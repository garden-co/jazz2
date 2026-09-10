---
"jazz-tools": patch
---

Preserve optional JSON column nullability in physical storage and version records so explicit null inserts and clearing updates succeed. This pre-freeze descriptor correction requires matching peers and fresh data for affected schemas.

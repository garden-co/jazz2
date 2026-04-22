---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

Batch outbound sync payloads to the server and avoid replaying client-originated rows during query scope bootstrap. This cuts the long first `full` query stalls that could happen after large local write bursts while keeping explicit batch-settlement tracking available for persisted-write waiters.

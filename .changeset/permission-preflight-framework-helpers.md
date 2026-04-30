---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

Add local permission preflight APIs for inserts and updates across the core runtime, native bindings, and TypeScript clients.

- Expose `db.canInsert(...)` and `db.canUpdate(...)` as consultative `true | false | "unknown"` checks for UI gating.
- Add WASM, NAPI, and React Native runtime bindings for the preflight calls.
- Add React/React Native, Vue, and Svelte framework helpers for reactive permission-aware controls.
- Document local-only semantics and the draft-data pattern for insert permission checks.

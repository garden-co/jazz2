---
"jazz-tools": patch
"jazz-wasm": patch
---

Move the browser dedicated-worker dispatch and the main-thread worker bridge into Rust. The `jazz-worker.ts` entry shrinks to a ~10-line bootstrap; the `worker-protocol.ts` types are replaced by a single bincode-encoded `WorkerFrame` enum in `jazz-tools`; the main↔worker transport is unified under the existing `StreamAdapter` abstraction via a new `PostMessageStream`. `JsSyncSender` is deleted. `WorkerClient` (new, in `jazz-wasm`) is the single entry point for the main-thread side. Public runtime API unchanged.

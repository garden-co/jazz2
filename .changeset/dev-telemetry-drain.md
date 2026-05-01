---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
---

Add opt-in development telemetry export for Jazz runtimes and local dev servers. WASM runtimes now buffer spans and logs in Rust only when telemetry is enabled, notify JavaScript through a coalesced drain subscription, and lazy-load client-side OpenTelemetry exporters only after a collector URL is configured.

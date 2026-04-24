---
"jazz-tools": patch
---

`jazzPlugin` now adds `jazz-napi` to `ssr.external` so that SSR builds on Vite-based frameworks (e.g. SvelteKit) don't attempt to bundle the native add-on. Also tightens error handling in `persistAppIdToEnv` to only swallow `ENOENT`; other I/O errors now propagate correctly.

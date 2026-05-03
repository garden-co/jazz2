---
"jazz-tools": patch
---

Embed TypeScript sources into published `.js.map` files via `inlineSources`. Resolves "Sourcemap points to missing source files" warnings in consuming projects, since the `src/` tree is not shipped in the npm tarball.

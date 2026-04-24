# Jazz World Tour example

A local-first tour management app built with Vue + Vite and the Jazz Vite plugin.

## Getting started

```bash
pnpm dev
```

`pnpm dev` starts the Jazz dev server and the Vite dev server together via the Jazz Vite plugin.

### Installing dependencies in the monorepo

This example is excluded from the monorepo workspace install (`"!examples/world-tour"` in `pnpm-workspace.yaml`) to avoid pulling in its heavy dependencies (MapLibre GL) for every contributor. To install its deps, temporarily re-add it:

1. Remove the `"!examples/world-tour"` line from `pnpm-workspace.yaml`
2. Run `pnpm install` from the repo root
3. Restore the exclusion line

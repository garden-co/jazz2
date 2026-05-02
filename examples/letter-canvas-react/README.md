# Jazz Letter Canvas

A local-first React example where anonymous users share a fixed-ratio canvas, drag letters onto it, and see each other's colored cursors in real time.

## What It Shows

- Local-first auth with no login screen.
- A shared `canvases` table with `letters` and `cursors` associated through refs.
- A fixed `16:9` logical canvas so every client sees the same letter positions at any viewport size.
- Rate-limited cursor and letter writes at roughly 20 FPS.
- Client-side smoothing and light prediction to hide network latency.
- Drawer-to-canvas drag previews, including shared draft previews while a user is dragging a new letter.

## Run Locally

From this example directory:

```sh
pnpm dev
```

The app reads `VITE_JAZZ_APP_ID` from `.env.local` when present and connects to `https://v2.sync.jazz.tools/`. If you want an isolated hosted app, create a Jazz app and set:

```sh
VITE_JAZZ_APP_ID=your-app-id
```

Only the app id is exposed to the browser. Admin or backend secrets should stay in local env files and should not be imported by app code.

## Schema And Permissions

- `canvases`: one row per shared canvas URL.
- `letters`: draggable letters associated with a canvas.
- `cursors`: one presence row per user per canvas.

Users can read all rows for a shared canvas, insert their own cursor, and insert or update letters. Cursor updates are intentionally permissive in this demo so presence remains smooth while the sync ordering work is being reviewed upstream.

## URL Model

If the URL has `?canvas=<id>`, the app joins that canvas. Otherwise it creates a new canvas row, writes the generated id into the URL, and waits for the row to sync before enabling interactions.

## Tests

```sh
pnpm test
```

The browser tests cover the canvas shell, drag behavior, shared cursor affordances, QR code, arrow-key forwarding, and the small helper modules that handle rate limiting, motion, cursor visibility, and draft previews.

## Manual Vercel Deploy

This example is set up for manual deployment rather than Git integration. Build locally, refresh Vercel's prebuilt output, then deploy:

```sh
pnpm build
rm -rf .vercel/output/static
mkdir -p .vercel/output/static
cp -R dist/. .vercel/output/static/
vercel deploy --prod --prebuilt --yes
```

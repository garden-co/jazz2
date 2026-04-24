---
"jazz-tools": patch
---

`jazz-tools` CLI now accepts framework-prefixed env vars (`PUBLIC_`, `VITE_`, `NEXT_PUBLIC_`, `EXPO_PUBLIC_`) as fallbacks for `JAZZ_SERVER_URL` and `JAZZ_APP_ID`, matching the names the SvelteKit, Vite, Next.js and Expo plugins already write. `JAZZ_ADMIN_SECRET` remains unprefixed.

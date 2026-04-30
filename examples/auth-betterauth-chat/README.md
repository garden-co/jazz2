# auth-betterauth-chat

A small Next.js example that shows how to integrate [Better Auth](https://www.better-auth.com/) with Jazz.

What it demonstrates:

- A single Next.js app that serves both the UI and Better Auth routes
- Better Auth tables stored in Jazz through `jazz-tools/better-auth-adapter`
- A Jazz backend context using in-memory storage while syncing auth rows to the local sync server
- Better Auth's built-in `jwt` plugin to issue ES256 JWTs and expose a JWKS endpoint
- The `admin` plugin to assign roles (`admin` / `member`) to users
- Fetching the JWT from the Better Auth session and passing it to `JazzProvider`
- Recreating `JazzProvider` on login and logout, while keeping `db.updateAuthToken(...)` only for same-user JWT refresh after auth expiry
- Falling back to local-first auth when no session exists
- Permission-preflight UI gating with `db.canInsert(...)`. Write rules are defined in [permissions.ts](./permissions.ts): `admin` can post to Announcements, local-first/authenticated users can post to the general chat, and generic-chat message ownership is enforced via `$createdBy`.

One default account is seeded on startup: `admin@example.com / admin` with `role = "admin"`.
New sign-ups receive `role = "member"` by default (configured via the `admin` plugin).

## Setup

### 1. Create `.env`

```bash
cp .env.example .env
```

The `.env` file only needs the values already in `.env.example`:

- `NEXT_PUBLIC_APP_ORIGIN` — Next app origin used by Better Auth and Playwright
- `BACKEND_SECRET` — backend secret used by the Better Auth Jazz context
- `NEXT_PUBLIC_CHAT_ID` — general chat room id
- `NEXT_PUBLIC_ANNOUNCEMENTS_CHAT_ID` — announcements chat room id

`NEXT_PUBLIC_JAZZ_APP_ID`, `NEXT_PUBLIC_JAZZ_SERVER_URL`, and the sync server admin secret are
injected automatically by `withJazz` in `next.config.ts` — you do not need to add them.

`pnpm dev` and `pnpm test:e2e` both read the same `.env`.

### 2. Start the Next app

```bash
pnpm dev
```

Starts Next.js at `NEXT_PUBLIC_APP_ORIGIN`. Better Auth is mounted under `/api/auth/*` via a Next
route handler.

Key routes exposed by Better Auth:

- `POST /api/auth/sign-in/email` — verify credentials, set session cookie
- `POST /api/auth/sign-up/email` — create account, set session cookie
- `GET  /api/auth/token` — exchange active session cookie for a JWT (bearer plugin)
- `GET  /api/auth/jwks` — public key set used by the Jazz sync server

Open `NEXT_PUBLIC_APP_ORIGIN`.

## How the Better Auth integration works

### Server — `src/lib/auth.ts`, `src/lib/auth-jazz-context.ts`, and `schema-better-auth/schema.ts`

`auth.ts` wires up the Better Auth instance with four plugins and points the adapter at the
root Better Auth schema module:

```ts
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app as authSchema } from "../../schema-better-auth/schema";

betterAuth({
  database: jazzAdapter({
    db: () => authJazzContext().asBackend(authSchema),
    schema: authSchema,
  }),
  emailAndPassword: { enabled: true, autoSignIn: true, minPasswordLength: 1 },
  plugins: [
    nextCookies(),
    admin({ adminRoles: ["admin"], defaultRole: "member" }),
    bearer(), // enables GET /api/auth/token → JWT exchange
    jwt({
      jwks: { keyPairConfig: { alg: "ES256" } },
      jwt: {
        issuer,
        expirationTime: "30d",
        definePayload: ({ user }) => ({
          claims: { role: user.role ?? "" },
          username: user.name,
        }),
        getSubject: ({ user }) => user.id, // becomes session.user_id in Jazz
      },
    }),
  ],
});
```

`schema-better-auth/schema.ts` is the Better Auth schema source file that the Jazz adapter now
generates for the new root-schema workflow. `authJazzContext()` is a lazy accessor that returns
a server-side Jazz context configured with `driver: { type: "memory" }`, the same `serverUrl`,
and the same backend secret as the local sync server. It caches the context on `globalThis` so
route modules don't instantiate it at import time (which would fail during Next's build-time
page data collection before env vars are available). That keeps Better Auth state out of Better Auth's in-process memory
adapter while still avoiding local on-disk storage in the Next app.

- **`nextCookies` integration** — lets Better Auth session cookies participate in Next.js route
  handlers and server actions.
- **`admin` plugin** — tracks a `role` field on each user, defaults new accounts to `"member"`.
- **`bearer` plugin** — adds the `GET /api/auth/token` endpoint that turns a valid session cookie
  into a short-lived JWT signed by Better Auth's managed ES256 key pair.
- **`jwt` plugin** — manages JWKS key rotation and controls the JWT payload shape.
  `definePayload` injects `claims.role` and `username`; `getSubject` sets the JWT `sub` claim,
  which Jazz surfaces as `session.user_id` on the client.

The JWKS endpoint (`/api/auth/jwks`) is automatically provided by the `jwt` plugin and is what
the Jazz sync server polls to verify every incoming token. The same sync server also accepts the
backend secret used by the Better Auth Jazz context so auth rows can sync through Jazz too.

### Client — `src/lib/auth-client.ts`

```ts
import { jwtClient } from "better-auth/client/plugins";
import { createAuthClient } from "better-auth/react";

export const authClient = createAuthClient({ plugins: [jwtClient()] });
```

`jwtClient()` adds the `authClient.token()` method used to fetch the JWT after sign-in. No explicit
base URL is required; Better Auth defaults to `/api/auth` in the browser.

### Client — `app/page.tsx`

`Page` subscribes to the Better Auth session and, when a session exists, exchanges it for a JWT
before mounting `JazzProvider`:

```tsx
const { data: authSession } = authClient.useSession();
const [initialJwtToken, setInitialJwtToken] = React.useState<string | null>(null);

React.useEffect(() => {
  if (!authSession?.session) {
    setInitialJwtToken(null);
    return;
  }
  authClient.token().then(({ data }) => setInitialJwtToken(data.token));
}, [authSession?.session?.id]);
```

While the JWT is being fetched the app renders a loading state. Once the token arrives,
`JazzProvider` is mounted in JWT mode. On sign-out Better Auth clears the session cookie and
the effect resets `initialJwtToken` to `null`, recreating Jazz in local-first mode.

```tsx
const config: DbConfig = initialJwtToken
  ? { appId, jwtToken: initialJwtToken, serverUrl, ... }
  : { appId, auth: { localFirstSecret: secret }, serverUrl, ... };

<JazzProvider key={initialJwtToken ? "external" : "local"} config={config}>
  <ChatShell />
</JazzProvider>
```

If the sync server later returns `401` for an expired or invalid bearer token, the example calls
`db.updateAuthToken(freshJwt)` only after fetching a replacement JWT for the same Better Auth session.
It does not use `db.updateAuthToken(null)` for logout.

## Playwright

Run the full end-to-end setup and flow tests with:

```bash
pnpm test:e2e
```

The Playwright `webServer` starts Next at `NEXT_PUBLIC_APP_ORIGIN` from `.env`. Global setup waits
for the Next-hosted JWKS endpoint, spins up a local Jazz sync server pointed at that JWKS URL, and
pushes the schema catalogue before the browser test runs.

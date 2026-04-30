# auth-workos-chat

A small React + Vite example that shows how to integrate [WorkOS AuthKit](https://workos.com/docs/user-management) with Jazz.

What it demonstrates:

- Using `@workos-inc/authkit-react` to handle the full OAuth / SSO sign-in flow
- Pointing the Jazz sync server at WorkOS's hosted JWKS endpoint — no local auth server needed
- Calling `getAccessToken()` from the AuthKit hook and passing the WorkOS access token directly to `JazzProvider`
- Recreating `JazzProvider` on login and logout, while reserving `db.updateAuthToken(...)` for same-user JWT refresh after auth expiry
- Falling back to local-first auth when no WorkOS session exists
- Permission-preflight UI gating with `db.canInsert(...)`. Write rules are defined in `permissions.ts`: `admin` posts to Announcements, `admin`/`member` posts to the general chat, and generic-chat message ownership is enforced via `$createdBy`

There is no local auth server in this example. WorkOS issues and signs all tokens;
the JWKS is fetched from `https://api.workos.com/sso/jwks/<clientId>`.

## Prerequisites

You need a [WorkOS account](https://workos.com/docs/authkit/client-only) with a configured AuthKit environment and a client ID.

## Setup

### 1. Configure WorkOS

1. Configure the `WORKOS_CLIENT_ID` in `constants.ts` with WorkOS client id
2. Configure redirect URI in the WorkOS dashboard to `http://127.0.0.1:5173`.
3. Under Authentication > Sessions > JWT Template add the following snippet to expose the Organization's role to the JWT claims

```
{
  "claims": {
    "role": {{organization_membership.role}}
  }
}
```

### 2. Start the Jazz sync server

```bash
pnpm sync-server
```

Builds the `jazz-tools` binary if needed, starts a local sync server on port 1625 pointed at the
WorkOS JWKS URL, and pushes the schema catalogue in one step.

The JWKS URL is assembled from `WORKOS_CLIENT_ID` at runtime:

```ts
jwksUrl: `https://api.workos.com/sso/jwks/${WORKOS_CLIENT_ID}`;
```

The sync server fetches that URL to verify every incoming Jazz JWT signature.

### 3. Start the Vite app

```bash
pnpm dev
```

Open `http://127.0.0.1:5173`. Click **Continue with WorkOS** to be redirected to the WorkOS
hosted login page. After sign-in WorkOS redirects back to the app with an access token.

## How the WorkOS integration works

### Provider setup — `src/App.tsx`

The root wraps the whole app in `AuthKitProvider`, which manages the WorkOS session and OAuth
redirect lifecycle. The example enables `devMode` explicitly so the documented
`127.0.0.1` setup persists the refresh token locally and can restore the SPA session after the
redirect back from WorkOS:

```tsx
export function App() {
  return (
    <AuthKitProvider clientId={WORKOS_CLIENT_ID} devMode={true}>
      <JazzApp />
    </AuthKitProvider>
  );
}
```

### Token exchange — `src/App.tsx`

`JazzApp` reads the WorkOS session with `useAuth()` and calls `getAccessToken()` whenever the
user object changes. The access token is a standard JWT signed by WorkOS:

```tsx
const { isLoading, user, getAccessToken, signIn, signOut } = useAuth();

React.useEffect(() => {
  let isCancelled = false;

  if (!user) {
    setInitialJwtToken(null);
    return;
  }

  getAccessToken().then((accessToken) => {
    if (!isCancelled) {
      setInitialJwtToken(accessToken ?? null);
    }
  });

  return () => {
    isCancelled = true;
  };
}, [getAccessToken, user]);
```

Once the token is available, `JazzProvider` is mounted in JWT mode. When the user signs out,
`signOut` ends the WorkOS session and Jazz is recreated in local-first mode.

```tsx
const config: DbConfig = initialJwtToken
  ? { appId, jwtToken: initialJwtToken, serverUrl, ... }
  : { appId, auth: { localFirstSecret }, serverUrl, ... };

<JazzProvider key={initialJwtToken ? "external" : "local"} config={config}>
  <ChatShell user={user} onSignIn={signIn} onSignOut={signOut} />
</JazzProvider>
```

If the sync server later returns `401` for an expired or invalid bearer token, the example asks
WorkOS for a fresh access token and calls `db.updateAuthToken(freshJwt)` for that same user. Login and
logout still recreate the Jazz client.

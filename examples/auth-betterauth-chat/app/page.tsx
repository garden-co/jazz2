"use client";

import * as React from "react";
import { type DbConfig } from "jazz-tools";
import { JazzProvider, useDb, useLocalFirstAuth, useAuthState } from "jazz-tools/react";
import { ChatPanel } from "../src/ChatPanel";
import { AuthCard } from "../src/AuthCard";
import { authClient, getJwtFromBetterAuth } from "../src/lib/auth-client";

function ChatShell(): React.JSX.Element {
  const db = useDb();
  const { claims, authMode, userId } = useAuthState();

  const localFirstAuth = useLocalFirstAuth();

  async function handleSignIn(email: string, password: string) {
    const res = await authClient.signIn.email({
      email,
      password,
    });

    if (res.error) {
      throw new Error(res.error.message);
    }
  }

  async function handleSignUp(email: string, password: string) {
    const proofToken = await db.getLocalFirstIdentityProof({
      ttlSeconds: 60,
      audience: "betterauth-signup",
    });

    if (!proofToken) {
      throw new Error("Sign up requires an active Jazz session");
    }

    // proofToken is a custom field consumed by our server-side sign-up hook
    const res = await authClient.signUp.email({
      email,
      name: email,
      password,
      proofToken,
    } as Parameters<typeof authClient.signUp.email>[0]);

    if (res.error) {
      throw new Error(res.error.message);
    }
  }

  async function handleSignOut() {
    await authClient.signOut();
    await localFirstAuth.signOut();
  }

  return (
    <main className="app-shell">
      <span data-testid="user-id" style={{ display: "none" }}>
        {userId ?? ""}
      </span>
      <section className="content-grid">
        <AuthCard
          loggedIn={authMode !== "local-first"}
          role={claims.role as string | null | undefined}
          onSignIn={handleSignIn}
          onSignUp={handleSignUp}
          onSignOut={handleSignOut}
        />

        <ChatPanel
          chatId={process.env.NEXT_PUBLIC_ANNOUNCEMENTS_CHAT_ID!}
          title="Announcements"
          authorName={userId} // TODO: This should come from better auth (email, name/surname)
          readOnlyNotice="Only admins can post announcements."
        />

        <ChatPanel
          chatId={process.env.NEXT_PUBLIC_CHAT_ID!}
          title={process.env.NEXT_PUBLIC_CHAT_ID!}
          authorName={userId}
          readOnlyNotice="Sign in as admin or member to participate."
        />
      </section>
    </main>
  );
}

function useBetterAuthJWT() {
  const { data, isPending } = authClient.useSession();
  const [jwt, setJWT] = React.useState<string | null>(null);
  const [isLoadingJWT, setIsLoadingJWT] = React.useState(false);

  const sessionId = data?.session?.id;

  React.useEffect(() => {
    let cancelled = false;

    if (isPending) {
      return;
    }

    if (!sessionId) {
      setJWT(null);
      setIsLoadingJWT(false);
      return;
    }

    setIsLoadingJWT(true);

    void getJwtFromBetterAuth().then((accessToken) => {
      if (cancelled) {
        return;
      }

      setJWT(accessToken ?? null);
      setIsLoadingJWT(false);
    });

    return () => {
      cancelled = true;
    };
  }, [isPending, sessionId]);

  function getRefreshedJWT() {
    return getJwtFromBetterAuth();
  }

  return {
    isLoading: isPending || isLoadingJWT || (!!sessionId && !jwt),
    jwt,
    getRefreshedJWT,
  };
}

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!;

export default function Page(): React.JSX.Element {
  const betterAuth = useBetterAuthJWT();
  const { secret: localFirstSecret, isLoading: localFirstLoading } = useLocalFirstAuth();

  const secret = !betterAuth.jwt ? (localFirstSecret ?? undefined) : undefined;

  const config = React.useMemo(
    (): DbConfig => ({
      appId,
      env: "dev" as const, // TODO: detect from process.env
      userBranch: "main" as const, // TODO: should be the default
      serverUrl,
      jwtToken: betterAuth.jwt ?? undefined,
      secret,
    }),
    [betterAuth.jwt, secret],
  );

  if (betterAuth.isLoading || (!betterAuth.jwt && localFirstLoading)) {
    return <p className="loading-state">Loading auth credentials...</p>;
  }

  return (
    <JazzProvider
      config={config}
      onJWTExpired={() => betterAuth.getRefreshedJWT()}
      fallback={<p className="loading-state">Loading Jazz DB...</p>}
    >
      <ChatShell />
    </JazzProvider>
  );
}

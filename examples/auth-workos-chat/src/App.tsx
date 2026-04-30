import * as React from "react";
import { type User, AuthKitProvider, useAuth } from "@workos-inc/authkit-react";
import { JazzProvider, useAuthState } from "jazz-tools/react";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID, WORKOS_CLIENT_ID } from "../constants.js";
import { ChatPanel } from "../../auth-simple-chat/src/ChatPanel.js";
import { AuthCard } from "./AuthCard.js";
import { DbConfig } from "jazz-tools";

type ChatShellProps = {
  user: User | null;
  onSignIn: () => void | Promise<void>;
  onSignOut: () => void | Promise<void>;
};

function ChatShell({ user, onSignIn, onSignOut }: ChatShellProps) {
  const { claims } = useAuthState();
  const displayName = user ? `${user.firstName} ${user.lastName}`.trim() : "Anonymous";
  const statusDetail = user ? "Signed in with WorkOS" : "Sign in with WorkOS to unlock chat-01";

  return (
    <main className="app-shell">
      <section className="content-grid">
        <AuthCard
          role={claims.role as string}
          statusDetail={statusDetail}
          user={user}
          onSignIn={onSignIn}
          onSignOut={onSignOut}
        />

        <ChatPanel
          chatId={ANNOUNCEMENTS_CHAT_ID}
          title="Announcements"
          authorName={displayName}
          readOnlyNotice="Only admins can post announcements."
        />

        <ChatPanel
          chatId={CHAT_ID}
          title={CHAT_ID}
          authorName={displayName}
          readOnlyNotice="Sign in as admin or member to participate."
        />
      </section>
    </main>
  );
}

function useWorkOsJWT() {
  const { isLoading, user, getAccessToken, signIn, signOut } = useAuth();
  const [jwt, setJWT] = React.useState<string | undefined>(undefined);
  const [isLoadingJWT, setIsLoadingJWT] = React.useState(true);

  const userId = user?.id;

  React.useEffect(() => {
    let cancelled = false;

    if (isLoading) {
      return;
    }

    if (!userId) {
      setJWT(undefined);
      setIsLoadingJWT(false);
      return;
    }

    setIsLoadingJWT(true);

    void getAccessToken().then((accessToken) => {
      if (cancelled) {
        return;
      }

      setJWT(accessToken);
      setIsLoadingJWT(false);
    });

    return () => {
      cancelled = true;
    };
  }, [isLoading, userId]);

  function getRefreshedJWT() {
    return getAccessToken({ forceRefresh: true });
  }

  return {
    isLoading: isLoading || isLoadingJWT,
    jwt,
    getRefreshedJWT,
  };
}

const appId = import.meta.env.VITE_JAZZ_APP_ID;
const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;

function JazzApp() {
  const workos = useWorkOsJWT();
  const { user, signIn, signOut } = useAuth();

  const config = React.useMemo(
    (): DbConfig => ({
      appId,
      env: "dev" as const,
      userBranch: "main" as const,
      serverUrl,
      jwtToken: workos.jwt,
    }),
    [workos.jwt],
  );

  if (workos.isLoading) {
    return <p className="loading-state">Loading auth...</p>;
  }

  return (
    <JazzProvider
      config={config}
      onJWTExpired={() => workos.getRefreshedJWT()}
      fallback={<p className="loading-state">Connecting to Jazz...</p>}
    >
      <ChatShell
        user={user}
        onSignIn={signIn}
        onSignOut={() => {
          void signOut({
            returnTo: window.location.href,
          });
        }}
      />
    </JazzProvider>
  );
}

export function App() {
  return (
    <AuthKitProvider clientId={WORKOS_CLIENT_ID} devMode={true}>
      <JazzApp />
    </AuthKitProvider>
  );
}

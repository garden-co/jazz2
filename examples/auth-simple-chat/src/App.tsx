import * as React from "react";
import { type DbConfig } from "jazz-tools";
import { JazzProvider, useDb, useAuthState } from "jazz-tools/react";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID, DEFAULT_APP_ID, SYNC_SERVER_URL } from "../constants.js";
import {
  clearStoredAuthSession,
  readStoredAuthSession,
  type StoredAuthSession,
  writeStoredAuthSession,
} from "./auth-storage.js";
import { ChatPanel } from "./ChatPanel.js";
import { AuthCard } from "./AuthCard.js";
import { requestSignIn, requestSignUp } from "./api.js";

type ChatShellProps = {
  onStoredAuthSessionChange(session: StoredAuthSession | null): void;
};

function ChatShell({ onStoredAuthSessionChange }: ChatShellProps) {
  const db = useDb();
  const { authMode, claims, userId } = useAuthState();
  const role = typeof claims.role === "string" ? claims.role : null;

  async function handleSignIn(email: string, password: string) {
    const session = await requestSignIn(email, password);
    writeStoredAuthSession(DEFAULT_APP_ID, session);
    onStoredAuthSessionChange(session);
  }

  async function handleSignUp(email: string, password: string) {
    const session = await requestSignUp(email, password);
    writeStoredAuthSession(DEFAULT_APP_ID, session);
    onStoredAuthSessionChange(session);
  }

  function handleSignOut() {
    clearStoredAuthSession(DEFAULT_APP_ID);
    onStoredAuthSessionChange(null);
  }

  React.useEffect(() => {
    return db.onAuthChanged((state) => {
      if (state.error) {
        clearStoredAuthSession(DEFAULT_APP_ID);
        onStoredAuthSessionChange(null);
      }
    });
  }, [db, onStoredAuthSessionChange]);

  return (
    <main className="app-shell">
      <section className="content-grid">
        <AuthCard
          loggedIn={authMode !== "anonymous"}
          role={role}
          onSignIn={handleSignIn}
          onSignUp={handleSignUp}
          onSignOut={handleSignOut}
        />

        <ChatPanel
          chatId={ANNOUNCEMENTS_CHAT_ID}
          title="Announcements"
          authorName={userId ?? null}
          readOnlyNotice="Only admins can post announcements."
        />

        <ChatPanel
          chatId={CHAT_ID}
          title={CHAT_ID}
          authorName={userId ?? null}
          readOnlyNotice="Sign in as admin or member to participate."
        />
      </section>
    </main>
  );
}

export function App() {
  const [storedAuthSession, setStoredAuthSession] = React.useState<StoredAuthSession | null>(() =>
    readStoredAuthSession(DEFAULT_APP_ID),
  );

  const config = React.useMemo((): DbConfig => {
    const sharedConfig = {
      appId: DEFAULT_APP_ID,
      env: "dev" as const,
      userBranch: "main" as const,
      serverUrl: SYNC_SERVER_URL,
      // driver: { type: "memory" as const },
    };

    if (storedAuthSession) {
      return {
        ...sharedConfig,
        jwtToken: storedAuthSession.token,
      };
    }

    return sharedConfig;
  }, [storedAuthSession]);

  return (
    <JazzProvider config={config} fallback={<p className="loading-state">Connecting to Jazz...</p>}>
      <ChatShell onStoredAuthSessionChange={setStoredAuthSession} />
    </JazzProvider>
  );
}

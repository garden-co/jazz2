import { withJazz } from "jazz-tools/dev/next";

export const baseNextConfig = {
  reactStrictMode: true,
  serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
};

const appOrigin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";

export const jazzOptions = {
  server: {
    backendSecret: "auth-betterauth-chat-dev-backend-secret",
    jwksUrl: `${appOrigin}/api/auth/jwks`,
  },
};

export default withJazz(baseNextConfig, jazzOptions);

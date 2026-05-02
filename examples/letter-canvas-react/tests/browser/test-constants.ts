export const TEST_PORT = 19881;
export const ADMIN_SECRET = "test-admin-secret-for-letter-canvas-react";
export const APP_ID = "019d5cf9-29b8-7446-9a48-f6ca4fd0af91";

export async function testSecret(label: string): Promise<string> {
  const data = new TextEncoder().encode(label);
  const hash = await crypto.subtle.digest("SHA-256", data);
  const bytes = new Uint8Array(hash);
  let binary = "";
  for (const b of bytes) {
    binary += String.fromCharCode(b);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

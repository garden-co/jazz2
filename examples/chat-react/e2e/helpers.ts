import { expect, type Browser, type BrowserContext, type Page } from "@playwright/test";

const APP_URL = "http://127.0.0.1:5183";

export interface UserContext {
  page: Page;
  context: BrowserContext;
}

/**
 * Spin up a fresh browser context (isolated localStorage) for a named user and
 * navigate to the app.  The app auto-generates a local-first secret on first
 * load, so each context gets a distinct identity.
 */
export async function newUserContext(browser: Browser, _name: string): Promise<UserContext> {
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.goto(APP_URL);
  return { page, context };
}

/**
 * Wait until the app has finished creating the initial chat and has navigated
 * to it, then return the chat ID from the URL hash.
 */
export async function createChatAndGetId(page: Page): Promise<string> {
  // The app redirects from / to /#/chat/:id after creating the initial chat.
  await page.waitForURL(/\/#\/chat\//, { timeout: 30_000 });
  await waitForComposer(page);
  const hash = new URL(page.url()).hash; // e.g. "#/chat/abc123"
  const match = hash.match(/\/chat\/([^/]+)/);
  if (!match) throw new Error(`Could not extract chatId from URL: ${page.url()}`);
  return match[1];
}

/**
 * Wait for the message composer to be ready (not disabled).
 */
export async function waitForComposer(page: Page, timeout = 20_000): Promise<void> {
  // The send button is disabled when composerReady is false.
  const sendBtn = page
    .locator("button[data-slot='button']")
    .filter({ has: page.locator("svg") })
    .last();
  // More reliable: wait for the ProseMirror editor to be editable.
  const editor = page.locator("#messageEditor .ProseMirror");
  await expect(editor).not.toHaveAttribute("contenteditable", "false", { timeout });
}

/**
 * Send a message via the composer.  Waits for the composer to be ready first.
 */
export async function sendMessage(page: Page, text: string): Promise<void> {
  await waitForComposer(page);

  // Use the __editorHandle exposed on the editor container to insert text
  // without relying on keyboard events (avoids contenteditable quirks).
  await page.evaluate((msg) => {
    const el = document.getElementById("messageEditor") as
      | (HTMLElement & { __editorHandle?: { insertText: (t: string) => void; send: () => void } })
      | null;
    if (!el?.__editorHandle) throw new Error("Editor handle not found");
    el.__editorHandle.insertText(msg);
    el.__editorHandle.send();
  }, text);
}

/**
 * Wait for a message with the given text to appear in the chat view.
 */
export async function waitForMessage(page: Page, text: string, timeout = 20_000): Promise<void> {
  await expect(page.locator("article").filter({ hasText: text })).toBeVisible({ timeout });
}

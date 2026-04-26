/**
 * Regression test for the auto-join race condition.
 *
 * ChatView performs a fire-and-forget db.insert(chatMembers) when a user lands
 * on a chat they can see but are not yet a member of.  The MessageComposer
 * enables as soon as userId + myProfile are available — independently of
 * membership — so the user can send a message before the server has
 * acknowledged the chatMembers insert.  The server then rejects the message
 * insert with a permissions error because the sender is not yet a member.
 *
 * Fix: gate the composer on membershipReady (true only after chatMembers insert
 * is acknowledged at edge-tier), not just userId + myProfile.
 *
 * Test contract:
 *   A. The composer MUST transition through a disabled state before enabling
 *      (i.e. it must not start enabled immediately).
 *   B. Sending a message the moment the composer first enables must always
 *      deliver the message to other participants (no permission error).
 */
import { expect, test, type Page } from "@playwright/test";
import { newUserContext, createChatAndGetId, waitForComposer, waitForMessage } from "./helpers";

test.describe("auto-join race on first message send", () => {
  test("composer is disabled until membership is confirmed, then message delivers", async ({
    browser,
  }) => {
    const runId = Date.now();
    const bobMessage = `first-send-${runId}`;

    // ── Alice: create a public chat ───────────────────────────────────────────
    const { page: alice } = await newUserContext(browser, "alice");
    const chatId = await createChatAndGetId(alice);

    // ── Bob: open the chat directly via a fresh browser context ─────────────
    // We do NOT use newUserContext here (which navigates to the root first),
    // because navigating to the chat hash URL from the root is a client-side
    // hash change that won't trigger a page reload — meaning addInitScript
    // wouldn't re-run.  Instead we navigate directly to the final URL so that
    // the very first page load includes the observer script.
    const bobContext = await browser.newContext();
    const bob = await bobContext.newPage();

    const consoleErrors: string[] = [];
    bob.on("console", (msg) => {
      if (msg.type() === "error") consoleErrors.push(msg.text());
    });

    // Install the observer before the first (and only) navigation.
    await bob.addInitScript(() => {
      (window as typeof window & { __editorHistory: string[] }).__editorHistory = [];

      // Poll until the ProseMirror editor appears, then record its state.
      const poll = setInterval(() => {
        const pm = document.querySelector("#messageEditor .ProseMirror");
        if (!pm) return;
        clearInterval(poll);

        const initial = pm.getAttribute("contenteditable") ?? "missing";
        (window as typeof window & { __editorHistory: string[] }).__editorHistory.push(
          `initial:${initial}`,
        );

        new MutationObserver((mutations) => {
          for (const m of mutations) {
            if (m.attributeName === "contenteditable") {
              const val = (m.target as Element).getAttribute("contenteditable") ?? "missing";
              (window as typeof window & { __editorHistory: string[] }).__editorHistory.push(
                `change:${val}`,
              );
            }
          }
        }).observe(pm, { attributes: true });
      }, 5);
    });

    await bob.goto(`http://127.0.0.1:5183/#/chat/${chatId}`);

    // ── Assert A: composer must transition through disabled before enabling ───
    //
    // With the fix, the editor starts as contenteditable="false" and transitions
    // to "true" after the chatMembers insert is server-acknowledged.
    //
    // With the buggy code, composerReady = !!userId && !!myProfile skips the
    // membership gate, so the editor starts as contenteditable="true" directly.
    //
    // We wait until the editor is enabled, then inspect the recorded history.
    await waitForComposer(bob, 20_000);

    const editorHistory = await bob.evaluate(
      () => (window as typeof window & { __editorHistory: string[] }).__editorHistory ?? [],
    );

    // The history must include an initial or change event showing "false",
    // meaning the editor was disabled before it became enabled.
    const wasEverDisabled = editorHistory.some(
      (entry) => entry === "initial:false" || entry === "change:false",
    );
    expect(
      wasEverDisabled,
      `composer must have been disabled before enabling — history: ${JSON.stringify(editorHistory)}`,
    ).toBe(true);

    // ── Assert B: message sent the moment composer enables is delivered ───────
    await sendImmediately(bob, bobMessage);
    await waitForMessage(alice, bobMessage, 20_000);

    // ── Assert C: no permission errors ───────────────────────────────────────
    await bob.waitForTimeout(1_000);
    const permissionErrors = consoleErrors.filter(
      (e) =>
        e.toLowerCase().includes("policy denied") ||
        e.toLowerCase().includes("writeerror") ||
        e.toLowerCase().includes("permission"),
    );
    expect(permissionErrors, "expected no permission errors on Bob's page").toHaveLength(0);
  });
});

/**
 * Send a message without waiting for any network idle — as fast as possible.
 * Uses the __editorHandle exposed on the editor container.
 */
async function sendImmediately(page: Page, text: string): Promise<void> {
  await page.evaluate((msg) => {
    const el = document.getElementById("messageEditor") as
      | (HTMLElement & {
          __editorHandle?: { insertText: (t: string) => void; send: () => void };
        })
      | null;
    if (!el?.__editorHandle) throw new Error("Editor handle not found on #messageEditor");
    el.__editorHandle.insertText(msg);
    el.__editorHandle.send();
  }, text);
}

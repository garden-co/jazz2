import { expect, test, type Locator, type Page } from "@playwright/test";

test.describe("auth-simple-chat", () => {
  test("enforces announcement and generic chat access by role", async ({ page }) => {
    const announcements = chat(page, "Announcements");
    const genericChat = chat(page, "chat-01");
    const runId = Date.now();
    const adminAnnouncement = `Admin announcement ${runId}`;
    const adminGenericMessage = `Admin generic chat ${runId}`;
    const memberGenericMessage = `Member generic chat ${runId}`;

    await page.goto("/");

    // admin is a pre-seeded account — use sign-in
    await signIn(page, { email: "admin@example.com", password: "admin" });
    await expect(page.getByTestId("auth-status")).toContainText("admin", { timeout: 20_000 });
    await expect(page.getByText("Log out")).toBeVisible();

    await sendMessage(announcements, adminAnnouncement);
    await expect(messageItem(announcements.list, adminAnnouncement)).toBeVisible({
      timeout: 20_000,
    });

    await sendMessage(genericChat, adminGenericMessage);
    await expect(messageItem(genericChat.list, adminGenericMessage)).toBeVisible({
      timeout: 20_000,
    });

    await page.getByTestId("logout-button").click();
    await expect(page.getByTestId("auth-status")).toContainText("Anonymous", { timeout: 20_000 });

    await expect(messageItem(announcements.list, adminAnnouncement)).toBeVisible({
      timeout: 20_000,
    });
    await expect(announcements.readOnlyNotice).toBeVisible();
    await expect(messageItem(genericChat.list, adminGenericMessage)).toHaveCount(0, {
      timeout: 20_000,
    });
    await expect(genericChat.list).toContainText("No messages yet.");
    await expect(genericChat.readOnlyNotice).toBeVisible();

    // member is a new account — use sign-up
    const memberEmail = `member-${runId}@example.com`;
    await signUp(page, { email: memberEmail, password: "member" });
    await expect(page.getByTestId("auth-status")).toContainText("member", { timeout: 20_000 });
    await expect(page.getByTestId("auth-status")).not.toContainText("Anonymous");

    await expect(announcements.readOnlyNotice).toBeVisible();
    await expect(announcements.input).toBeDisabled();
    await expect(announcements.send).toBeDisabled();

    await page.reload();
    await expect(page.getByTestId("auth-status")).toContainText("member", { timeout: 20_000 });
    await expect(page.getByTestId("auth-status")).not.toContainText("Anonymous");

    await expect(messageItem(announcements.list, adminAnnouncement)).toBeVisible({
      timeout: 20_000,
    });
    await expect(announcements.readOnlyNotice).toBeVisible();
    await expect(announcements.input).toBeDisabled();
    await expect(announcements.send).toBeDisabled();

    await expect(messageItem(genericChat.list, adminGenericMessage)).toBeVisible({
      timeout: 20_000,
    });
    await expect(genericChat.readOnlyNotice).toHaveCount(0);
    await expect(genericChat.input).toBeEnabled();
    await expect(genericChat.send).toBeDisabled();

    await sendMessage(genericChat, memberGenericMessage);
    await expect(messageItem(genericChat.list, memberGenericMessage)).toBeVisible({
      timeout: 20_000,
    });

    await page.reload();
    await expect(page.getByTestId("auth-status")).toContainText("member", { timeout: 20_000 });

    await page.getByTestId("logout-button").click();
    await expect(page.getByTestId("auth-status")).toContainText("Anonymous", { timeout: 20_000 });
    await expect(messageItem(genericChat.list, memberGenericMessage)).toHaveCount(0, {
      timeout: 20_000,
    });
    await expect(genericChat.readOnlyNotice).toBeVisible();
  });
});

type Credentials = {
  email: string;
  password: string;
};

type ChatLocators = {
  input: Locator;
  list: Locator;
  readOnlyNotice: Locator;
  send: Locator;
};

function chat(page: Page, title: string): ChatLocators {
  return {
    input: page.getByTestId(`message-input-${title}`),
    list: page.getByTestId(`message-list-${title}`),
    readOnlyNotice: page.getByTestId(`chat-readonly-notice-${title}`),
    send: page.getByTestId(`send-button-${title}`),
  };
}

function messageItem(list: Locator, text: string): Locator {
  return list.getByTestId("message-item").filter({ hasText: text }).last();
}

async function signIn(page: Page, credentials: Credentials) {
  await page.getByRole("button", { name: "Sign in" }).click();
  await page.getByLabel("Email").fill(credentials.email);
  await page.getByLabel("Password").fill(credentials.password);
  await page.getByTestId("auth-submit").click();
}

async function signUp(page: Page, credentials: Credentials) {
  await page.getByRole("button", { name: "Sign up" }).click();
  await page.getByLabel("Email").fill(credentials.email);
  await page.getByLabel("Password").fill(credentials.password);
  await page.getByTestId("auth-submit").click();
}

async function sendMessage(target: ChatLocators, text: string) {
  await target.input.fill(text);
  await target.send.click();
}

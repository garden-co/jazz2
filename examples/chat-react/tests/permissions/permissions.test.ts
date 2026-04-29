import { fileURLToPath } from "node:url";
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { app } from "../../schema.js";

const SCHEMA_DIR = fileURLToPath(new URL("../..", import.meta.url));

let testApp: PolicyTestApp;

beforeEach(async () => {
  testApp = await createPolicyTestApp(SCHEMA_DIR, expect);
});

afterEach(async () => {
  await testApp?.shutdown();
});

describe("chat permissions", () => {
  it("allows pre-authorized private chat reads via join_code claim", async () => {
    const privateChat = testApp.seed((db) => {
      const { value: privateChat } = db.insert(app.chats, {
        name: "Private room",
        isPublic: false,
        createdBy: "alice",
        joinCode: "invite-123",
      });
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "alice",
        joinCode: "invite-123",
      });
      return privateChat;
    });

    const bobWithoutClaim = testApp.as({ user_id: "bob", claims: {}, authMode: "local-first" });
    const bobWithClaim = testApp.as({
      user_id: "bob",
      claims: { join_code: "invite-123" },
      authMode: "local-first",
    });

    await expect(bobWithoutClaim.all(app.chats.where({ id: privateChat.id }))).resolves.toEqual([]);
    await expect(bobWithClaim.all(app.chats.where({ id: privateChat.id }))).resolves.toEqual([
      expect.objectContaining({ id: privateChat.id, name: "Private room" }),
    ]);
  });

  it("allows message inserts only for chat members", async () => {
    const { aliceProfile, bobProfile, privateChat } = testApp.seed((db) => {
      const { value: aliceProfile } = db.insert(app.profiles, {
        userId: "alice",
        name: "Alice",
      });
      const { value: bobProfile } = db.insert(app.profiles, {
        userId: "bob",
        name: "Bob",
      });
      const { value: privateChat } = db.insert(app.chats, {
        name: "Members only",
        isPublic: false,
        createdBy: "alice",
        joinCode: "invite-456",
      });
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "alice",
        joinCode: "invite-456",
      });
      return { aliceProfile, bobProfile, privateChat };
    });

    const aliceDb = testApp.as({ user_id: "alice", claims: {}, authMode: "local-first" });
    const bobDb = testApp.as({ user_id: "bob", claims: {}, authMode: "local-first" });

    testApp.expectAllowed(() =>
      aliceDb.insert(app.messages, {
        chatId: privateChat.id,
        text: "hello from alice",
        senderId: aliceProfile.id,
        createdAt: new Date("2026-01-01T00:00:00.000Z"),
      }),
    );

    testApp.expectDenied(() =>
      bobDb.insert(app.messages, {
        chatId: privateChat.id,
        text: "hello from bob",
        senderId: bobProfile.id,
        createdAt: new Date("2026-01-01T00:00:01.000Z"),
      }),
    );

    testApp.seed((db) => {
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "bob",
        joinCode: "invite-456",
      });
    });

    testApp.expectAllowed(() =>
      bobDb.insert(app.messages, {
        chatId: privateChat.id,
        text: "hello from bob after joining",
        senderId: bobProfile.id,
        createdAt: new Date("2026-01-01T00:00:02.000Z"),
      }),
    );
  });

  it("inherits reaction reads from the parent message/chat chain", async () => {
    const reaction = testApp.seed((db) => {
      const { value: aliceProfile } = db.insert(app.profiles, {
        userId: "alice",
        name: "Alice",
      });
      const { value: privateChat } = db.insert(app.chats, {
        name: "Uploads",
        isPublic: false,
        createdBy: "alice",
        joinCode: "invite-789",
      });
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "alice",
        joinCode: "invite-789",
      });
      db.insert(app.chatMembers, {
        chatId: privateChat.id,
        userId: "bob",
        joinCode: "invite-789",
      });
      const { value: message } = db.insert(app.messages, {
        chatId: privateChat.id,
        text: "see attachment",
        senderId: aliceProfile.id,
        createdAt: new Date("2026-01-01T00:00:03.000Z"),
      });
      const { value: reaction } = db.insert(app.reactions, {
        messageId: message.id,
        userId: "alice",
        emoji: "fire",
      });
      return reaction;
    });

    const bobDb = testApp.as({ user_id: "bob", claims: {}, authMode: "local-first" });
    const carolDb = testApp.as({ user_id: "carol", claims: {}, authMode: "local-first" });

    await expect(bobDb.all(app.reactions.where({ id: reaction.id }))).resolves.toEqual([
      expect.objectContaining({ id: reaction.id, emoji: "fire" }),
    ]);
    await expect(carolDb.all(app.reactions.where({ id: reaction.id }))).resolves.toEqual([]);
  });
});

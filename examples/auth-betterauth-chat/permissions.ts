import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema";

const ANNOUNCEMENTS_CHAT_ID = process.env.NEXT_PUBLIC_ANNOUNCEMENTS_CHAT_ID!;
const CHAT_ID = process.env.NEXT_PUBLIC_CHAT_ID!;

export default definePermissions(app, ({ policy, allOf, session }) => {
  const isAdmin = session.where({ "claims.role": "admin" });
  const canMutateGenericChat = { $createdBy: session.user_id };

  policy.messages.allowRead.where({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  policy.messages.allowRead.where({ chat_id: CHAT_ID });

  policy.messages.allowInsert.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]));
  policy.messages.allowInsert.where({ chat_id: CHAT_ID });

  policy.messages.allowUpdate
    .whereOld(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]))
    .whereNew({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  policy.messages.allowUpdate
    .whereOld(allOf([{ chat_id: CHAT_ID }, canMutateGenericChat]))
    .whereNew({ chat_id: CHAT_ID });

  policy.messages.allowDelete.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]));
  policy.messages.allowDelete.where(allOf([{ chat_id: CHAT_ID }, canMutateGenericChat]));
});

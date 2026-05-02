import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, session }) => {
  policy.canvases.allowRead.where({});
  policy.canvases.allowInsert.where({});
  policy.canvases.allowUpdate.never();
  policy.canvases.allowDelete.never();

  policy.letters.allowRead.where({});
  policy.letters.allowInsert.where({});
  policy.letters.allowUpdate.whereOld({}).whereNew({});
  policy.letters.allowDelete.never();

  policy.cursors.allowRead.where({});
  policy.cursors.allowInsert.where({ user_id: session.user_id });
  policy.cursors.allowUpdate.whereOld({}).whereNew({});
  policy.cursors.allowDelete.never();
});

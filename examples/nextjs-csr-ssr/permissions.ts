import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema";

export default definePermissions(app, ({ policy }) => {
  policy.todos.allowRead.always();
  policy.todos.allowInsert.always();
  policy.todos.allowUpdate.always();
  policy.todos.allowDelete.always();
});

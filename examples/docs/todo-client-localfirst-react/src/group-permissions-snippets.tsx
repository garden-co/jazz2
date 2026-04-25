import { schema as s } from "jazz-tools";
import { useAll, useDb } from "jazz-tools/react";

// #region group-schema
const schema = {
  workspaces: s.table({
    name: s.string(),
  }),
  workspaceMembers: s.table({
    workspaceId: s.ref("workspaces"),
    user_id: s.string(),
    role: s.enum(["reader", "writer", "contributor", "admin"]),
  }),
  documents: s.table({
    title: s.string(),
    content: s.string(),
    workspaceId: s.ref("workspaces"),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
// #endregion group-schema

// #region group-permissions
type Role = "reader" | "writer" | "contributor" | "admin";

s.definePermissions(app, ({ policy, session, anyOf, allOf }) => {
  // Re-usable helpers to improve readability
  const isMember = (workspaceId: string) =>
    policy.workspaceMembers.exists.where({ workspaceId, user_id: session.user_id });

  const hasRole = (workspaceId: string, role: Role) =>
    policy.workspaceMembers.exists.where({ workspaceId, user_id: session.user_id, role });

  const isAdmin = (workspaceId: string) => hasRole(workspaceId, "admin");

  // --- documents ---

  policy.documents.allowRead.where((doc) => isMember(doc.workspaceId));

  policy.documents.allowInsert.where((doc) =>
    anyOf([
      hasRole(doc.workspaceId, "writer"),
      hasRole(doc.workspaceId, "contributor"),
      hasRole(doc.workspaceId, "admin"),
    ]),
  );

  // Writers and admins can edit any document; contributors can only edit their own
  policy.documents.allowUpdate.where((doc) =>
    anyOf([
      hasRole(doc.workspaceId, "writer"),
      hasRole(doc.workspaceId, "admin"),
      allOf([{ $createdBy: session.user_id }, hasRole(doc.workspaceId, "contributor")]),
    ]),
  );

  // Writers and admins can delete any document; contributors can delete their own
  policy.documents.allowDelete.where((doc) =>
    anyOf([
      hasRole(doc.workspaceId, "writer"),
      isAdmin(doc.workspaceId),
      allOf([{ $createdBy: session.user_id }, hasRole(doc.workspaceId, "contributor")]),
    ]),
  );

  // --- workspaces ---

  policy.workspaces.allowRead.where((workspace) => isMember(workspace.id));
  policy.workspaces.allowInsert.always();
  policy.workspaces.allowUpdate.where((workspace) => isAdmin(workspace.id));
  policy.workspaces.allowDelete.where((workspace) => isAdmin(workspace.id));

  // --- workspaceMembers ---

  policy.workspaceMembers.allowRead.where((member) => isMember(member.workspaceId));

  // Admins can add members; workspace creators can bootstrap themselves as the first admin
  policy.workspaceMembers.allowInsert.where((member) =>
    anyOf([
      isAdmin(member.workspaceId),
      allOf([
        { user_id: session.user_id, role: "admin" },
        policy.workspaces.exists.where({ id: member.workspaceId, $createdBy: session.user_id }),
      ]),
    ]),
  );

  policy.workspaceMembers.allowUpdate.where((member) => isAdmin(member.workspaceId));

  // Admins can remove any member; members can leave on their own
  policy.workspaceMembers.allowDelete.where((member) =>
    anyOf([isAdmin(member.workspaceId), { user_id: session.user_id }]),
  );
});
// #endregion group-permissions

// #region group-create
export function createWorkspace(db: ReturnType<typeof useDb>, name: string, creatorId: string) {
  const workspace = db.insert(app.workspaces, { name });
  // Add the creator as admin immediately so they can manage the workspace
  db.insert(app.workspaceMembers, {
    workspaceId: workspace.id,
    user_id: creatorId,
    role: "admin",
  });
  return workspace;
}
// #endregion group-create

// #region group-add-member
export function addMember(
  db: ReturnType<typeof useDb>,
  workspaceId: string,
  userId: string,
  role: "reader" | "writer" | "contributor" | "admin",
) {
  db.insert(app.workspaceMembers, { workspaceId, user_id: userId, role });
}
// #endregion group-add-member

// #region group-query-docs
export function WorkspaceDocuments({ workspaceId }: { workspaceId: string }) {
  const docs = useAll(app.documents.where({ workspaceId }));

  if (!docs) return <p>Loading…</p>;

  return (
    <ul>
      {docs.map((doc) => (
        <li key={doc.id}>{doc.title}</li>
      ))}
    </ul>
  );
}
// #endregion group-query-docs

// #region group-members-list
export function WorkspaceMembers({ workspaceId }: { workspaceId: string }) {
  const members = useAll(app.workspaceMembers.where({ workspaceId }));

  if (!members) return <p>Loading…</p>;

  return (
    <ul>
      {members.map((member) => (
        <li key={member.id}>
          {member.user_id} — {member.role}
        </li>
      ))}
    </ul>
  );
}
// #endregion group-members-list

// #region group-change-role
export function changeRole(
  db: ReturnType<typeof useDb>,
  memberId: string,
  newRole: "reader" | "contributor" | "writer" | "admin",
) {
  db.update(app.workspaceMembers, memberId, { role: newRole });
}
// #endregion group-change-role

// #region group-remove-member
export async function removeMember(
  db: ReturnType<typeof useDb>,
  workspaceId: string,
  userId: string,
) {
  const member = await db.one(app.workspaceMembers.where({ workspaceId, user_id: userId }));
  if (member) db.delete(app.workspaceMembers, member.id);
}
// #endregion group-remove-member

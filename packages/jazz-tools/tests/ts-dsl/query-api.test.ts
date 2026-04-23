import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, beforeEach, describe, it, expect, assert, expectTypeOf } from "vitest";
import { app, type Project, type Todo, type User } from "./fixtures/basic/schema";
import { insertProject, insertTodo, insertUser, uniqueDbName } from "./factories";

function makeFriends(db: Db, user1: User, user2: User) {
  const user1Friends = [...user1.friendsIds, user2.id];
  const user2Friends = [...user2.friendsIds, user1.id];

  db.update(app.users, user1.id, { friendsIds: user1Friends });
  db.update(app.users, user2.id, { friendsIds: user2Friends });

  // Keep the in-memory fixtures aligned with the DB row so later updates append correctly.
  user1.friendsIds = user1Friends;
  user2.friendsIds = user2Friends;
}

describe("TS Query API", () => {
  let db: Db;

  beforeEach(async () => {
    db = await createDb({
      appId: "test-app",
      driver: { type: "persistent" },
    });
  });

  afterEach(async () => {
    await db.shutdown();
  });

  describe("filtering", () => {
    it("queries by id", async () => {
      const { id } = insertProject(db, "Project A");

      const results = await db.all(app.projects.where({ id: { eq: id } }));
      expect(results.length).toBe(1);

      expectTypeOf(results[0]!).branded.toEqualTypeOf<Project>();
      expect(results[0]!.id).toBe(id);
      expect(results[0]!.name).toBe("Project A");
    });

    it("filters nullable columns with isNull:true", async () => {
      const todoWithoutOwner = insertTodo(db, {
        title: "Todo without owner",
        ownerId: null,
      });
      const _todoWithOwner = insertTodo(db, {
        title: "Todo with owner",
        ownerId: insertUser(db).id,
      });

      const results = await db.all(app.todos.where({ ownerId: { isNull: true } }));

      expect(results.map((todo) => todo.id)).toEqual([todoWithoutOwner.id]);
    });

    it("filters non-nullable columns with isNull:false", async () => {
      const _todoWithoutOwner = insertTodo(db, {
        title: "Todo without owner",
        ownerId: null,
      });
      const todoWithOwner = insertTodo(db, {
        title: "Todo with owner",
        ownerId: insertUser(db).id,
      });

      const results = await db.all(app.todos.where({ ownerId: { isNull: false } }));

      expect(results.map((todo) => todo.id)).toEqual([todoWithOwner.id]);
    });

    // Note: this is a difference with respect to SQL, when =null checks always return false.
    it("filters with explicit null values work as isNull:true", async () => {
      const todoWithoutOwner = insertTodo(db, {
        title: "Todo without owner",
        ownerId: null,
      });
      const _todoWithOwner = insertTodo(db, {
        title: "Todo with owner",
        ownerId: insertUser(db).id,
      });

      const resultsDirectNull = await db.all(app.todos.where({ ownerId: null }));
      const resultsEqNull = await db.all(app.todos.where({ ownerId: { eq: null } }));
      expect(resultsDirectNull.map((todo) => todo.id)).toEqual([todoWithoutOwner.id]);
      expect(resultsEqNull.map((todo) => todo.id)).toEqual([todoWithoutOwner.id]);
    });

    it("filters with explicit undefined values are no-ops", async () => {
      const todoWithoutOwner = insertTodo(db, {
        title: "Todo without owner",
        ownerId: null,
      });
      const todoWithOwner = insertTodo(db, {
        title: "Todo with owner",
        ownerId: insertUser(db).id,
      });

      const results = await db.all(app.todos.where({ ownerId: undefined }));
      expect(results.map((todo) => todo.id)).toEqual([todoWithoutOwner.id, todoWithOwner.id]);
    });

    describe("query by array column", () => {
      it("using eq", async () => {
        const { id: id1 } = insertTodo(db, {
          title: "Todo 1",
          tags: ["tag1"],
        });
        insertTodo(db, {
          title: "Todo 2",
          tags: ["tag2"],
        });
        insertTodo(db, {
          title: "Todo 3",
          tags: ["tag1", "tag2"],
        });

        const todosWithTags = await db.all(app.todos.where({ tags: { eq: ["tag1"] } }));
        expect(todosWithTags.length).toBe(1);
        expect(todosWithTags[0]!.id).toEqual(id1);
      });

      it("using contains", async () => {
        const { id: id1 } = insertTodo(db, {
          title: "Todo 1",
          tags: ["tag1"],
        });
        insertTodo(db, {
          title: "Todo 2",
          tags: ["tag2"],
        });
        const { id: id3 } = insertTodo(db, {
          title: "Todo 3",
          tags: ["tag1", "tag2"],
        });

        const todosWithTags = await db.all(app.todos.where({ tags: { contains: "tag1" } }));
        expect(todosWithTags.length).toBe(2);
        expect(todosWithTags).toContainEqual(expect.objectContaining({ id: id1 }));
        expect(todosWithTags).toContainEqual(expect.objectContaining({ id: id3 }));
      });
    });
  });

  describe("include", () => {
    it("include returns the related entity", async () => {
      const { id: projectId } = insertProject(db, "Announcements");
      const { id: ownerId } = insertUser(db);
      const { id: todoId } = insertTodo(db, {
        title: "Write tests",
        projectId: projectId,
        ownerId: ownerId,
      });

      const results = await db.all(
        app.todos.where({ id: { eq: todoId } }).include({ project: true }),
      );

      expect(results.length).toBe(1);
      const todo = results[0]!;
      expect(todo.title).toBe("Write tests");
      expectTypeOf(todo.ownerId).toEqualTypeOf<string | null>();
      expect(todo.ownerId).toBe(ownerId);
      expectTypeOf(todo.project).toEqualTypeOf<Project | null>();
      expect(todo.project?.name).toBe("Announcements");
    });

    it("include only resolves the provided columns, not all references", async () => {
      const { id: projectId } = insertProject(db, "Announcements");
      const { id: ownerId } = insertUser(db);
      const { id: todoId } = insertTodo(db, {
        projectId: projectId,
        ownerId: ownerId,
      });

      const result = await db.one(
        app.todos
          .select("ownerId")
          .where({ id: { eq: todoId } })
          .include({ project: true }),
      );

      assert(result, "Result is not defined");
      expectTypeOf(result.ownerId).toEqualTypeOf<string | null>();
      expect(result.ownerId).toBe(ownerId);
      expectTypeOf(result.project).toEqualTypeOf<Project | null>();
      assert(result.project, "Project include is not defined");
      expect(result.project.name).toBe("Announcements");
    });

    it("include returns null for null foreign key columns", async () => {
      const { id: todoId } = insertTodo(db, {
        ownerId: undefined,
      });

      const result = await db.one(app.todos.where({ id: { eq: todoId } }).include({ owner: true }));

      assert(result, "Result is not defined");
      expectTypeOf(result.owner).toEqualTypeOf<User | null>();
      expect(result.owner).toBeNull();
    });

    it("text is not corrupted when using include", async () => {
      const { id: projectId } = insertProject(db);
      const { id: ownerId } = insertUser(db);
      const { id: todoId } = insertTodo(db, {
        title: "Hello world",
        tags: ["general"],
        projectId: projectId,
        ownerId: ownerId,
      });

      const baseline = await db.all(app.todos.where({ id: { eq: todoId } }));
      expect(baseline[0]!.title).toBe("Hello world");

      const withInclude = await db.all(
        app.todos.where({ id: { eq: todoId } }).include({ project: true }),
      );

      expect(withInclude.length).toBe(1);
      expect(withInclude[0]!.title).toBe("Hello world");
    });
  });

  describe("missing reference handling", () => {
    it("include returns null for missing scalar referenced entities", async () => {
      const project = insertProject(db);
      const todo = insertTodo(db, {
        projectId: project.id,
      });

      await db.delete(app.projects, project.id);

      const result = await db.one(
        app.todos.where({ id: { eq: todo.id } }).include({ project: true }),
      );
      assert(result, "Result is not defined");
      expectTypeOf(result.project).toEqualTypeOf<Project | null>();
      expect(result.project).toBeNull();
    });

    it("include skips missing referenced entities in forward array relations", async () => {
      const assignee1 = insertUser(db);
      const assignee2 = insertUser(db);
      const todo = insertTodo(db, {
        assigneesIds: [assignee1.id, assignee2.id],
      });

      await db.delete(app.users, assignee1.id);

      const result = await db.one(
        app.todos.where({ id: { eq: todo.id } }).include({ assignees: app.users.select("id") }),
      );
      assert(result, "Result is not defined");
      expectTypeOf(result.assignees).branded.toEqualTypeOf<{ id: string }[]>();
      expect(result.assignees).toEqual([{ id: assignee2.id }]);
    });

    it("include skips missing referenced entities in reverse relations", async () => {
      const owner = insertUser(db);
      const { id: todoId } = insertTodo(db, {
        ownerId: owner.id,
      });
      const { id: todoId2 } = insertTodo(db, {
        ownerId: owner.id,
      });

      await db.delete(app.todos, todoId);

      const result = await db.one(
        app.users
          .where({ id: { eq: owner.id } })
          .include({ todosViaOwner: app.todos.select("id") }),
      );
      assert(result, "Result is not defined");
      expectTypeOf(result.todosViaOwner).branded.toEqualTypeOf<{ id: string }[]>();
      expect(result.todosViaOwner).toEqual([{ id: todoId2 }]);
    });

    describe("requireIncludes", () => {
      it("requireIncludes filters out rows with missing scalar referenced entities", async () => {
        const project = insertProject(db);
        const todo = insertTodo(db, {
          projectId: project.id,
        });

        await db.delete(app.projects, project.id);

        const result = await db.one(
          app.todos
            .where({ id: { eq: todo.id } })
            .include({ project: true })
            .requireIncludes(),
        );

        expect(result).toBeNull();
        if (result) {
          expectTypeOf(result.project).toEqualTypeOf<Project>();
        }
      });

      it("requireIncludes does not filter out rows with null scalar references", async () => {
        const todo = insertTodo(db, {
          ownerId: undefined,
        });

        const result = await db.one(
          app.todos
            .where({ id: { eq: todo.id } })
            .include({ owner: true })
            .requireIncludes(),
        );

        assert(result, "Result is not defined");
        expect(result.id).toBe(todo.id);
        expectTypeOf(result.owner).toEqualTypeOf<User | null>();
        expect(result.owner).toBeNull();
      });

      it("requireIncludes filters out rows with missing entities in forward array relations", async () => {
        const assignee1 = insertUser(db);
        const assignee2 = insertUser(db);
        const todo = insertTodo(db, {
          assigneesIds: [assignee1.id, assignee2.id],
        });

        await db.delete(app.users, assignee1.id);

        const result = await db.one(
          app.todos
            .where({ id: { eq: todo.id } })
            .include({ assignees: app.users.select("id") })
            .requireIncludes(),
        );

        expect(result).toBeNull();
      });

      it("requireIncludes does not filter rows for reverse relations", async () => {
        const owner = insertUser(db);
        const { id: todoId } = insertTodo(db, {
          ownerId: owner.id,
        });
        const { id: todoId2 } = insertTodo(db, {
          ownerId: owner.id,
        });

        await db.delete(app.todos, todoId);

        const result = await db.one(
          app.users
            .where({ id: { eq: owner.id } })
            .include({ todosViaOwner: app.todos.select("id") })
            .requireIncludes(),
        );
        assert(result, "Result is not defined");
        expect(result.todosViaOwner).toEqual([{ id: todoId2 }]);
      });

      it("can use requireIncludes in nested includes", async () => {
        const alice = insertUser(db);
        const bob = insertUser(db);
        const deletedUser = insertUser(db);

        makeFriends(db, alice, bob);
        makeFriends(db, bob, deletedUser);

        db.delete(app.users, deletedUser.id);

        const result = await db.one(
          app.users
            .where({ id: { eq: alice.id } })
            .include({ friends: app.users.include({ friends: true }).requireIncludes() }),
        );

        assert(result, "Result is not defined");
        // Bob is not loaded because he's friends with a deleted user
        // But Alice can still be loaded, because we didn't use requireIncludes on the top-level include
        expect(result.friends).toHaveLength(0);
      });

      it("top-level requireIncludes does not affect inner includes", async () => {
        const alice = insertUser(db);
        const bob = insertUser(db);
        const deletedUser = insertUser(db);

        makeFriends(db, alice, bob);
        makeFriends(db, bob, deletedUser);

        db.delete(app.users, deletedUser.id);

        const result = await db.one(
          app.users
            .where({ id: { eq: alice.id } })
            .include({ friends: { friends: true } })
            .requireIncludes(),
        );

        assert(result, "Result is not defined");
        expect(result.friends.map((f) => f.id)).toEqual([bob.id]);
        const aliceFriend = result.friends[0];
        assert(aliceFriend, "Alice's friend is not defined");
        // requireIncludes only affects Alice. Bob's remaining friends still load.
        expect(aliceFriend.friends.map((f) => f.id)).toEqual([alice.id]);
      });

      it("rows skipped by requireIncludes affect limit-offset pagination", async () => {
        const alice = insertUser(db);
        const bob = insertUser(db);
        const deletedUser = insertUser(db);

        makeFriends(db, alice, bob);
        makeFriends(db, bob, deletedUser);

        const results = await db.all(
          app.users.include({ friends: true }).requireIncludes().limit(1).offset(1),
        );
        expect(results.map((u) => u.id)).toEqual([bob.id]);

        await db.delete(app.users, deletedUser.id);

        const results2 = await db.all(
          app.users.include({ friends: true }).requireIncludes().limit(1).offset(1),
        );
        expect(results2).toHaveLength(0);
      });
    });
  });

  describe("select", () => {
    it("select narrows root columns while preserving id and includes", async () => {
      const { id: projectId } = insertProject(db, "Announcements");
      const { id: todoId } = insertTodo(db, {
        title: "Write tests",
        done: false,
        tags: ["dev"],
        projectId: projectId,
      });

      const result = await db.one(
        app.todos
          .select("title")
          .where({ id: { eq: todoId } })
          .include({ project: true }),
      );

      assert(result, "Result is not defined");
      expectTypeOf(result.id).toEqualTypeOf<string>();
      expectTypeOf(result.title).toEqualTypeOf<string>();
      expectTypeOf(result.project).toEqualTypeOf<Project | null>();
      expect(result).toEqual({
        id: todoId,
        title: "Write tests",
        project: {
          id: projectId,
          name: "Announcements",
        },
      });
      expect("done" in result).toBe(false);
      expect("tags" in result).toBe(false);
    });

    it('select("*") resets to all root columns', async () => {
      const { id: projectId } = insertProject(db);
      const { id: ownerId } = insertUser(db);
      const { id: todoId } = insertTodo(db, {
        title: "Write tests",
        done: false,
        tags: ["dev"],
        projectId: projectId,
        ownerId: ownerId,
        assigneesIds: [],
      });

      const result = await db.one(app.todos.select("*").where({ id: { eq: todoId } }));

      assert(result, "Result is not defined");
      expectTypeOf(result).branded.toEqualTypeOf<Todo>();
      expect(result).toEqual({
        id: todoId,
        title: "Write tests",
        done: false,
        tags: ["dev"],
        projectId,
        ownerId,
        assigneesIds: [],
      });
    });

    it("selects and filters permission magic columns end to end", async () => {
      const db = await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("select-magic-columns") },
        secret: "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
      });

      const { id: projectId } = insertProject(db, "Announcements");
      const { id: editableId } = insertTodo(db, {
        title: "Draft docs",
        done: false,
        tags: ["dev"],
        projectId,
        assigneesIds: [],
      });
      const { id: lockedId } = insertTodo(db, {
        title: "Shipped docs",
        done: true,
        tags: ["docs"],
        projectId,
        assigneesIds: [],
      });

      const projected = await db.all(
        app.todos.select("title", "$canRead", "$canEdit", "$canDelete").orderBy("title", "asc"),
      );

      expect(projected).toEqual([
        {
          id: editableId,
          title: "Draft docs",
          $canRead: true,
          $canEdit: true,
          $canDelete: true,
        },
        {
          id: lockedId,
          title: "Shipped docs",
          $canRead: true,
          $canEdit: false,
          $canDelete: false,
        },
      ]);

      const editableOnly = await db.all(
        app.todos.where({ $canEdit: true }).select("title", "$canEdit").orderBy("title", "asc"),
      );

      expect(editableOnly).toEqual([
        {
          id: editableId,
          title: "Draft docs",
          $canEdit: true,
        },
      ]);

      const readableOnly = await db.all(
        app.todos.where({ $canRead: true }).select("title", "$canRead").orderBy("title", "asc"),
      );

      expect(readableOnly).toEqual([
        {
          id: editableId,
          title: "Draft docs",
          $canRead: true,
        },
        {
          id: lockedId,
          title: "Shipped docs",
          $canRead: true,
        },
      ]);

      const deletableOnly = await db.all(
        app.todos.where({ $canDelete: true }).select("title", "$canDelete").orderBy("title", "asc"),
      );

      expect(deletableOnly).toEqual([
        {
          id: editableId,
          title: "Draft docs",
          $canDelete: true,
        },
      ]);

      const fullRowWithDeletePermission = await db.one(
        app.todos.select("*", "$canDelete").where({ id: { eq: editableId } }),
      );

      assert(fullRowWithDeletePermission, "Result is not defined");
      expectTypeOf(fullRowWithDeletePermission.$canDelete).toEqualTypeOf<boolean | null>();
      expect(fullRowWithDeletePermission).toEqual({
        id: editableId,
        title: "Draft docs",
        done: false,
        tags: ["dev"],
        projectId,
        ownerId: null,
        assigneesIds: [],
        $canDelete: true,
      });

      await db.shutdown();
    });

    it("selects and filters provenance magic timestamp columns as JS dates", async () => {
      const startedAt = Date.now();
      const { id: projectId } = insertProject(db, "Announcements");
      const { id: todoId } = insertTodo(db, {
        title: "Draft docs",
        done: false,
        tags: ["dev"],
        projectId,
        assigneesIds: [],
      });

      const projected = await db.one(
        app.todos.select("title", "$createdAt", "$updatedAt").where({ id: { eq: todoId } }),
      );

      assert(projected, "Result is not defined");
      expectTypeOf(projected.$createdAt).toEqualTypeOf<Date>();
      expectTypeOf(projected.$updatedAt).toEqualTypeOf<Date>();
      expect(projected.title).toBe("Draft docs");
      expect(projected.$createdAt).toBeInstanceOf(Date);
      expect(projected.$updatedAt).toBeInstanceOf(Date);
      expect(projected.$createdAt.getTime()).toBeGreaterThanOrEqual(startedAt - 60_000);
      expect(projected.$createdAt.getTime()).toBeLessThanOrEqual(Date.now() + 60_000);
      expect(projected.$updatedAt.getTime()).toBeGreaterThanOrEqual(startedAt - 60_000);
      expect(projected.$updatedAt.getTime()).toBeLessThanOrEqual(Date.now() + 60_000);

      const upperBound = new Date(Date.now() + 60_000);
      const withinUpperBound = await db.all(
        app.todos
          .where({ $updatedAt: { lte: upperBound } })
          .select("title", "$updatedAt")
          .orderBy("title", "asc"),
      );

      expect(withinUpperBound).toContainEqual(
        expect.objectContaining({
          id: todoId,
          title: "Draft docs",
          $updatedAt: projected.$updatedAt,
        }),
      );
    });

    it("include builders can project nested relation columns", async () => {
      const { id: projectId } = insertProject(db, "Announcements");
      const { id: ownerId } = insertUser(db);
      const { id: todoId } = insertTodo(db, {
        title: "Write tests",
        done: false,
        tags: ["dev"],
        projectId,
        ownerId,
        assigneesIds: [],
      });

      const result = await db.one(
        app.projects
          .where({ id: { eq: projectId } })
          .include({ todosViaProject: app.todos.select("title") }),
      );

      assert(result, "Result is not defined");
      expect(result).toEqual({
        id: projectId,
        name: "Announcements",
        todosViaProject: [
          {
            id: todoId,
            title: "Write tests",
          },
        ],
      });
      expectTypeOf(result.name).toEqualTypeOf<string>();
      expectTypeOf(result.todosViaProject).branded.toEqualTypeOf<{ id: string; title: string }[]>();
      expect("done" in result.todosViaProject[0]!).toBe(false);
      expect("tags" in result.todosViaProject[0]!).toBe(false);
      expect("project" in result.todosViaProject[0]!).toBe(false);
    });

    it("subscribeAll preserves projected root columns with includes", async () => {
      const { id: projectId } = insertProject(db, "Announcements");
      const { id: ownerId } = insertUser(db);

      type SubscribedTodo = {
        id: string;
        title: string;
        project: {
          id: string;
          name: string;
        };
      };

      let unsubscribe = () => {};
      let timeout: ReturnType<typeof setTimeout> | undefined;
      const deltaPromise = new Promise<{ all: SubscribedTodo[] }>((resolve, reject) => {
        timeout = setTimeout(() => {
          unsubscribe();
          reject(new Error("Timed out waiting for subscribeAll projection update"));
        }, 10_000);

        unsubscribe = db.subscribeAll(
          app.todos.select("title").include({ project: true }),
          (delta) => {
            if (delta.all.length !== 1) {
              return;
            }

            resolve(delta as { all: SubscribedTodo[] });
          },
        );
      });

      await new Promise((resolve) => setTimeout(resolve, 0));

      const { id: todoId } = insertTodo(db, {
        title: "Watch subscription",
        done: false,
        tags: ["dev"],
        projectId,
        ownerId,
        assigneesIds: [],
      });

      const delta = await deltaPromise;
      if (timeout) {
        clearTimeout(timeout);
      }
      unsubscribe();

      expect(delta.all).toEqual([
        {
          id: todoId,
          title: "Watch subscription",
          project: {
            id: projectId,
            name: "Announcements",
          },
        },
      ]);
      assert(delta.all[0]);
      expect("done" in delta.all[0]).toBe(false);
      expect("tags" in delta.all[0]).toBe(false);
    });

    it("subscribeAll preserves permission magic columns with the default auth context", async () => {
      const { id: projectId } = insertProject(db, "Announcements");
      const { id: editableId } = insertTodo(db, {
        title: "Draft docs",
        done: false,
        tags: ["dev"],
        projectId,
        assigneesIds: [],
      });
      const { id: lockedId } = insertTodo(db, {
        title: "Shipped docs",
        done: true,
        tags: ["docs"],
        projectId,
        assigneesIds: [],
      });

      type SubscribedTodo = {
        id: string;
        title: string;
        $canEdit: boolean | null;
        $canDelete: boolean | null;
      };

      let unsubscribe = () => {};
      let timeout: ReturnType<typeof setTimeout> | undefined;
      const deltaPromise = new Promise<{ all: SubscribedTodo[] }>((resolve, reject) => {
        timeout = setTimeout(() => {
          unsubscribe();
          reject(new Error("Timed out waiting for subscribeAll permission update"));
        }, 10_000);

        unsubscribe = db.subscribeAll(
          app.todos.select("title", "$canEdit", "$canDelete").orderBy("title", "asc"),
          (delta) => {
            if (delta.all.length !== 2) {
              return;
            }

            resolve(delta as { all: SubscribedTodo[] });
          },
        );
      });

      const delta = await deltaPromise;
      if (timeout) {
        clearTimeout(timeout);
      }
      unsubscribe();

      expect(delta.all).toEqual([
        {
          id: editableId,
          title: "Draft docs",
          $canEdit: true,
          $canDelete: true,
        },
        {
          id: lockedId,
          title: "Shipped docs",
          $canEdit: false,
          $canDelete: false,
        },
      ]);
    });

    it("subscribeAll returns null for selected nullable columns while omitting unselected columns", async () => {
      const { id: projectId } = insertProject(db, "Announcements");

      type SubscribedTodo = {
        id: string;
        title: string;
        ownerId: string | null;
      };

      let unsubscribe = () => {};
      let timeout: ReturnType<typeof setTimeout> | undefined;
      const deltaPromise = new Promise<{ all: SubscribedTodo[] }>((resolve, reject) => {
        timeout = setTimeout(() => {
          unsubscribe();
          reject(new Error("Timed out waiting for subscribeAll nullable update"));
        }, 10_000);

        unsubscribe = db.subscribeAll(app.todos.select("title", "ownerId"), (delta) => {
          if (delta.all.length !== 1) {
            return;
          }

          resolve(delta as { all: SubscribedTodo[] });
        });
      });

      await new Promise((resolve) => setTimeout(resolve, 0));

      const { id: todoId } = insertTodo(db, {
        title: "Watch nullable subscription",
        done: false,
        tags: ["dev"],
        projectId,
        ownerId: null,
        assigneesIds: [],
      });

      const delta = await deltaPromise;
      if (timeout) {
        clearTimeout(timeout);
      }
      unsubscribe();

      expect(delta.all).toEqual([
        {
          id: todoId,
          title: "Watch nullable subscription",
          ownerId: null,
        },
      ]);
      assert(delta.all[0]);
      expect("done" in delta.all[0]).toBe(false);
      expect("tags" in delta.all[0]).toBe(false);
    });
  });
});

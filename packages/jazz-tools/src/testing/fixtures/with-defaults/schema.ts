import { col } from "../../../dsl.js";
import { defineApp, type Schema, type App } from "../../../typed-app.js";

const schema = {
  todos: {
    title: col.string(),
    done: col.boolean().default(false),
  },
};

type AppSchema = Schema<typeof schema>;
export const app: App<AppSchema> = defineApp(schema);

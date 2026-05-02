import { schema as s } from "jazz-tools";

const schema = {
  canvases: s.table({
    created_at: s.timestamp(),
  }),
  letters: s.table({
    canvasId: s.ref("canvases"),
    value: s.string(),
    x: s.float(),
    y: s.float(),
    placed_by: s.string(),
  }),
  cursors: s.table({
    canvasId: s.ref("canvases"),
    user_id: s.string(),
    x: s.float(),
    y: s.float(),
    gesture: s.enum("pointer", "hand").default("pointer"),
    drag_value: s.string().optional().default(null),
    drag_x: s.float().optional().default(null),
    drag_y: s.float().optional().default(null),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Canvas = s.RowOf<typeof app.canvases>;
export type Letter = s.RowOf<typeof app.letters>;
export type Cursor = s.RowOf<typeof app.cursors>;

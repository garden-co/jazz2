// DSL for defining schemas and migrations

import type { StandardJSONSchemaV1 } from "@standard-schema/spec";
import type {
  Column,
  ColumnMergeStrategy,
  ColumnMergeStrategyName,
  Schema,
  Table,
  SqlType,
  EnumSqlType,
  JsonSqlType,
  JsonSchema,
  JsonValue,
  Lens,
  LensOp,
  AddOp,
  DropOp,
  RenameOp,
  MigrationOp,
  TableMigration,
  ScalarSqlType,
  TSTypeFromSqlType,
} from "./schema.js";
import { assertUserColumnNameAllowed } from "./magic-columns.js";

function normalizeEnumVariants(variants: readonly string[]): string[] {
  if (variants.length === 0) {
    throw new Error("Enum columns require at least one variant.");
  }
  for (const variant of variants) {
    if (variant.length === 0) {
      throw new Error("Enum variants cannot be empty strings.");
    }
  }
  const unique = new Set(variants);
  if (unique.size !== variants.length) {
    throw new Error("Enum variants must be unique.");
  }
  return [...unique].sort((a, b) => a.localeCompare(b));
}

type JsonSchemaSource<Output = JsonValue> = StandardJSONSchemaV1<unknown, Output> | JsonSchema;

function isJsonObject(value: unknown): value is JsonSchema {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeJsonSchema<Output>(schema: JsonSchemaSource<Output>): JsonSchema {
  const maybeStandard = "~standard" in schema ? schema["~standard"] : undefined;
  if (maybeStandard) {
    const converted = maybeStandard.jsonSchema.output({ target: "draft-07" });
    if (!isJsonObject(converted)) {
      throw new Error(
        "JSON schema conversion failed: expected an object from ~standard.jsonSchema.output(...).",
      );
    }
    return converted;
  }

  if (!isJsonObject(schema)) {
    throw new Error("JSON schema must be an object or implement ~standard.jsonSchema.");
  }
  return schema;
}

function jsonColumn(): JsonBuilder<JsonValue>;
function jsonColumn<Schema extends StandardJSONSchemaV1<unknown, unknown>>(
  schema: Schema,
): JsonBuilder<StandardJSONSchemaV1.InferOutput<Schema>>;
function jsonColumn(schema: JsonSchema): JsonBuilder<JsonValue>;
function jsonColumn(schema?: JsonSchemaSource): JsonBuilder {
  return new JsonBuilder(schema);
}

// ============================================================================
// Column Builder (for schema context)
// ============================================================================

interface ColumnBuilder {
  optional(): this;
  default(value: unknown): this;
  merge(strategy: ColumnMergeStrategyName): this;
  _build(name: string): Column;
  _sqlType: SqlType;
  _references: string | undefined;
}

type AllowedColumnMergeStrategy<
  Sql extends SqlType,
  Optional extends boolean,
> = Optional extends true ? "lww" : Sql extends "INTEGER" ? ColumnMergeStrategyName : "lww";

export type TypedColumnBuilder<
  Sql extends SqlType = SqlType,
  Optional extends boolean = boolean,
  Ref extends string | undefined = string | undefined,
  HasDefault extends boolean = boolean,
> = Omit<ColumnBuilder, "optional" | "default"> & {
  readonly __jazzSqlType: Sql;
  readonly __jazzOptional: Optional;
  readonly __jazzReferences: Ref;
  readonly __jazzHasDefault: HasDefault;
  /**
   * Set the default value for the column
   */
  default(
    value: MaybeOptional<TSTypeFromSqlType<Sql>, Optional>,
  ): ColumnAlias<Sql, Optional, Ref, true>;
  /**
   * Set the merge strategy for the column (defaults to LWW)
   */
  merge(
    strategy: AllowedColumnMergeStrategy<Sql, Optional>,
  ): ColumnAlias<Sql, Optional, Ref, HasDefault>;
  /**
   * Make the column nullable
   */
  optional(): ColumnAlias<Sql, true, Ref, HasDefault>;
};

export type AnyTypedColumnBuilder = TypedColumnBuilder<
  SqlType,
  boolean,
  string | undefined,
  boolean
>;
export type ColumnBuilderSqlType<TBuilder extends AnyTypedColumnBuilder> =
  TBuilder["__jazzSqlType"];
export type ColumnBuilderOptional<TBuilder extends AnyTypedColumnBuilder> =
  TBuilder["__jazzOptional"];
export type ColumnBuilderReferences<TBuilder extends AnyTypedColumnBuilder> =
  TBuilder["__jazzReferences"];
export type ColumnBuilderHasDefault<TBuilder extends AnyTypedColumnBuilder> =
  TBuilder["__jazzHasDefault"];

export type StringColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
> = TypedColumnBuilder<"TEXT", Optional, undefined, HasDefault>;
export type BooleanColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
> = TypedColumnBuilder<"BOOLEAN", Optional, undefined, HasDefault>;
export type IntColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
> = TypedColumnBuilder<"INTEGER", Optional, undefined, HasDefault>;
export type TimestampColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
> = TypedColumnBuilder<"TIMESTAMP", Optional, undefined, HasDefault>;
export type FloatColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
> = TypedColumnBuilder<"REAL", Optional, undefined, HasDefault>;
export type BytesColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
> = TypedColumnBuilder<"BYTEA", Optional, undefined, HasDefault>;
export type JsonColumn<
  Output = JsonValue,
  Optional extends boolean = false,
  HasDefault extends boolean = false,
> = TypedColumnBuilder<JsonSqlType<Output>, Optional, undefined, HasDefault>;
export type EnumColumn<
  Variants extends readonly string[] = readonly string[],
  Optional extends boolean = false,
  HasDefault extends boolean = false,
> = TypedColumnBuilder<
  {
    kind: "ENUM";
    variants: [...Variants];
  },
  Optional,
  undefined,
  HasDefault
>;
export type RefColumn<
  TargetTable extends string,
  Optional extends boolean = false,
  HasDefault extends boolean = false,
> = TypedColumnBuilder<"UUID", Optional, TargetTable, HasDefault>;
export type ArrayColumn<
  ElementSql extends SqlType = SqlType,
  Optional extends boolean = false,
  Ref extends string | undefined = undefined,
  HasDefault extends boolean = false,
> = TypedColumnBuilder<
  {
    kind: "ARRAY";
    element: ElementSql;
  },
  Optional,
  Ref,
  HasDefault
>;
export type ColumnAlias<
  Sql extends SqlType = SqlType,
  Optional extends boolean = boolean,
  Ref extends string | undefined = string | undefined,
  HasDefault extends boolean = boolean,
> = Sql extends {
  kind: "ARRAY";
  element: infer ElementSql extends SqlType;
}
  ? ArrayColumn<ElementSql, Optional, Ref, HasDefault>
  : Ref extends string
    ? RefColumn<Ref, Optional, HasDefault>
    : Sql extends "TEXT"
      ? StringColumn<Optional, HasDefault>
      : Sql extends "BOOLEAN"
        ? BooleanColumn<Optional, HasDefault>
        : Sql extends "INTEGER"
          ? IntColumn<Optional, HasDefault>
          : Sql extends "TIMESTAMP"
            ? TimestampColumn<Optional, HasDefault>
            : Sql extends "REAL"
              ? FloatColumn<Optional, HasDefault>
              : Sql extends "BYTEA"
                ? BytesColumn<Optional, HasDefault>
                : Sql extends JsonSqlType<infer Output>
                  ? JsonColumn<Output, Optional, HasDefault>
                  : Sql extends {
                        kind: "ENUM";
                        variants: infer Variants extends readonly string[];
                      }
                    ? EnumColumn<Variants, Optional, HasDefault>
                    : TypedColumnBuilder<Sql, Optional, Ref, HasDefault>;

type RefColumnKey = `${string}Id` | `${string}_id`;
type RefArrayColumnKey = `${string}Ids` | `${string}_ids`;

function isValidRefColumnKey(name: string): name is RefColumnKey {
  return name.endsWith("Id") || name.endsWith("_id");
}

function isValidRefArrayColumnKey(name: string): name is RefArrayColumnKey {
  return name.endsWith("Ids") || name.endsWith("_ids");
}

function validateReferenceColumnName(name: string, builder: ColumnBuilder): void {
  if (!builder._references) {
    return;
  }

  if (builder instanceof ArrayBuilder) {
    if (!isValidRefArrayColumnKey(name)) {
      throw new Error(
        `Invalid array reference key '${name}'. Rename it to '${name}_ids' or '${name}Ids'.`,
      );
    }
    return;
  }

  if (!isValidRefColumnKey(name)) {
    throw new Error(`Invalid reference key '${name}'. Rename it to '${name}_id' or '${name}Id'.`);
  }
}

function normalizeColumnMergeStrategy(
  strategy: ColumnMergeStrategyName,
  sqlType: SqlType,
  nullable: boolean,
): ColumnMergeStrategy | undefined {
  if (strategy === "lww") {
    return undefined;
  }
  if (sqlType !== "INTEGER" || nullable) {
    throw new Error("Counter merge strategy is only supported on non-nullable INTEGER columns.");
  }
  return "counter";
}

class ScalarBuilder implements ColumnBuilder {
  private _nullable = false;
  private _default: unknown = undefined;
  private _mergeStrategy: ColumnMergeStrategy | undefined;

  constructor(public _sqlType: ScalarSqlType) {}

  optional(): this {
    if (this._mergeStrategy === "counter") {
      throw new Error("Counter merge strategy is only supported on non-nullable INTEGER columns.");
    }
    this._nullable = true;
    return this;
  }

  default(value: unknown): this {
    this._default = value;
    return this;
  }

  merge(strategy: ColumnMergeStrategyName): this {
    this._mergeStrategy = normalizeColumnMergeStrategy(strategy, this._sqlType, this._nullable);
    return this;
  }

  _build(name: string): Column {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._default === undefined ? {} : { default: this._default }),
      ...(this._mergeStrategy === undefined ? {} : { mergeStrategy: this._mergeStrategy }),
    };
  }

  get _references(): string | undefined {
    return undefined;
  }
}

class EnumBuilder implements ColumnBuilder {
  private _nullable = false;
  private _default: unknown = undefined;
  private _mergeStrategy: ColumnMergeStrategy | undefined;
  public _sqlType: EnumSqlType;

  constructor(...variants: string[]) {
    this._sqlType = { kind: "ENUM", variants: normalizeEnumVariants(variants) };
  }

  optional(): this {
    if (this._mergeStrategy === "counter") {
      throw new Error("Counter merge strategy is only supported on non-nullable INTEGER columns.");
    }
    this._nullable = true;
    return this;
  }

  default(value: unknown): this {
    this._default = value;
    return this;
  }

  merge(strategy: ColumnMergeStrategyName): this {
    this._mergeStrategy = normalizeColumnMergeStrategy(strategy, this._sqlType, this._nullable);
    return this;
  }

  _build(name: string): Column {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._default === undefined ? {} : { default: this._default }),
      ...(this._mergeStrategy === undefined ? {} : { mergeStrategy: this._mergeStrategy }),
    };
  }

  get _references(): string | undefined {
    return undefined;
  }
}

class JsonBuilder<Output = JsonValue> implements ColumnBuilder {
  private _nullable = false;
  private _default: unknown = undefined;
  private _mergeStrategy: ColumnMergeStrategy | undefined;
  public _sqlType: JsonSqlType<Output>;

  constructor(schema?: JsonSchemaSource<Output>) {
    this._sqlType = schema
      ? { kind: "JSON", schema: normalizeJsonSchema(schema) }
      : { kind: "JSON" };
  }

  optional(): this {
    if (this._mergeStrategy === "counter") {
      throw new Error("Counter merge strategy is only supported on non-nullable INTEGER columns.");
    }
    this._nullable = true;
    return this;
  }

  default(value: unknown): this {
    this._default = value;
    return this;
  }

  merge(strategy: ColumnMergeStrategyName): this {
    this._mergeStrategy = normalizeColumnMergeStrategy(strategy, this._sqlType, this._nullable);
    return this;
  }

  _build(name: string): Column {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._default === undefined ? {} : { default: this._default }),
      ...(this._mergeStrategy === undefined ? {} : { mergeStrategy: this._mergeStrategy }),
    };
  }

  get _references(): string | undefined {
    return undefined;
  }
}

// ============================================================================
// Ref Builder (for foreign key references in schema context)
// ============================================================================

class RefBuilder implements ColumnBuilder {
  private _nullable = false;
  private _default: unknown = undefined;
  private _mergeStrategy: ColumnMergeStrategy | undefined;

  constructor(private _targetTable: string) {}

  optional(): this {
    if (this._mergeStrategy === "counter") {
      throw new Error("Counter merge strategy is only supported on non-nullable INTEGER columns.");
    }
    this._nullable = true;
    return this;
  }

  default(value: unknown): this {
    this._default = value;
    return this;
  }

  merge(strategy: ColumnMergeStrategyName): this {
    this._mergeStrategy = normalizeColumnMergeStrategy(strategy, this._sqlType, this._nullable);
    return this;
  }

  _build(name: string): Column {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._default === undefined ? {} : { default: this._default }),
      ...(this._mergeStrategy === undefined ? {} : { mergeStrategy: this._mergeStrategy }),
      references: this._references,
    };
  }

  get _sqlType(): SqlType {
    return "UUID";
  }

  get _references(): string | undefined {
    return this._targetTable;
  }
}

class ArrayBuilder<T extends ColumnBuilder> implements ColumnBuilder {
  private _nullable = false;
  private _default: unknown = undefined;
  private _mergeStrategy: ColumnMergeStrategy | undefined;

  constructor(public _element: T) {}

  optional(): this {
    if (this._mergeStrategy === "counter") {
      throw new Error("Counter merge strategy is only supported on non-nullable INTEGER columns.");
    }
    this._nullable = true;
    return this;
  }

  default(value: unknown): this {
    this._default = value;
    return this;
  }

  merge(strategy: ColumnMergeStrategyName): this {
    this._mergeStrategy = normalizeColumnMergeStrategy(strategy, this._sqlType, this._nullable);
    return this;
  }

  _build(name: string): Column {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._default === undefined ? {} : { default: this._default }),
      ...(this._mergeStrategy === undefined ? {} : { mergeStrategy: this._mergeStrategy }),
      references: this._references,
    };
  }

  get _sqlType(): SqlType {
    return { kind: "ARRAY" as const, element: this._element._sqlType };
  }

  get _references(): string | undefined {
    return this._element._references;
  }
}

// ============================================================================
// Add Builder (for migration context)
// ============================================================================

type MaybeOptional<T, Optional extends boolean> = Optional extends true ? T | null : T;
type ArrayElementSource = SqlType | AnyTypedColumnBuilder;
type ArrayElementSqlType<TElement extends ArrayElementSource> =
  TElement extends AnyTypedColumnBuilder ? ColumnBuilderSqlType<TElement> : TElement;
type ArrayElementValue<TElement extends ArrayElementSource> = TSTypeFromSqlType<
  ArrayElementSqlType<TElement>
>;

function isTypedColumnBuilder(value: ArrayElementSource): value is AnyTypedColumnBuilder {
  return typeof value === "object" && value !== null && "_build" in value && "_sqlType" in value;
}

class AddBuilder {
  string<const TDefault extends string | null>(opts: {
    default: TDefault;
  }): AddOp<"TEXT", TDefault> {
    return { _type: "add", sqlType: "TEXT", default: opts.default };
  }

  int<const TDefault extends number | null>(opts: {
    default: TDefault;
  }): AddOp<"INTEGER", TDefault> {
    return { _type: "add", sqlType: "INTEGER", default: opts.default };
  }

  timestamp<const TDefault extends Date | number | null>(opts: {
    default: TDefault;
  }): AddOp<"TIMESTAMP", TDefault> {
    return { _type: "add", sqlType: "TIMESTAMP", default: opts.default };
  }

  boolean<const TDefault extends boolean | null>(opts: {
    default: TDefault;
  }): AddOp<"BOOLEAN", TDefault> {
    return { _type: "add", sqlType: "BOOLEAN", default: opts.default };
  }

  float<const TDefault extends number | null>(opts: {
    default: TDefault;
  }): AddOp<"REAL", TDefault> {
    return { _type: "add", sqlType: "REAL", default: opts.default };
  }

  bytes<const TDefault extends Uint8Array | null>(opts: {
    default: TDefault;
  }): AddOp<"BYTEA", TDefault> {
    return { _type: "add", sqlType: "BYTEA", default: opts.default };
  }

  ref<const TTargetTable extends string, const TDefault extends string | null>(
    _targetTable: TTargetTable,
    opts: { default: TDefault },
  ): AddOp<"UUID", TDefault> {
    return { _type: "add", sqlType: "UUID", default: opts.default };
  }

  json<const TDefault extends JsonValue | null>(opts: {
    default: TDefault;
    schema?: JsonSchemaSource;
  }): AddOp<JsonSqlType, TDefault> {
    return {
      _type: "add",
      sqlType: opts.schema
        ? { kind: "JSON", schema: normalizeJsonSchema(opts.schema) }
        : { kind: "JSON" },
      default: opts.default,
    };
  }

  enum<
    const Variants extends readonly [string, ...string[]],
    const TDefault extends Variants[number] | null,
  >(
    ...args: [...variants: Variants, opts: { default: TDefault }]
  ): AddOp<{ kind: "ENUM"; variants: [...Variants] }, TDefault> {
    const opts = args[args.length - 1] as { default: TDefault };
    const variants = normalizeEnumVariants(args.slice(0, -1) as string[]);
    return {
      _type: "add",
      sqlType: { kind: "ENUM", variants: variants as [...Variants] },
      default: opts.default,
    };
  }

  array<
    TElement extends ArrayElementSource,
    const TDefault extends ArrayElementValue<TElement>[] | null,
  >(opts: {
    of: TElement;
    default: TDefault;
  }): AddOp<{ kind: "ARRAY"; element: ArrayElementSqlType<TElement> }, TDefault> {
    return {
      _type: "add",
      sqlType: {
        kind: "ARRAY",
        element: (isTypedColumnBuilder(opts.of)
          ? opts.of._sqlType
          : opts.of) as ArrayElementSqlType<TElement>,
      },
      default: opts.default,
    };
  }
}

// ============================================================================
// Drop Builder (for migration context)
// ============================================================================

class DropBuilder {
  string<const TBackwardsDefault extends string | null>(opts: {
    backwardsDefault: TBackwardsDefault;
  }): DropOp<"TEXT", TBackwardsDefault> {
    return { _type: "drop", sqlType: "TEXT", backwardsDefault: opts.backwardsDefault };
  }

  int<const TBackwardsDefault extends number | null>(opts: {
    backwardsDefault: TBackwardsDefault;
  }): DropOp<"INTEGER", TBackwardsDefault> {
    return { _type: "drop", sqlType: "INTEGER", backwardsDefault: opts.backwardsDefault };
  }

  timestamp<const TBackwardsDefault extends Date | number | null>(opts: {
    backwardsDefault: TBackwardsDefault;
  }): DropOp<"TIMESTAMP", TBackwardsDefault> {
    return { _type: "drop", sqlType: "TIMESTAMP", backwardsDefault: opts.backwardsDefault };
  }

  boolean<const TBackwardsDefault extends boolean | null>(opts: {
    backwardsDefault: TBackwardsDefault;
  }): DropOp<"BOOLEAN", TBackwardsDefault> {
    return { _type: "drop", sqlType: "BOOLEAN", backwardsDefault: opts.backwardsDefault };
  }

  float<const TBackwardsDefault extends number | null>(opts: {
    backwardsDefault: TBackwardsDefault;
  }): DropOp<"REAL", TBackwardsDefault> {
    return { _type: "drop", sqlType: "REAL", backwardsDefault: opts.backwardsDefault };
  }

  bytes<const TBackwardsDefault extends Uint8Array | null>(opts: {
    backwardsDefault: TBackwardsDefault;
  }): DropOp<"BYTEA", TBackwardsDefault> {
    return { _type: "drop", sqlType: "BYTEA", backwardsDefault: opts.backwardsDefault };
  }

  ref<const TTargetTable extends string, const TBackwardsDefault extends string | null>(
    _targetTable: TTargetTable,
    opts: { backwardsDefault: TBackwardsDefault },
  ): DropOp<"UUID", TBackwardsDefault> {
    return { _type: "drop", sqlType: "UUID", backwardsDefault: opts.backwardsDefault };
  }

  json<const TBackwardsDefault extends JsonValue | null>(opts: {
    backwardsDefault: TBackwardsDefault;
    schema?: JsonSchemaSource;
  }): DropOp<JsonSqlType, TBackwardsDefault> {
    return {
      _type: "drop",
      sqlType: opts.schema
        ? { kind: "JSON", schema: normalizeJsonSchema(opts.schema) }
        : { kind: "JSON" },
      backwardsDefault: opts.backwardsDefault,
    };
  }

  enum<
    const Variants extends readonly [string, ...string[]],
    const TBackwardsDefault extends Variants[number] | null,
  >(
    ...args: [...variants: Variants, opts: { backwardsDefault: TBackwardsDefault }]
  ): DropOp<{ kind: "ENUM"; variants: [...Variants] }, TBackwardsDefault> {
    const opts = args[args.length - 1] as { backwardsDefault: TBackwardsDefault };
    const variants = normalizeEnumVariants(args.slice(0, -1) as string[]);
    return {
      _type: "drop",
      sqlType: { kind: "ENUM", variants: variants as [...Variants] },
      backwardsDefault: opts.backwardsDefault,
    };
  }

  array<
    TElement extends ArrayElementSource,
    const TBackwardsDefault extends ArrayElementValue<TElement>[] | null,
  >(opts: {
    of: TElement;
    backwardsDefault: TBackwardsDefault;
  }): DropOp<{ kind: "ARRAY"; element: ArrayElementSqlType<TElement> }, TBackwardsDefault> {
    return {
      _type: "drop",
      sqlType: {
        kind: "ARRAY",
        element: (isTypedColumnBuilder(opts.of)
          ? opts.of._sqlType
          : opts.of) as ArrayElementSqlType<TElement>,
      },
      backwardsDefault: opts.backwardsDefault,
    };
  }
}

// ============================================================================
// col namespace
// ============================================================================

export const col = {
  // Schema context

  string: () => new ScalarBuilder("TEXT") as unknown as StringColumn,
  boolean: () => new ScalarBuilder("BOOLEAN") as unknown as BooleanColumn,
  int: () => new ScalarBuilder("INTEGER") as unknown as IntColumn,
  timestamp: () => new ScalarBuilder("TIMESTAMP") as unknown as TimestampColumn,
  float: () => new ScalarBuilder("REAL") as unknown as FloatColumn,
  bytes: () => new ScalarBuilder("BYTEA") as unknown as BytesColumn,
  json: jsonColumn as unknown as {
    (): JsonColumn;
    <Schema extends StandardJSONSchemaV1<unknown, unknown>>(
      schema: Schema,
    ): JsonColumn<StandardJSONSchemaV1.InferOutput<Schema>>;
    (schema: JsonSchema): JsonColumn;
  },
  enum: <const Variants extends readonly [string, ...string[]]>(...variants: Variants) =>
    new EnumBuilder(...variants) as unknown as EnumColumn<Variants>,
  ref: <const TargetTable extends string>(targetTable: TargetTable) =>
    new RefBuilder(targetTable) as unknown as RefColumn<TargetTable>,
  array: <Builder extends AnyTypedColumnBuilder>(element: Builder) =>
    new ArrayBuilder(element) as unknown as ArrayColumn<
      ColumnBuilderSqlType<Builder>,
      false,
      ColumnBuilderReferences<Builder>
    >,

  // Migration context

  /**
   * Add a new column to the table
   */
  add: new AddBuilder(),
  /**
   * Drop a column from the table
   */
  drop: new DropBuilder(),
  /**
   * Rename a column in the table
   * @deprecated Use {@link col.renameFrom} instead
   */
  rename: (oldName: string): RenameOp => ({ _type: "rename", oldName }),
  /**
   * Rename a column in the table
   */
  renameFrom: <const TOldName extends string>(oldName: TOldName): RenameOp<TOldName> => ({
    _type: "rename",
    oldName,
  }),
};

// ============================================================================
// Side-effect collection
// ============================================================================

let collectedTables: Table[] = [];
let collectedMigrations: TableMigration[] = [];

type ScalarIdColumnError<K extends string> =
  `Invalid reference key '${K}'. Rename it to '${K}_id' or '${K}Id'`;

type ArrayIdColumnError<K extends string> =
  `Invalid array reference key '${K}'. Rename it to '${K}_ids' or '${K}Ids'`;

type EnforceReferenceColumnNames<T extends Record<string, ColumnBuilder>> = {
  [K in keyof T & string]: T[K] extends RefBuilder
    ? K extends RefColumnKey
      ? T[K]
      : ScalarIdColumnError<K>
    : T[K] extends ArrayBuilder<RefBuilder>
      ? K extends RefArrayColumnKey
        ? T[K]
        : ArrayIdColumnError<K>
      : T[K];
};

export function table<const T extends Record<string, ColumnBuilder>>(
  name: string,
  columns: EnforceReferenceColumnNames<T>,
): void {
  if (arguments.length > 2) {
    throw new Error(
      "Inline table permissions are no longer supported in schema.ts. " +
        "Define policies in permissions.ts with definePermissions(...).",
    );
  }

  const cols: Column[] = [];
  for (const [colName, builder] of Object.entries(columns as Record<string, ColumnBuilder>)) {
    validateReferenceColumnName(colName, builder);
    assertUserColumnNameAllowed(colName);
    cols.push(builder._build(colName));
  }
  collectedTables.push({
    name,
    columns: cols,
  });
}

export function migrate(tableName: string, ops: Record<string, MigrationOp>): void {
  const operations = Object.entries(ops).map(([column, op]) => {
    if (op._type !== "drop") {
      assertUserColumnNameAllowed(column);
    }
    return { column, op };
  });
  collectedMigrations.push({ table: tableName, operations });
}

export function getCollectedSchema(): Schema {
  const schema = { tables: [...collectedTables] };
  collectedTables = [];
  return schema;
}

export function getCollectedMigration(): Lens | null {
  const migration = collectedMigrations.shift();
  if (!migration) {
    return null;
  }

  const operations: LensOp[] = migration.operations.map(({ column, op }) => {
    switch (op._type) {
      case "add":
        return {
          type: "introduce" as const,
          column,
          sqlType: op.sqlType,
          value: op.default,
        };
      case "drop":
        return {
          type: "drop" as const,
          column,
          sqlType: op.sqlType,
          value: op.backwardsDefault,
        };
      case "rename":
        return { type: "rename" as const, column, value: op.oldName };
    }
  });

  return { table: migration.table, operations };
}

export function getCollectedMigrations(): Lens[] {
  const migrations = [...collectedMigrations];
  collectedMigrations = [];
  return migrations.map((migration) => {
    const operations: LensOp[] = migration.operations.map(({ column, op }) => {
      switch (op._type) {
        case "add":
          return {
            type: "introduce" as const,
            column,
            sqlType: op.sqlType,
            value: op.default,
          };
        case "drop":
          return {
            type: "drop" as const,
            column,
            sqlType: op.sqlType,
            value: op.backwardsDefault,
          };
        case "rename":
          return { type: "rename" as const, column, value: op.oldName };
      }
    });
    return { table: migration.table, operations };
  });
}

export function resetCollectedState(): void {
  collectedTables = [];
  collectedMigrations = [];
}

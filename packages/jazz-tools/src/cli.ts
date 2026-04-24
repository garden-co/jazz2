#!/usr/bin/env node

// CLI for jazz-tools schema tooling

import { access, mkdir, readFile, readdir, rm, writeFile } from "fs/promises";
import { basename, dirname, join, resolve } from "path";
import { pathToFileURL } from "url";
import { build } from "esbuild";
import type {
  ColumnDescriptor,
  ColumnType as WasmColumnType,
  WasmSchema,
} from "./drivers/types.js";
import type { DefinedMigration } from "./migrations.js";
import { schemaDefinitionToAst } from "./migrations.js";
import type { Lens, SqlType } from "./schema.js";
import { loadCompiledSchema, type LoadedSchemaProject } from "./schema-loader.js";
import { collectMissingExplicitPolicyDiagnostics } from "./schema-permissions.js";
import {
  encodePublishedMigrationValue,
  fetchPermissionsHead,
  fetchSchemaConnectivity,
  fetchSchemaHashes,
  fetchStoredWasmSchema,
  publishStoredSchema,
  publishStoredPermissions,
  publishStoredMigration,
  type PublishedTableLens,
  type StoredPermissionsHead,
} from "./runtime/schema-fetch.js";
import { toValue } from "./runtime/value-converter.js";

export interface BuildOptions {
  jazzBin?: string;
  schemaDir: string;
}

export interface SchemaExportOptions {
  schemaDir: string;
  migrationsDir?: string;
  schemaHash?: string;
  appId?: string;
  serverUrl?: string;
  adminSecret?: string;
}

export interface SchemaHashOptions {
  schemaDir: string;
}

const PERMISSIONS_LIFECYCLE_NOTE =
  "Permission-only changes do not create schema hashes or require migrations.";

function parseArgs(): { command: string; options: BuildOptions } {
  const args = process.argv.slice(2);
  const command = args[0] || "";
  let schemaDir = process.cwd();
  let jazzBin: string | undefined;

  for (let i = 1; i < args.length; i++) {
    const arg = args[i];
    const nextArg = args[i + 1];
    if (arg === "--jazz-bin" && nextArg) {
      jazzBin = nextArg;
      i += 1;
    } else if (arg === "--schema-dir" && nextArg) {
      schemaDir = nextArg;
      i += 1;
    }
  }

  return { command, options: { jazzBin, schemaDir } };
}

let importCounter = 0;

async function bundleToTempFile(filePath: string): Promise<string> {
  const sourceDir = dirname(resolve(filePath));
  const outFile = join(sourceDir, `.jazz-bundle-${++importCounter}.mjs`);

  await build({
    entryPoints: [resolve(filePath)],
    bundle: true,
    format: "esm",
    platform: "node",
    outfile: outFile,
    packages: "external",
  });

  return outFile;
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

export async function validate(options: BuildOptions): Promise<void> {
  const compiled = await loadCompiledSchema(options.schemaDir);
  const tableCount = compiled.schema.tables.length;
  console.log(`Loaded structural schema from ${compiled.schemaFile}.`);
  if (compiled.permissionsFile) {
    console.log(`Loaded current permissions from ${compiled.permissionsFile}.`);
    console.log(PERMISSIONS_LIFECYCLE_NOTE);
    console.log(
      "Use `jazz-tools permissions status <appId>` or `jazz-tools deploy <appId>` for auth publication.",
    );
  }
  for (const diagnostic of collectMissingExplicitPolicyDiagnostics(
    compiled.schema.tables.map((table) => table.name),
    compiled.permissions,
  )) {
    console.warn(`\x1b[33m${diagnostic.message}\x1b[0m`);
  }
  console.log(`Validated ${tableCount} table${tableCount === 1 ? "" : "s"} in schema.ts.`);
}

export async function exportSchema(options: SchemaExportOptions): Promise<void> {
  if (options.schemaHash) {
    const schema = await resolveExportedSchemaByHash(options);
    process.stdout.write(`${JSON.stringify(schema, null, 2)}\n`);
    return;
  }

  const currentSchema = await loadCurrentSchema(options.schemaDir);
  await ensureLocalSnapshot(options.schemaDir, options.migrationsDir, currentSchema);
  process.stdout.write(`${JSON.stringify(currentSchema.schema, null, 2)}\n`);
}

export async function schemaHash(options: SchemaHashOptions): Promise<void> {
  const compiled = await loadCompiledSchema(options.schemaDir);
  console.log(`Loaded structural schema from ${compiled.schemaFile}.`);
  const hash = await computeSchemaHash(compiled.wasmSchema);
  console.log(`Current schema hash: ${shortSchemaHash(hash)}`);
}

export interface MigrationCommandOptions {
  appId?: string;
  serverUrl?: string;
  adminSecret?: string;
  migrationsDir: string;
  schemaDir?: string;
}

export interface PermissionsCommandOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  schemaDir: string;
}

export interface CreateMigrationOptions extends MigrationCommandOptions {
  schemaDir: string;
  fromHash?: string;
  toHash?: string;
  name?: string;
}

export interface PushMigrationOptions extends MigrationCommandOptions {
  // Can be a full hash or short hash prefix
  fromHash: string;
  // Can be a full hash or short hash prefix
  toHash: string;
}

export interface DeployOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  schemaDir: string;
  migrationsDir: string;
  noVerify?: boolean;
}

const SHORT_SCHEMA_HASH_LENGTH = 12;

// Framework bundlers (Vite, SvelteKit, Next.js, Expo) expose public env vars
// under their own prefix so the client bundle can read them. The CLI often
// runs in the same project, so accept those prefixed names as fallbacks for
// the canonical JAZZ_ form. The unprefixed JAZZ_ name always wins — it's the
// explicit opt-in when the framework var points somewhere else (e.g. prod).
// Admin/backend secrets stay unprefixed by design: a PUBLIC_/VITE_/NEXT_PUBLIC_
// prefix would leak them into the client bundle.
export const SERVER_URL_ENV_VARS = [
  "JAZZ_SERVER_URL",
  "PUBLIC_JAZZ_SERVER_URL",
  "VITE_JAZZ_SERVER_URL",
  "NEXT_PUBLIC_JAZZ_SERVER_URL",
  "EXPO_PUBLIC_JAZZ_SERVER_URL",
] as const;

export const APP_ID_ENV_VARS = [
  "JAZZ_APP_ID",
  "PUBLIC_JAZZ_APP_ID",
  "VITE_JAZZ_APP_ID",
  "NEXT_PUBLIC_JAZZ_APP_ID",
  "EXPO_PUBLIC_JAZZ_APP_ID",
] as const;

export function resolveEnvVar(
  names: readonly string[],
  env: Record<string, string | undefined> = process.env,
): string | undefined {
  for (const name of names) {
    const value = env[name];
    if (value) return value;
  }
  return undefined;
}

function getFlagValue(args: string[], flag: string): string | undefined {
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (!arg) {
      continue;
    }
    if (arg === flag) {
      return args[i + 1];
    }
    const prefix = `${flag}=`;
    if (arg.startsWith(prefix)) {
      return arg.slice(prefix.length);
    }
  }
  return undefined;
}

function hasFlag(args: string[], flag: string): boolean {
  return args.includes(flag);
}

function splitLeadingAppId(args: string[]): { appId?: string; args: string[] } {
  const first = args[0];
  if (!first || first.startsWith("-")) {
    return { args: args, appId: resolveEnvVar(APP_ID_ENV_VARS) };
  }

  return {
    appId: first,
    args: args.slice(1),
  };
}

function resolveMigrationOptions(args: string[]): MigrationCommandOptions {
  const serverUrl = getFlagValue(args, "--server-url") ?? resolveEnvVar(SERVER_URL_ENV_VARS);
  const adminSecret = getFlagValue(args, "--admin-secret") ?? process.env.JAZZ_ADMIN_SECRET;
  const migrationsDir = resolve(
    process.cwd(),
    getFlagValue(args, "--migrations-dir") ?? join(process.cwd(), "migrations"),
  );
  const schemaDir = resolve(process.cwd(), getFlagValue(args, "--schema-dir") ?? process.cwd());

  return {
    serverUrl,
    adminSecret,
    migrationsDir,
    schemaDir,
  };
}

function resolvePermissionsOptions(args: string[]): Omit<PermissionsCommandOptions, "appId"> {
  const serverUrl = getFlagValue(args, "--server-url") ?? resolveEnvVar(SERVER_URL_ENV_VARS);
  const adminSecret = getFlagValue(args, "--admin-secret") ?? process.env.JAZZ_ADMIN_SECRET;
  const schemaDir = resolve(process.cwd(), getFlagValue(args, "--schema-dir") ?? process.cwd());

  if (!serverUrl) {
    throw new Error(
      "Missing server URL. Pass --server-url <url> or set JAZZ_SERVER_URL (or a framework-prefixed form such as VITE_JAZZ_SERVER_URL).",
    );
  }

  if (!adminSecret) {
    throw new Error("Missing admin secret. Pass --admin-secret <secret> or set JAZZ_ADMIN_SECRET.");
  }

  return {
    serverUrl,
    adminSecret,
    schemaDir,
  };
}

function normalizeSchemaHashInput(hash: string, label: string): string {
  const normalized = hash.trim().toLowerCase();
  if (!/^[0-9a-f]{12,64}$/.test(normalized)) {
    throw new Error(`${label} must be a 12-64 character lowercase hex schema hash.`);
  }
  return normalized;
}

function shortSchemaHash(hash: string): string {
  return normalizeSchemaHashInput(hash, "schema hash").slice(0, SHORT_SCHEMA_HASH_LENGTH);
}

function hashMatchesFullSchema(hash: string, fullHash: string): boolean {
  return fullHash.startsWith(normalizeSchemaHashInput(hash, "schema hash"));
}

function resolveKnownSchemaHash(
  hash: string,
  label: string,
  knownHashes: readonly string[],
): string {
  const normalized = normalizeSchemaHashInput(hash, label);

  if (normalized.length === 64) {
    if (!knownHashes.includes(normalized)) {
      throw new Error(`No stored schema found for ${label} ${normalized}.`);
    }
    return normalized;
  }

  const matches = knownHashes.filter((candidate) => candidate.startsWith(normalized));
  if (matches.length === 0) {
    throw new Error(`No stored schema found for ${label} prefix ${normalized}.`);
  }
  if (matches.length > 1) {
    throw new Error(
      `${label} prefix ${normalized} is ambiguous: ${matches
        .map((candidate) => shortSchemaHash(candidate))
        .join(", ")}`,
    );
  }
  return matches[0]!;
}

function defaultMigrationsDir(schemaDir: string): string {
  return join(schemaDir, "migrations");
}

function resolvedMigrationsDir(schemaDir: string, migrationsDir?: string): string {
  return migrationsDir ?? defaultMigrationsDir(schemaDir);
}

function snapshotsDirForMigrations(migrationsDir: string): string {
  return join(migrationsDir, "snapshots");
}

function snapshotsDir(schemaDir: string, migrationsDir?: string): string {
  return snapshotsDirForMigrations(resolvedMigrationsDir(schemaDir, migrationsDir));
}

interface SnapshotEntry {
  hash: string;
  fileName: string;
  filePath: string;
  schema: WasmSchema;
}

// Supports both millisecond and microsecond-precision timestamps
function looksLikeSnapshotFileName(fileName: string): boolean {
  return /^(?:\d{8,17}T\d{6}-)?[0-9a-f]{12}\.json$/i.test(fileName);
}

async function readSnapshotEntry(dir: string, fileName: string): Promise<SnapshotEntry | null> {
  if (!looksLikeSnapshotFileName(fileName)) {
    return null;
  }

  const filePath = join(dir, fileName);
  const schema = JSON.parse(await readFile(filePath, "utf8")) as WasmSchema;
  return {
    hash: await computeSchemaHash(schema),
    fileName,
    filePath,
    schema,
  };
}

async function listSnapshotEntries(dir: string): Promise<SnapshotEntry[]> {
  if (!(await pathExists(dir))) {
    return [];
  }

  const files = await readdir(dir);
  return (await Promise.all(files.map((fileName) => readSnapshotEntry(dir, fileName)))).filter(
    (entry): entry is SnapshotEntry => entry !== null,
  );
}

async function listLocalSnapshotEntries(
  schemaDir: string,
  migrationsDir?: string,
): Promise<SnapshotEntry[]> {
  return listSnapshotEntries(snapshotsDir(schemaDir, migrationsDir));
}

async function listSnapshotEntriesForMigrations(migrationsDir: string): Promise<SnapshotEntry[]> {
  return listSnapshotEntries(snapshotsDirForMigrations(migrationsDir));
}

async function resolveLocalSnapshotEntry(
  schemaDir: string,
  migrationsDir: string | undefined,
  hash: string,
  label: string,
): Promise<SnapshotEntry | null> {
  const entries = await listLocalSnapshotEntries(schemaDir, migrationsDir);
  if (entries.length === 0) {
    return null;
  }

  const fullHash = normalizeSchemaHashInput(hash, label);
  if (fullHash.length === 64) {
    return entries.find((entry) => entry.hash === fullHash) ?? null;
  }

  const matches = entries.filter((entry) => hashMatchesFullSchema(fullHash, entry.hash));
  if (matches.length === 0) {
    return null;
  }
  if (matches.length > 1) {
    throw new Error(
      `${label} prefix ${fullHash} is ambiguous: ${matches
        .map((entry) => shortSchemaHash(entry.hash))
        .join(", ")}`,
    );
  }
  return matches[0]!;
}

async function loadLocalSnapshotSchema(
  schemaDir: string,
  migrationsDir: string | undefined,
  hash: string,
  label: string,
): Promise<{ hash: string; schema: WasmSchema } | null> {
  const entry = await resolveLocalSnapshotEntry(schemaDir, migrationsDir, hash, label);
  if (!entry) {
    return null;
  }

  return {
    hash: entry.hash,
    schema: entry.schema,
  };
}

async function writeSnapshotSchema(
  schemaDir: string,
  migrationsDir: string | undefined,
  hash: string,
  schema: WasmSchema,
  timestamp: string = createTimestamp(),
): Promise<string> {
  const dir = snapshotsDir(schemaDir, migrationsDir);
  await mkdir(dir, { recursive: true });
  const filePath = join(dir, snapshotFilename(hash, timestamp));
  await writeFile(filePath, `${JSON.stringify(schema, null, 2)}\n`);
  return filePath;
}

async function ensureLocalSnapshot(
  schemaDir: string,
  migrationsDir: string | undefined,
  schema: { hash: string; schema: WasmSchema },
): Promise<string | null> {
  const entries = await listLocalSnapshotEntries(schemaDir, migrationsDir);
  if (entries.some((entry) => entry.hash === schema.hash)) {
    return null;
  }

  return writeSnapshotSchema(schemaDir, migrationsDir, schema.hash, schema.schema);
}

async function writeSnapshotSchemaForMigrations(
  migrationsDir: string,
  fileName: string,
  schema: WasmSchema,
): Promise<string> {
  const dir = snapshotsDirForMigrations(migrationsDir);
  await mkdir(dir, { recursive: true });
  const filePath = join(dir, fileName);
  await writeFile(filePath, `${JSON.stringify(schema, null, 2)}\n`);
  return filePath;
}

function requireSchemaExportServerValue(
  value: string | undefined,
  kind: "serverUrl" | "adminSecret",
): string {
  if (value) {
    return value;
  }

  if (kind === "serverUrl") {
    throw new Error(
      "Missing server URL. Pass --server-url <url> or set JAZZ_SERVER_URL (or a framework-prefixed form such as VITE_JAZZ_SERVER_URL).",
    );
  }

  throw new Error("Missing admin secret. Pass --admin-secret <secret> or set JAZZ_ADMIN_SECRET.");
}

function requireAppId(appId: string | undefined): string {
  if (appId) {
    return appId;
  }

  throw new Error(
    "Missing app ID. Pass an <appId> positional argument or set JAZZ_APP_ID (or a framework-prefixed form such as VITE_JAZZ_APP_ID).",
  );
}

function requireMigrationServerOptions(options: MigrationCommandOptions): {
  appId: string;
  serverUrl: string;
  adminSecret: string;
} {
  return {
    appId: requireAppId(options.appId),
    serverUrl: requireSchemaExportServerValue(options.serverUrl, "serverUrl"),
    adminSecret: requireSchemaExportServerValue(options.adminSecret, "adminSecret"),
  };
}

async function resolveExportedSchemaByHash(options: SchemaExportOptions): Promise<WasmSchema> {
  const schemaHash = normalizeSchemaHashInput(options.schemaHash!, "schema hash");
  const local = await loadLocalSnapshotSchema(
    options.schemaDir,
    options.migrationsDir,
    schemaHash,
    "schema hash",
  );
  if (local) {
    return local.schema;
  }

  const serverUrl = requireSchemaExportServerValue(
    options.serverUrl ?? resolveEnvVar(SERVER_URL_ENV_VARS),
    "serverUrl",
  );
  const adminSecret = requireSchemaExportServerValue(
    options.adminSecret ?? process.env.JAZZ_ADMIN_SECRET,
    "adminSecret",
  );
  const appId = requireAppId(options.appId);

  const resolvedHash =
    schemaHash.length === 64
      ? schemaHash
      : resolveKnownSchemaHash(
          schemaHash,
          "schema hash",
          (await fetchSchemaHashes(serverUrl, { appId, adminSecret })).hashes,
        );
  const storedSchema = await fetchStoredWasmSchema(serverUrl, {
    appId,
    adminSecret,
    schemaHash: resolvedHash,
  });
  await writeSnapshotSchema(
    options.schemaDir,
    options.migrationsDir,
    resolvedHash,
    storedSchema.schema,
    createSnapshotTimestampFromPublishedAt(storedSchema.publishedAt),
  );
  return storedSchema.schema;
}

let wasmModulePromise: Promise<any> | null = null;

async function loadCliWasmModule(): Promise<any> {
  if (!wasmModulePromise) {
    wasmModulePromise = import("./runtime/client.js").then(({ loadWasmModule }) =>
      loadWasmModule(),
    );
  }
  return wasmModulePromise;
}

async function computeSchemaHash(schema: WasmSchema): Promise<string> {
  const wasmModule = await loadCliWasmModule();
  const runtime = new wasmModule.WasmRuntime(
    JSON.stringify(schema),
    "jazz-tools-cli",
    "dev",
    "main",
    null,
    null,
  );

  try {
    return runtime.getSchemaHash();
  } finally {
    if (typeof runtime.free === "function") {
      runtime.free();
    }
  }
}

function columnTypeSignature(columnType: WasmColumnType): string {
  return JSON.stringify(columnType);
}

function columnsEqual(left: ColumnDescriptor, right: ColumnDescriptor): boolean {
  return (
    left.name === right.name &&
    left.nullable === right.nullable &&
    left.references === right.references &&
    left.merge_strategy === right.merge_strategy &&
    columnTypeSignature(left.column_type) === columnTypeSignature(right.column_type)
  );
}

function tableSchemasEqual(
  left: WasmSchema[string] | undefined,
  right: WasmSchema[string] | undefined,
): boolean {
  if (!left || !right) {
    return false;
  }

  if (left.columns.length !== right.columns.length) {
    return false;
  }

  const leftColumns = [...left.columns].sort((a, b) => a.name.localeCompare(b.name));
  const rightColumns = [...right.columns].sort((a, b) => a.name.localeCompare(b.name));

  return leftColumns.every((column, index) => columnsEqual(column, rightColumns[index]!));
}

function tableSchemasRequireRowTransform(
  left: WasmSchema[string] | undefined,
  right: WasmSchema[string] | undefined,
): boolean {
  if (!left || !right) {
    return true;
  }

  const leftColumnNames = left.columns.map((column) => column.name).sort();
  const rightColumnNames = right.columns.map((column) => column.name).sort();

  if (leftColumnNames.length !== rightColumnNames.length) {
    return true;
  }

  for (const [index, columnName] of leftColumnNames.entries()) {
    if (columnName !== rightColumnNames[index]) {
      return true;
    }
  }

  const leftColumns = new Map(left.columns.map((column) => [column.name, column]));
  const rightColumns = new Map(right.columns.map((column) => [column.name, column]));

  return leftColumnNames.some((columnName) => {
    const leftColumn = leftColumns.get(columnName)!;
    const rightColumn = rightColumns.get(columnName)!;
    return (
      leftColumn.nullable !== rightColumn.nullable ||
      leftColumn.references !== rightColumn.references ||
      columnTypeSignature(leftColumn.column_type) !== columnTypeSignature(rightColumn.column_type)
    );
  });
}

function wasmSchemasEqual(left: WasmSchema, right: WasmSchema): boolean {
  const leftTableNames = Object.keys(left).sort();
  const rightTableNames = Object.keys(right).sort();

  if (leftTableNames.length !== rightTableNames.length) {
    return false;
  }

  return leftTableNames.every((tableName, index) => {
    if (tableName !== rightTableNames[index]) {
      return false;
    }
    return tableSchemasEqual(left[tableName], right[tableName]);
  });
}

function schemaTransitionRequiresRowTransform(
  fromSchema: WasmSchema,
  toSchema: WasmSchema,
): boolean {
  const fromTableNames = Object.keys(fromSchema).sort();
  const toTableNames = Object.keys(toSchema).sort();

  if (fromTableNames.length !== toTableNames.length) {
    return true;
  }

  for (const [index, tableName] of fromTableNames.entries()) {
    if (tableName !== toTableNames[index]) {
      return true;
    }
  }

  return fromTableNames.some((tableName) =>
    tableSchemasRequireRowTransform(fromSchema[tableName], toSchema[tableName]),
  );
}

function changedTableNames(fromSchema: WasmSchema, toSchema: WasmSchema): string[] {
  const names = new Set([...Object.keys(fromSchema), ...Object.keys(toSchema)]);
  return [...names].filter(
    (tableName) => !tableSchemasEqual(fromSchema[tableName], toSchema[tableName]),
  );
}

type TableRenameSuggestion = {
  oldTableName: string;
  newTableName: string;
};

function detectPossibleTableRenames(
  fromSchema: WasmSchema,
  toSchema: WasmSchema,
): TableRenameSuggestion[] {
  const removedTables = Object.keys(fromSchema)
    .filter((tableName) => !toSchema[tableName])
    .sort();
  const addedTables = Object.keys(toSchema)
    .filter((tableName) => !fromSchema[tableName])
    .sort();
  const matches = removedTables
    .map((oldTableName) => {
      const candidateAddedTables = addedTables.filter((newTableName) =>
        tableSchemasEqual(fromSchema[oldTableName], toSchema[newTableName]),
      );
      return candidateAddedTables.length === 1
        ? ([oldTableName, candidateAddedTables[0]!] as const)
        : undefined;
    })
    .filter((match) => match !== undefined);

  return matches.flatMap(([oldTableName, newTableName], i) => {
    const isDuplicateNewTableMatch = matches.some(([_, otherNewTableName], j) => {
      return i !== j && newTableName === otherNewTableName;
    });
    return !isDuplicateNewTableMatch ? [{ oldTableName, newTableName }] : [];
  });
}

function ensurePermissionsProject(compiled: LoadedSchemaProject): LoadedSchemaProject & {
  permissions: NonNullable<LoadedSchemaProject["permissions"]>;
  permissionsFile: string;
} {
  if (!compiled.permissions || !compiled.permissionsFile) {
    throw new Error(
      "No permissions found for this app. Create a permissions.ts file before using permissions commands.",
    );
  }

  return compiled as LoadedSchemaProject & {
    permissions: NonNullable<LoadedSchemaProject["permissions"]>;
    permissionsFile: string;
  };
}

/**
 * If the provided schema exists in the server, returns its hash. Otherwise, throws an error.
 */
async function resolveStoredStructuralSchemaHashOrThrow(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  wasmSchema: WasmSchema,
): Promise<string> {
  const hash = await resolveStoredStructuralSchemaHash(appId, serverUrl, adminSecret, wasmSchema);
  if (!hash) {
    throw new Error(
      "No stored structural schema matches the local schema.ts. Publish the structural schema before pushing permissions.",
    );
  }

  return hash;
}

async function resolveStoredStructuralSchemaHash(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  wasmSchema: WasmSchema,
): Promise<string | null> {
  const { hashes } = await fetchSchemaHashes(serverUrl, { appId, adminSecret });
  const storedSchemas = await Promise.all(
    hashes.map(async (hash) => ({
      hash,
      schema: (await fetchStoredWasmSchema(serverUrl, { appId, adminSecret, schemaHash: hash }))
        .schema,
    })),
  );

  const match = storedSchemas.find(({ schema }) => wasmSchemasEqual(schema, wasmSchema));
  return match?.hash ?? null;
}

function pickWitnessSchema(schema: WasmSchema, tableNames: readonly string[]): WasmSchema {
  const uniqueTableNames = [...new Set(tableNames)];
  return Object.fromEntries(
    uniqueTableNames
      .filter((tableName) => schema[tableName])
      .map((tableName) => [tableName, schema[tableName]!]),
  );
}

function indentBlock(text: string, indent: number): string {
  const prefix = " ".repeat(indent);
  return text
    .split("\n")
    .map((line) => (line.length === 0 ? line : `${prefix}${line}`))
    .join("\n");
}

function baseBuilderExpression(columnType: WasmColumnType, references?: string): string {
  switch (columnType.type) {
    case "Text":
      return "s.string()";
    case "Boolean":
      return "s.boolean()";
    case "Integer":
      return "s.int()";
    case "Double":
      return "s.float()";
    case "Timestamp":
      return "s.timestamp()";
    case "Bytea":
      return "s.bytes()";
    case "Json":
      return columnType.schema ? `s.json(${JSON.stringify(columnType.schema)})` : "s.json()";
    case "Enum":
      return `s.enum(${columnType.variants.map((variant) => JSON.stringify(variant)).join(", ")})`;
    case "Uuid":
      if (!references) {
        throw new Error("Migration stub generation does not yet support bare UUID columns.");
      }
      return `s.ref(${JSON.stringify(references)})`;
    case "Array":
      return `s.array(${baseBuilderExpression(columnType.element, references)})`;
    case "BigInt":
      throw new Error("Migration stub generation does not yet support BIGINT columns.");
    case "Row":
      throw new Error("Migration stub generation does not yet support row-valued columns.");
  }
}

function builderExpressionForColumn(column: ColumnDescriptor): string {
  const base = baseBuilderExpression(column.column_type, column.references);
  const withOptional = column.nullable ? `${base}.optional()` : base;
  if (column.merge_strategy === "Counter") {
    return `${withOptional}.merge("counter")`;
  }
  return withOptional;
}

function sqlTypeToWasmColumnType(sqlType: SqlType): WasmColumnType {
  if (typeof sqlType === "string") {
    switch (sqlType) {
      case "TEXT":
        return { type: "Text" };
      case "BOOLEAN":
        return { type: "Boolean" };
      case "INTEGER":
        return { type: "Integer" };
      case "REAL":
        return { type: "Double" };
      case "TIMESTAMP":
        return { type: "Timestamp" };
      case "UUID":
        return { type: "Uuid" };
      case "BYTEA":
        return { type: "Bytea" };
    }
  }

  if (sqlType.kind === "ENUM") {
    return {
      type: "Enum",
      variants: [...sqlType.variants],
    };
  }

  if (sqlType.kind === "JSON") {
    return {
      type: "Json",
      schema: sqlType.schema,
    };
  }

  return {
    type: "Array",
    element: sqlTypeToWasmColumnType(sqlType.element),
  };
}

function serializeForwardLenses(forward: readonly Lens[]): PublishedTableLens[] {
  return forward.map((tableLens) => ({
    table: tableLens.table,
    added: tableLens.added,
    removed: tableLens.removed,
    renamedFrom: tableLens.renamedFrom,
    operations: tableLens.operations.map((op) => {
      if (op.type === "rename") {
        return op;
      }

      const columnType = sqlTypeToWasmColumnType(op.sqlType);
      const value = encodePublishedMigrationValue(toValue(op.value, columnType));

      return {
        type: op.type,
        column: op.column,
        column_type: columnType,
        value,
      };
    }),
  }));
}

function renderSchemaWitness(schema: WasmSchema): string {
  const tableEntries = Object.entries(schema)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([tableName, tableSchema]) => {
      const columnLines = tableSchema.columns.map(
        (column) => `${JSON.stringify(column.name)}: ${builderExpressionForColumn(column)},`,
      );
      return `${JSON.stringify(tableName)}: s.table({\n${indentBlock(columnLines.join("\n"), 2)}\n})`;
    });

  if (tableEntries.length === 0) {
    return "{}";
  }

  return `{\n${indentBlock(tableEntries.join(",\n"), 2)}\n}`;
}

type TableSuggestion = {
  tableName: string;
  comments: string[];
  properties: string[];
};

function renderArrayElementExpression(columnType: WasmColumnType, references?: string): string {
  return baseBuilderExpression(columnType, references);
}

function renderAddOperationExpression(column: ColumnDescriptor, defaultExpression: string): string {
  switch (column.column_type.type) {
    case "Text":
      return `s.add.string({ default: ${defaultExpression} })`;
    case "Boolean":
      return `s.add.boolean({ default: ${defaultExpression} })`;
    case "Integer":
      return `s.add.int({ default: ${defaultExpression} })`;
    case "Double":
      return `s.add.float({ default: ${defaultExpression} })`;
    case "Timestamp":
      return `s.add.timestamp({ default: ${defaultExpression} })`;
    case "Bytea":
      return `s.add.bytes({ default: ${defaultExpression} })`;
    case "Json":
      return column.column_type.schema
        ? `s.add.json({ default: ${defaultExpression}, schema: ${JSON.stringify(column.column_type.schema)} })`
        : `s.add.json({ default: ${defaultExpression} })`;
    case "Enum":
      return `s.add.enum(${column.column_type.variants
        .map((variant) => JSON.stringify(variant))
        .join(", ")}, { default: ${defaultExpression} })`;
    case "Uuid":
      if (column.references) {
        return `s.add.ref(${JSON.stringify(column.references)}, { default: ${defaultExpression} })`;
      }
      return `s.add.ref("TODO_TABLE", { default: ${defaultExpression} })`;
    case "Array":
      return `s.add.array({ of: ${renderArrayElementExpression(column.column_type.element, column.references)}, default: ${defaultExpression} })`;
    case "BigInt":
      throw new Error("Migration stub generation does not yet support BIGINT columns.");
    case "Row":
      throw new Error("Migration stub generation does not yet support row-valued columns.");
  }
}

function renderDropOperationExpression(
  column: ColumnDescriptor,
  defaultExpression: string,
): string {
  switch (column.column_type.type) {
    case "Text":
      return `s.drop.string({ backwardsDefault: ${defaultExpression} })`;
    case "Boolean":
      return `s.drop.boolean({ backwardsDefault: ${defaultExpression} })`;
    case "Integer":
      return `s.drop.int({ backwardsDefault: ${defaultExpression} })`;
    case "Double":
      return `s.drop.float({ backwardsDefault: ${defaultExpression} })`;
    case "Timestamp":
      return `s.drop.timestamp({ backwardsDefault: ${defaultExpression} })`;
    case "Bytea":
      return `s.drop.bytes({ backwardsDefault: ${defaultExpression} })`;
    case "Json":
      return column.column_type.schema
        ? `s.drop.json({ backwardsDefault: ${defaultExpression}, schema: ${JSON.stringify(column.column_type.schema)} })`
        : `s.drop.json({ backwardsDefault: ${defaultExpression} })`;
    case "Enum":
      return `s.drop.enum(${column.column_type.variants
        .map((variant) => JSON.stringify(variant))
        .join(", ")}, { backwardsDefault: ${defaultExpression} })`;
    case "Uuid":
      if (column.references) {
        return `s.drop.ref(${JSON.stringify(column.references)}, { backwardsDefault: ${defaultExpression} })`;
      }
      return `s.drop.ref("TODO_TABLE", { backwardsDefault: ${defaultExpression} })`;
    case "Array":
      return `s.drop.array({ of: ${renderArrayElementExpression(column.column_type.element, column.references)}, backwardsDefault: ${defaultExpression} })`;
    case "BigInt":
      throw new Error("Migration stub generation does not yet support BIGINT columns.");
    case "Row":
      throw new Error("Migration stub generation does not yet support row-valued columns.");
  }
}

function inferTableSuggestions(
  tableName: string,
  fromTable: WasmSchema[string],
  toTable: WasmSchema[string],
): TableSuggestion {
  const fromColumns = new Map(fromTable.columns.map((column) => [column.name, column]));
  const toColumns = new Map(toTable.columns.map((column) => [column.name, column]));
  const comments: string[] = [];
  const properties: string[] = [];

  const removedColumns = [...fromColumns.keys()].filter((name) => !toColumns.has(name));
  const addedColumns = [...toColumns.keys()].filter((name) => !fromColumns.has(name));

  if (removedColumns.length === 1 && addedColumns.length === 1) {
    const removed = fromColumns.get(removedColumns[0]!)!;
    const added = toColumns.get(addedColumns[0]!)!;
    if (
      removed.nullable === added.nullable &&
      removed.references === added.references &&
      columnTypeSignature(removed.column_type) === columnTypeSignature(added.column_type)
    ) {
      comments.push(
        `Possible rename detected: ${JSON.stringify(removed.name)} -> ${JSON.stringify(added.name)}.`,
      );
    }
  }

  for (const columnName of addedColumns) {
    const column = toColumns.get(columnName)!;
    if (column.nullable) {
      properties.push(
        `${JSON.stringify(columnName)}: ${renderAddOperationExpression(column, "null")},`,
      );
    } else {
      comments.push(
        `Added required column ${JSON.stringify(columnName)} needs an explicit default.`,
      );
    }
  }

  for (const columnName of removedColumns) {
    const column = fromColumns.get(columnName)!;
    if (column.nullable) {
      properties.push(
        `${JSON.stringify(columnName)}: ${renderDropOperationExpression(column, "null")},`,
      );
    } else {
      comments.push(
        `Removed required column ${JSON.stringify(columnName)} needs an explicit backwardsDefault.`,
      );
    }
  }

  return {
    tableName,
    comments,
    properties,
  };
}

function renderMigrationBody(
  fromSchema: WasmSchema,
  toSchema: WasmSchema,
): {
  migrateBody?: string;
  renameTablesBody?: string;
  createTablesBody?: string;
  dropTablesBody?: string;
  witnessFrom: WasmSchema;
  witnessTo: WasmSchema;
} {
  const renameSuggestions = detectPossibleTableRenames(fromSchema, toSchema);
  const renamedOldTables = new Set(renameSuggestions.map((suggestion) => suggestion.oldTableName));
  const renamedNewTables = new Set(renameSuggestions.map((suggestion) => suggestion.newTableName));
  const addedTables = Object.keys(toSchema)
    .filter((tableName) => !fromSchema[tableName])
    .sort();
  const removedTables = Object.keys(fromSchema)
    .filter((tableName) => !toSchema[tableName])
    .sort();
  const explicitAddedTables = addedTables.filter((tableName) => !renamedNewTables.has(tableName));
  const explicitRemovedTables = removedTables.filter(
    (tableName) => !renamedOldTables.has(tableName),
  );
  const changedTables = changedTableNames(fromSchema, toSchema);
  const migratableTables = changedTables.filter(
    (tableName) => fromSchema[tableName] !== undefined && toSchema[tableName] !== undefined,
  );
  const witnessFromTables = [...migratableTables, ...explicitRemovedTables];
  const witnessToTables = [...migratableTables, ...explicitAddedTables];
  for (const renameSuggestion of renameSuggestions) {
    witnessFromTables.push(renameSuggestion.oldTableName);
    witnessToTables.push(renameSuggestion.newTableName);
  }
  const witnessFrom = pickWitnessSchema(fromSchema, witnessFromTables);
  const witnessTo = pickWitnessSchema(toSchema, witnessToTables);
  const lines: string[] = [];

  for (const tableName of migratableTables) {
    const fromTable = fromSchema[tableName]!;
    const toTable = toSchema[tableName]!;

    const suggestion = inferTableSuggestions(tableName, fromTable, toTable);
    lines.push(`${JSON.stringify(tableName)}: {`);
    for (const comment of suggestion.comments) {
      lines.push(`  // TODO: ${comment}`);
    }
    for (const property of suggestion.properties) {
      lines.push(`  ${property}`);
    }
    if (suggestion.comments.length === 0 && suggestion.properties.length === 0) {
      lines.push("  // TODO: No safe migration steps were inferred automatically.");
    }
    lines.push("},");
    lines.push("");
  }

  if (lines.length === 0) {
    if (
      renameSuggestions.length === 0 &&
      explicitAddedTables.length === 0 &&
      explicitRemovedTables.length === 0
    ) {
      lines.push(
        changedTables.length === 0
          ? "// TODO: No schema differences were detected."
          : "// TODO: No column-level migration steps were required for the detected schema changes.",
      );
    }
  }

  return {
    migrateBody: lines.length > 0 ? lines.join("\n").trimEnd() : undefined,
    createTablesBody:
      explicitAddedTables.length > 0
        ? explicitAddedTables.map((tableName) => `${JSON.stringify(tableName)}: true,`).join("\n")
        : undefined,
    dropTablesBody:
      explicitRemovedTables.length > 0
        ? explicitRemovedTables.map((tableName) => `${JSON.stringify(tableName)}: true,`).join("\n")
        : undefined,
    renameTablesBody:
      renameSuggestions.length > 0
        ? renameSuggestions
            .map(
              (renameSuggestion) =>
                `${renameSuggestion.newTableName}: s.renameTableFrom(${JSON.stringify(renameSuggestion.oldTableName)}),`,
            )
            .join("\n")
        : undefined,
    witnessFrom,
    witnessTo,
  };
}

async function packageVersion(): Promise<string> {
  const packageJson = JSON.parse(
    await readFile(new URL("../package.json", import.meta.url), "utf8"),
  ) as { version?: string };
  return packageJson.version ?? "unknown";
}

function createTimestamp(now: Date = new Date()): string {
  const year = now.getUTCFullYear();
  const month = String(now.getUTCMonth() + 1).padStart(2, "0");
  const day = String(now.getUTCDate()).padStart(2, "0");
  const hours = String(now.getUTCHours()).padStart(2, "0");
  const minutes = String(now.getUTCMinutes()).padStart(2, "0");
  const seconds = String(now.getUTCSeconds()).padStart(2, "0");
  return `${year}${month}${day}T${hours}${minutes}${seconds}`;
}

function createSnapshotTimestampFromPublishedAt(
  publishedAt: number | null | undefined,
  fallbackNow: Date = new Date(),
): string {
  if (typeof publishedAt !== "number" || !Number.isFinite(publishedAt) || publishedAt < 0) {
    return createTimestamp(fallbackNow);
  }

  return createTimestamp(new Date(publishedAt));
}

function normalizeMigrationName(name: string): string {
  const normalized = name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

  if (normalized.length === 0) {
    throw new Error(
      "Migration name must contain at least one ASCII letter or digit after normalization.",
    );
  }

  return normalized;
}

function migrationFilename(
  migrationsDir: string,
  fromHash: string,
  toHash: string,
  name: string = "unnamed",
  timestamp: string = createTimestamp(),
): string {
  return join(
    migrationsDir,
    `${timestamp}-${name}-${shortSchemaHash(fromHash)}-${shortSchemaHash(toHash)}.ts`,
  );
}

function snapshotFilename(hash: string, timestamp: string = createTimestamp()): string {
  return `${timestamp}-${shortSchemaHash(hash)}.json`;
}

function renderMigrationStub(input: {
  fromHash: string;
  toHash: string;
  fromSchema: WasmSchema;
  toSchema: WasmSchema;
}): string {
  const rendered = renderMigrationBody(input.fromSchema, input.toSchema);
  const sections: string[] = [];

  if (rendered.renameTablesBody) {
    sections.push(`  renameTables: {\n${indentBlock(rendered.renameTablesBody, 4)}\n  },`);
  }

  if (rendered.createTablesBody) {
    sections.push(`  createTables: {\n${indentBlock(rendered.createTablesBody, 4)}\n  },`);
  }

  if (rendered.dropTablesBody) {
    sections.push(`  dropTables: {\n${indentBlock(rendered.dropTablesBody, 4)}\n  },`);
  }

  if (rendered.migrateBody) {
    sections.push(`  migrate: {\n${indentBlock(rendered.migrateBody, 4)}\n  },`);
  }

  sections.push(`  fromHash: ${JSON.stringify(shortSchemaHash(input.fromHash))},`);
  sections.push(`  toHash: ${JSON.stringify(shortSchemaHash(input.toHash))},`);
  sections.push(`  from: ${renderSchemaWitness(rendered.witnessFrom)},`);
  sections.push(`  to: ${renderSchemaWitness(rendered.witnessTo)},`);

  return `import { schema as s } from "jazz-tools";

export default s.defineMigration({
${sections.join("\n")}
});
`;
}

function isDefinedMigration(value: unknown): value is DefinedMigration {
  if (typeof value !== "object" || value === null) {
    return false;
  }

  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.fromHash === "string" &&
    typeof candidate.toHash === "string" &&
    typeof candidate.from === "object" &&
    candidate.from !== null &&
    typeof candidate.to === "object" &&
    candidate.to !== null &&
    Array.isArray(candidate.forward)
  );
}

async function loadDefinedMigration(filePath: string): Promise<DefinedMigration> {
  const outFile = await bundleToTempFile(filePath);
  try {
    const loaded = (await import(pathToFileURL(outFile).href)) as {
      default?: unknown;
      migration?: unknown;
    };
    const migration = unwrapMigrationExport(loaded.default ?? loaded.migration);
    if (!isDefinedMigration(migration)) {
      throw new Error(
        `Invalid migration export in ${basename(filePath)}. Export default defineMigration(...).`,
      );
    }
    return migration;
  } finally {
    await rm(outFile, { force: true }).catch(() => undefined);
  }
}

function unwrapMigrationExport(value: unknown): unknown {
  let current = value;

  while (
    typeof current === "object" &&
    current !== null &&
    "default" in current &&
    Object.keys(current as Record<string, unknown>).length === 1
  ) {
    current = (current as { default: unknown }).default;
  }

  return current;
}

async function findMigrationFile(
  migrationsDir: string,
  fromHash: string,
  toHash: string,
): Promise<string> {
  if (!(await pathExists(migrationsDir))) {
    throw new Error(`No migration file found in ${migrationsDir} for ${fromHash} -> ${toHash}.`);
  }

  const fromShortHash = shortSchemaHash(fromHash);
  const toShortHash = shortSchemaHash(toHash);
  const files = await readdir(migrationsDir);
  const matches = files
    .filter((file) => file.endsWith(".ts"))
    .filter(
      (file) =>
        file.includes(`-${fromShortHash}-${toShortHash}.ts`) ||
        file.includes(`-${fromHash}-${toHash}.ts`),
    );

  if (matches.length === 0) {
    throw new Error(`No migration file found in ${migrationsDir} for ${fromHash} -> ${toHash}.`);
  }

  if (matches.length > 1) {
    throw new Error(
      `Multiple migration files found for ${fromHash} -> ${toHash}: ${matches.join(", ")}`,
    );
  }

  return join(migrationsDir, matches[0]!);
}

interface ResolvedSchemaInput {
  hash: string;
  schema: WasmSchema;
}

function isCommittedSnapshotFileName(fileName: string): boolean {
  return /^\d{8}T\d{6}-[0-9a-f]{12}\.json$/i.test(fileName);
}

async function loadLatestCommittedSnapshot(
  migrationsDir: string,
): Promise<ResolvedSchemaInput | null> {
  const entries = await listSnapshotEntriesForMigrations(migrationsDir);
  const latest = entries
    .filter((entry) => isCommittedSnapshotFileName(entry.fileName))
    .sort((left, right) => left.fileName.localeCompare(right.fileName))
    .at(-1);
  if (!latest) {
    return null;
  }

  return {
    hash: latest.hash,
    schema: latest.schema,
  };
}

async function ensureCommittedSnapshot(
  migrationsDir: string,
  schema: ResolvedSchemaInput,
  timestamp: string,
): Promise<string | null> {
  const entries = await listSnapshotEntriesForMigrations(migrationsDir);
  if (
    entries.some(
      (entry) => entry.hash === schema.hash && isCommittedSnapshotFileName(entry.fileName),
    )
  ) {
    return null;
  }

  return writeSnapshotSchemaForMigrations(
    migrationsDir,
    snapshotFilename(schema.hash, timestamp),
    schema.schema,
  );
}

async function loadCurrentSchema(schemaDir: string): Promise<ResolvedSchemaInput> {
  const compiled = await loadCompiledSchema(schemaDir);
  return {
    hash: await computeSchemaHash(compiled.wasmSchema),
    schema: compiled.wasmSchema,
  };
}

async function resolveHistoricalSchema(
  migrationsDir: string,
  hash: string,
  label: string,
  appId: string | undefined,
  serverUrl: string | undefined,
  adminSecret: string | undefined,
): Promise<ResolvedSchemaInput> {
  const localEntries = await listSnapshotEntriesForMigrations(migrationsDir);
  const normalized = normalizeSchemaHashInput(hash, label);
  const localFullHash =
    normalized.length === 64
      ? localEntries.find((entry) => entry.hash === normalized)?.hash
      : (() => {
          const matches = localEntries.filter((entry) => entry.hash.startsWith(normalized));
          if (matches.length === 0) {
            return null;
          }
          if (matches.length > 1) {
            throw new Error(
              `${label} prefix ${normalized} is ambiguous: ${matches
                .map((entry) => shortSchemaHash(entry.hash))
                .join(", ")}`,
            );
          }
          return matches[0]!.hash;
        })();

  if (localFullHash) {
    return {
      hash: localFullHash,
      schema: localEntries.find((entry) => entry.hash === localFullHash)!.schema,
    };
  }

  const resolvedHash =
    normalized.length === 64
      ? normalized
      : resolveKnownSchemaHash(
          normalized,
          label,
          (
            await fetchSchemaHashes(requireSchemaExportServerValue(serverUrl, "serverUrl"), {
              appId: requireAppId(appId),
              adminSecret: requireSchemaExportServerValue(adminSecret, "adminSecret"),
            })
          ).hashes,
        );

  const resolvedAppId = requireAppId(appId);
  const resolvedServerUrl = requireSchemaExportServerValue(serverUrl, "serverUrl");
  const resolvedAdminSecret = requireSchemaExportServerValue(adminSecret, "adminSecret");

  try {
    const storedSchema = await fetchStoredWasmSchema(resolvedServerUrl, {
      appId: resolvedAppId,
      adminSecret: resolvedAdminSecret,
      schemaHash: resolvedHash,
    });
    await writeSnapshotSchemaForMigrations(
      migrationsDir,
      snapshotFilename(
        resolvedHash,
        createSnapshotTimestampFromPublishedAt(storedSchema.publishedAt),
      ),
      storedSchema.schema,
    );
    return { hash: resolvedHash, schema: storedSchema.schema };
  } catch (error) {
    if (error instanceof Error && /Schema fetch failed: 404/i.test(error.message)) {
      throw new Error(`No stored schema found for ${label} ${resolvedHash}.`);
    }
    throw error;
  }
}

export async function createMigration(options: CreateMigrationOptions): Promise<string | null> {
  const explicitHashFlow = Boolean(options.fromHash || options.toHash);

  await mkdir(options.migrationsDir, { recursive: true });
  const currentSchema =
    !explicitHashFlow || !options.toHash ? await loadCurrentSchema(options.schemaDir) : null;

  let fromSchema: ResolvedSchemaInput;
  let toSchema: ResolvedSchemaInput;
  let shouldWriteCommittedSnapshot = false;
  const timestamp = createTimestamp();

  if (explicitHashFlow) {
    if (options.fromHash) {
      fromSchema = await resolveHistoricalSchema(
        options.migrationsDir,
        options.fromHash,
        "fromHash",
        options.appId,
        options.serverUrl,
        options.adminSecret,
      );
    } else {
      const latest = await loadLatestCommittedSnapshot(options.migrationsDir);
      if (!latest) {
        throw new Error(
          "No committed snapshot found. Provide --fromHash or run `jazz-tools migrations create` once to create an initial snapshot.",
        );
      }
      fromSchema = latest;
    }

    toSchema = options.toHash
      ? await resolveHistoricalSchema(
          options.migrationsDir,
          options.toHash,
          "toHash",
          options.appId,
          options.serverUrl,
          options.adminSecret,
        )
      : currentSchema!;
    shouldWriteCommittedSnapshot = !options.toHash;
  } else {
    const latest = await loadLatestCommittedSnapshot(options.migrationsDir);
    if (!latest) {
      const snapshotPath = await ensureCommittedSnapshot(
        options.migrationsDir,
        currentSchema!,
        timestamp,
      );
      console.log(`Wrote initial schema snapshot: ${snapshotPath}`);
      console.log("No migration created because there was no previous local schema baseline.");
      return null;
    }

    if (latest.hash === currentSchema!.hash) {
      console.log("No structural schema changes detected.");
      return null;
    }

    fromSchema = latest;
    toSchema = currentSchema!;
    shouldWriteCommittedSnapshot = true;
  }

  if (fromSchema.hash === toSchema.hash) {
    console.log("No structural schema changes detected.");
    return null;
  }

  if (!schemaTransitionRequiresRowTransform(fromSchema.schema, toSchema.schema)) {
    if (shouldWriteCommittedSnapshot) {
      await ensureCommittedSnapshot(options.migrationsDir, toSchema, timestamp);
    }

    const version = await packageVersion();
    console.log(
      "No reviewed migration file needed because this schema change does not require row transformations.",
    );
    console.log(
      `Next step: Run npx jazz-tools@${version} migrations push ${options.appId ?? "<appId>"} ${shortSchemaHash(fromSchema.hash)} ${shortSchemaHash(toSchema.hash)}`,
    );
    return null;
  }

  const filePath = migrationFilename(
    options.migrationsDir,
    fromSchema.hash,
    toSchema.hash,
    options.name ? normalizeMigrationName(options.name) : undefined,
    timestamp,
  );
  if (await pathExists(filePath)) {
    throw new Error(`Migration stub already exists: ${filePath}`);
  }

  const stub = renderMigrationStub({
    fromHash: fromSchema.hash,
    toHash: toSchema.hash,
    fromSchema: fromSchema.schema,
    toSchema: toSchema.schema,
  });
  await writeFile(filePath, stub);

  if (shouldWriteCommittedSnapshot) {
    await ensureCommittedSnapshot(options.migrationsDir, toSchema, timestamp);
  }

  const version = await packageVersion();
  console.log(`Generated: ${filePath}`);
  console.log("");
  console.log("Migration stubs are only for structural schema changes.");
  console.log(PERMISSIONS_LIFECYCLE_NOTE);
  console.log("");
  console.log("Next steps:");
  console.log("1. Fill in migrate.");
  if (!options.name) {
    console.log("2. Rename the file by replacing 'unnamed'.");
  }
  console.log(
    `${options.name ? "2" : "3"}. Run npx jazz-tools@${version} migrations push ${options.appId ?? "<appId>"} ${shortSchemaHash(fromSchema.hash)} ${shortSchemaHash(toSchema.hash)}`,
  );

  return filePath;
}

export async function pushMigration(options: PushMigrationOptions): Promise<void> {
  const { appId, serverUrl, adminSecret } = requireMigrationServerOptions(options);
  const { hashes } = await fetchSchemaHashes(serverUrl, {
    appId,
    adminSecret,
  });
  const fromHash = resolveKnownSchemaHash(options.fromHash, "fromHash", hashes);
  const toHash = resolveKnownSchemaHash(options.toHash, "toHash", hashes);
  let filePath: string | null = null;

  try {
    filePath = await findMigrationFile(options.migrationsDir, fromHash, toHash);
  } catch (error) {
    if (
      !(error instanceof Error) ||
      !error.message.startsWith(`No migration file found in ${options.migrationsDir}`)
    ) {
      throw error;
    }
  }

  if (!filePath) {
    const fromSchema = await resolveHistoricalSchema(
      options.migrationsDir,
      fromHash,
      "fromHash",
      appId,
      serverUrl,
      adminSecret,
    );
    const toSchema = await resolveHistoricalSchema(
      options.migrationsDir,
      toHash,
      "toHash",
      appId,
      serverUrl,
      adminSecret,
    );

    if (schemaTransitionRequiresRowTransform(fromSchema.schema, toSchema.schema)) {
      throw new Error(
        `No migration file found in ${options.migrationsDir} for ${fromHash} -> ${toHash}. Run \`jazz-tools migrations create ${appId} --fromHash ${shortSchemaHash(fromHash)} --toHash ${shortSchemaHash(toHash)}\` first.`,
      );
    }

    await publishStoredMigration(serverUrl, {
      appId,
      adminSecret,
      fromHash,
      toHash,
      forward: [],
    });

    console.log(
      `Pushed migration ${shortSchemaHash(fromHash)} -> ${shortSchemaHash(toHash)} without a reviewed migration file because no row transformations are required.`,
    );
    return;
  }

  const migration = await loadDefinedMigration(filePath);

  if (
    !hashMatchesFullSchema(migration.fromHash, fromHash) ||
    !hashMatchesFullSchema(migration.toHash, toHash)
  ) {
    throw new Error(
      `Migration ${basename(filePath)} exports ${migration.fromHash} -> ${migration.toHash}, expected ${shortSchemaHash(fromHash)} -> ${shortSchemaHash(toHash)}.`,
    );
  }

  schemaDefinitionToAst(migration.from as any);
  schemaDefinitionToAst(migration.to as any);

  if (migration.forward.length === 0) {
    const fromSchema = await resolveHistoricalSchema(
      options.migrationsDir,
      fromHash,
      "fromHash",
      appId,
      serverUrl,
      adminSecret,
    );
    const toSchema = await resolveHistoricalSchema(
      options.migrationsDir,
      toHash,
      "toHash",
      appId,
      serverUrl,
      adminSecret,
    );

    if (schemaTransitionRequiresRowTransform(fromSchema.schema, toSchema.schema)) {
      throw new Error(`Migration ${basename(filePath)} has no steps. Fill in migrate before push.`);
    }
  }

  const forward = migration.forward.length === 0 ? [] : serializeForwardLenses(migration.forward);
  await publishStoredMigration(serverUrl, {
    appId,
    adminSecret,
    fromHash,
    toHash,
    forward,
  });

  console.log(
    `Pushed migration ${shortSchemaHash(fromHash)} -> ${shortSchemaHash(toHash)} from ${basename(filePath)}.`,
  );
}

function describePermissionsHead(head: StoredPermissionsHead): string {
  return `v${head.version} on ${shortSchemaHash(head.schemaHash)}`;
}

export async function permissionsStatus(options: PermissionsCommandOptions): Promise<void> {
  const compiled = ensurePermissionsProject(await loadCompiledSchema(options.schemaDir));
  const localSchemaHash = await resolveStoredStructuralSchemaHashOrThrow(
    options.appId,
    options.serverUrl,
    options.adminSecret,
    compiled.wasmSchema,
  );
  const { head } = await fetchPermissionsHead(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
  });

  console.log(`Loaded structural schema from ${compiled.schemaFile}.`);
  console.log(`Loaded current permissions from ${compiled.permissionsFile}.`);
  console.log(`Local structural schema matches stored hash ${shortSchemaHash(localSchemaHash)}.`);
  console.log(PERMISSIONS_LIFECYCLE_NOTE);

  if (!head) {
    console.log("Server has no published permissions head yet.");
    console.log("Next push will publish version 1.");
    return;
  }

  console.log(`Server permissions head is ${describePermissionsHead(head)}.`);
  if (head.schemaHash === localSchemaHash) {
    console.log("Current server permissions already target this structural schema.");
  } else {
    console.log(
      `Current server permissions target ${shortSchemaHash(head.schemaHash)}; pushing will retarget the head to ${shortSchemaHash(localSchemaHash)}.`,
    );
  }
  console.log(`Next push will require parent bundle ${head.bundleObjectId}.`);
}

export async function deploy(options: DeployOptions): Promise<void> {
  const compiled = await loadCompiledSchema(options.schemaDir);
  console.log(`Loaded current schema from ${compiled.schemaFile}.`);

  for (const diagnostic of collectMissingExplicitPolicyDiagnostics(
    compiled.schema.tables.map((table) => table.name),
    compiled.permissions,
  )) {
    console.warn(`\x1b[33m${diagnostic.message}\x1b[0m`);
  }

  let localSchemaHash = await resolveStoredStructuralSchemaHash(
    options.appId,
    options.serverUrl,
    options.adminSecret,
    compiled.wasmSchema,
  );

  if (!localSchemaHash) {
    const publishedSchema = await publishStoredSchema(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
      schema: compiled.wasmSchema,
    });
    localSchemaHash = publishedSchema.hash;
    console.log(`Published the current schema as ${shortSchemaHash(localSchemaHash)}.`);
  } else {
    console.log(
      `The current schema is already stored in the server as ${shortSchemaHash(localSchemaHash)}; skipping publish.`,
    );
  }

  if (!compiled.permissions || !compiled.permissionsFile) {
    console.log("No permissions.ts found; skipping permissions publish.");
    return;
  }

  console.log(`Loaded current permissions from ${compiled.permissionsFile}.`);

  const { head: currentHead } = await fetchPermissionsHead(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
  });
  if (currentHead && currentHead.schemaHash !== localSchemaHash) {
    const fromShortHash = shortSchemaHash(currentHead.schemaHash);
    const toShortHash = shortSchemaHash(localSchemaHash);

    try {
      const { connected } = await fetchSchemaConnectivity(options.serverUrl, {
        appId: options.appId,
        adminSecret: options.adminSecret,
        fromHash: currentHead.schemaHash,
        toHash: localSchemaHash,
      });

      if (!connected) {
        await pushMigration({
          appId: options.appId,
          serverUrl: options.serverUrl,
          adminSecret: options.adminSecret,
          migrationsDir: options.migrationsDir,
          fromHash: currentHead.schemaHash,
          toHash: localSchemaHash,
        });
      }
    } catch (error) {
      const migrationMissingPrefix = `No migration file found in ${options.migrationsDir}`;
      if (!(error instanceof Error) || !error.message.startsWith(migrationMissingPrefix)) {
        throw error;
      }

      const message = `The new permissions schema ${toShortHash} is not connected to the previous permissions schema ${fromShortHash} on the server. Reads and writes may fail until you push a migration. Run \`jazz-tools migrations create ${options.appId} --fromHash ${fromShortHash} --toHash ${toShortHash}\` to create a migration and then re-run this command.`;
      if (options.noVerify) {
        console.warn(`Warning: ${message}`);
      } else {
        throw Error(message);
      }
    }
  }

  const { head: publishedHead } = await publishStoredPermissions(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schemaHash: localSchemaHash,
    permissions: compiled.permissions,
    expectedParentBundleObjectId: currentHead?.bundleObjectId ?? null,
  });
  const nextHead = publishedHead ?? {
    schemaHash: localSchemaHash,
    version: currentHead ? currentHead.version + 1 : 1,
    parentBundleObjectId: currentHead?.bundleObjectId ?? null,
    bundleObjectId: currentHead?.bundleObjectId ?? "",
  };

  console.log(`Published permissions as ${describePermissionsHead(nextHead)}.`);
}

function isMainModule(): boolean {
  const entry = process.argv[1];
  if (!entry) {
    return false;
  }
  return pathToFileURL(entry).href === import.meta.url;
}

if (isMainModule()) {
  const command = process.argv[2] ?? "";

  if (command === "validate") {
    const { options } = parseArgs();
    validate(options).catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
  } else if (command === "schema") {
    const subcommand = process.argv[3] ?? "";
    if (subcommand === "hash") {
      const args = process.argv.slice(4);
      const schemaDirFlag = getFlagValue(args, "--schema-dir");
      const schemaDir = resolve(process.cwd(), schemaDirFlag ?? process.cwd());
      schemaHash({ schemaDir }).catch((err) => {
        console.error(err.message);
        process.exit(1);
      });
    } else if (subcommand === "export") {
      const { appId, args } = splitLeadingAppId(process.argv.slice(4));
      const schemaDirFlag = getFlagValue(args, "--schema-dir");
      const schemaHashFlag = getFlagValue(args, "--schema-hash");
      if (schemaDirFlag && schemaHashFlag) {
        console.error("--schema-dir and --schema-hash are mutually exclusive.");
        process.exit(1);
      }

      const schemaDir = resolve(process.cwd(), schemaDirFlag ?? process.cwd());
      exportSchema({
        schemaDir,
        migrationsDir: getFlagValue(args, "--migrations-dir")
          ? resolve(process.cwd(), getFlagValue(args, "--migrations-dir")!)
          : undefined,
        schemaHash: schemaHashFlag,
        appId,
        serverUrl: getFlagValue(args, "--server-url") ?? resolveEnvVar(SERVER_URL_ENV_VARS),
        adminSecret: getFlagValue(args, "--admin-secret") ?? process.env.JAZZ_ADMIN_SECRET,
      }).catch((err) => {
        console.error(err.message);
        process.exit(1);
      });
    } else {
      console.error("Usage: node dist/cli.js schema <hash|export> [--schema-dir <path>] [...]");
      process.exit(1);
    }
  } else if (command === "migrations") {
    const subcommand = process.argv[3] ?? "";
    let task: Promise<unknown>;

    if (subcommand === "create") {
      const { appId, args } = splitLeadingAppId(process.argv.slice(4));
      const options = resolveMigrationOptions(args);
      task = createMigration({
        ...options,
        appId,
        schemaDir: options.schemaDir ?? process.cwd(),
        fromHash: getFlagValue(args, "--fromHash"),
        toHash: getFlagValue(args, "--toHash"),
        name: getFlagValue(args, "--name"),
      });
    } else if (subcommand === "push") {
      const appId = process.argv[4];
      const fromHash = process.argv[5];
      const toHash = process.argv[6];
      const sharedArgs = process.argv.slice(7);

      if (!appId || !fromHash || !toHash) {
        console.error(
          "Usage: node dist/cli.js migrations push <appId> <fromHash> <toHash> [options]",
        );
        process.exit(1);
      }

      const options = resolveMigrationOptions(sharedArgs);
      task = pushMigration({ ...options, appId, fromHash, toHash });
    } else {
      task = Promise.reject(
        new Error("Usage: node dist/cli.js migrations <create|push> [<appId>] [options]"),
      );
    }

    task.catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
  } else if (command === "permissions") {
    const subcommand = process.argv[3] ?? "";
    const { appId, args } = splitLeadingAppId(process.argv.slice(4));
    const options = { ...resolvePermissionsOptions(args), appId: requireAppId(appId) };
    const task =
      subcommand === "status"
        ? permissionsStatus(options)
        : Promise.reject(new Error("Usage: node dist/cli.js permissions status <appId> [options]"));

    task.catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
  } else if (command === "deploy") {
    const { appId, args } = splitLeadingAppId(process.argv.slice(3));
    const options = { ...resolveMigrationOptions(args), appId };
    deploy({
      ...requireMigrationServerOptions(options),
      schemaDir: options.schemaDir ?? process.cwd(),
      migrationsDir: options.migrationsDir,
      noVerify: hasFlag(args, "--no-verify"),
    }).catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
  } else {
    console.log("Usage: node <path-to-jazz-tools>/dist/cli.js <command> [options]");
    console.log("\nCommands:");
    console.log("  validate              Validate root schema.ts and optional permissions.ts");
    console.log("  schema hash           Print the short hash of the current schema.ts");
    console.log("  schema export         Print the compiled structural schema as JSON");
    console.log("  deploy <appId>        Publish the current schema.ts and permissions.ts");
    console.log(
      "  permissions status <appId> Show the current server permissions head for this app",
    );
    console.log(
      "  migrations create     Generate a typed structural migration stub between two schema versions",
    );
    console.log(
      "  migrations push <appId> <fromHash> <toHash> Push a reviewed migration edge to the server",
    );
    console.log("\nValidation options:");
    console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
    console.log("\nSchema hash options:");
    console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
    console.log("\nSchema export options:");
    console.log(
      "  <appId>               Required for server-backed schema export by hash (or set JAZZ_APP_ID / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_APP_ID)",
    );
    console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
    console.log("  --schema-hash <hash>  Export a stored structural schema by hash");
    console.log("  --migrations-dir <p>  Path to migrations directory (default: ./migrations)");
    console.log(
      "  --server-url <url>    Jazz server URL (or set JAZZ_SERVER_URL / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_SERVER_URL)",
    );
    console.log("  --admin-secret <sec>  Admin secret (or set JAZZ_ADMIN_SECRET)");
    console.log("\nPermissions options:");
    console.log(
      "  <appId>               Required (or set JAZZ_APP_ID / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_APP_ID)",
    );
    console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
    console.log(
      "  --server-url <url>    Jazz server URL (or set JAZZ_SERVER_URL / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_SERVER_URL)",
    );
    console.log("  --admin-secret <sec>  Admin secret (or set JAZZ_ADMIN_SECRET)");
    console.log("\nMigration options:");
    console.log(
      "  <appId>               Required for remote create/push commands (or set JAZZ_APP_ID / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_APP_ID)",
    );
    console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
    console.log(
      "  --server-url <url>    Jazz server URL (or set JAZZ_SERVER_URL / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_SERVER_URL)",
    );
    console.log("  --admin-secret <sec>  Admin secret (or set JAZZ_ADMIN_SECRET)");
    console.log("  --migrations-dir <p>  Path to migrations directory (default: ./migrations)");
    console.log(
      "  --fromHash <hash>     Optional source schema hash (defaults to latest snapshot)",
    );
    console.log("  --toHash <hash>       Optional target schema hash (defaults to current schema)");
    console.log("  --name <name>         Optional migration filename label (default: unnamed)");
    process.exit(command ? 1 : 0);
  }
}

/**
 * Translate public QueryBuilder JSON into the runtime query payload.
 *
 * QueryBuilder produces a compact JSON structure:
 * { table, conditions, includes, orderBy, limit, offset, hops?, gather? }
 *
 * Most queries are emitted as the flat runtime Query payload. Native relation IR
 * is reserved for relation traversal shapes that cannot use the flat path.
 */

import type { ColumnType, WasmSchema } from "../drivers/types.js";
import { canonicalAuthorSubject } from "./author-id.js";
import { stripColumnQualifier as stripQualifier } from "./query-column-name.js";
import { toJsonText } from "./json-text.js";
import { analyzeRelations, type Relation } from "../codegen/relation-analyzer.js";
import { magicColumnType } from "../magic-columns.js";
import {
  normalizeBuiltQuery,
  type BuiltCondition,
  type BuiltGather,
  type BuiltRelation,
  type LargeValueSelectDescriptor,
  type NormalizedIncludeEntry,
  type NormalizedIncludeSpec,
} from "./query-builder-shape.js";
import { hiddenIncludeColumnName, resolveSelectedColumns } from "./select-projection.js";
import type {
  RelColumnRef,
  RelExpr,
  RelJoinCondition,
  RelPredicateExpr,
  RelProjectColumn,
} from "../ir.js";

function relColumn(column: string, scope?: string): RelColumnRef {
  return scope ? { scope, column } : { column };
}

function relationColumnsForTable(
  table: string,
  scope: string,
  schema: WasmSchema,
): RelProjectColumn[] {
  const tableSchema = schema[table];
  if (!tableSchema) {
    throw new Error(`Unknown table "${table}" in relation projection.`);
  }
  return [
    {
      alias: "id",
      expr: { Column: relColumn("id", scope) },
    },
    ...tableSchema.columns.map((column) => ({
      alias: column.name,
      expr: { Column: relColumn(column.name, scope) } as const,
    })),
  ];
}

function getColumnType(schema: WasmSchema, table: string, column: string): ColumnType | undefined {
  // All tables have an implicit UUID primary key `id`.
  if (column === "id") return { type: "Uuid" };
  const magicType = magicColumnType(column);
  if (magicType) return magicType;
  const tableSchema = schema[table];
  if (!tableSchema) return undefined;
  const col = tableSchema.columns.find((c) => c.name === column);
  return col?.column_type;
}

function toTimestampMs(value: unknown): number {
  if (value instanceof Date) {
    const ts = value.getTime();
    if (!Number.isFinite(ts)) {
      throw new Error("Invalid Date value for timestamp condition");
    }
    return ts;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error("Invalid number value for timestamp condition");
    }
    return value;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
      const fromNumber = Number(trimmed);
      if (Number.isFinite(fromNumber)) {
        return fromNumber;
      }
    }
    const fromIso = Date.parse(trimmed);
    if (Number.isFinite(fromIso)) {
      return fromIso;
    }
  }
  throw new Error("Invalid timestamp condition. Expected Date, ISO string, or finite number.");
}

function toRuntimeTimestampValue(value: unknown): number {
  // Relation IR is evaluated by NAPI/WASM directly against core CurrentRows.
  // Both ordinary and provenance timestamps are Unix milliseconds there and
  // at every public result boundary.
  return toTimestampMs(value);
}

/**
 * Translate a JavaScript value to the runtime value format.
 */
function toRuntimeValue(value: unknown, columnType: ColumnType): object {
  if (value === null || value === undefined) {
    return { type: "Null" };
  }
  if (columnType.type === "Json") {
    return { type: "Text", value: toJsonText(value) };
  }
  if (columnType.type === "Timestamp" && value instanceof Date) {
    return { type: "Timestamp", value: toRuntimeTimestampValue(value) };
  }
  if (columnType.type === "Bytea") {
    if (value instanceof Uint8Array) {
      return { type: "Bytea", value: [...value] };
    }
    if (Array.isArray(value)) {
      const bytes = value.map((entry) => {
        const n = Number(entry);
        if (!Number.isInteger(n) || n < 0 || n > 255) {
          throw new Error("Bytea values must contain integers in range 0..255");
        }
        return n;
      });
      return { type: "Bytea", value: bytes };
    }
    throw new Error("Bytea values must be Uint8Array or byte arrays");
  }
  if (Array.isArray(value)) {
    if (columnType.type !== "Array") {
      throw new Error("Unexpected array value for scalar column");
    }
    return {
      type: "Array",
      value: value.map((item) => toRuntimeValue(item, columnType.element)),
    };
  }
  if (typeof value === "boolean") {
    return { type: "Boolean", value };
  }
  if (typeof value === "number") {
    if (columnType?.type === "Timestamp") {
      return { type: "Timestamp", value: toRuntimeTimestampValue(value) };
    }
    if (columnType.type === "BigInt") {
      if (!Number.isSafeInteger(value)) {
        throw new Error("BIGINT query values must be bigint or safe integer numbers");
      }
      return { type: "BigInt", value: BigInt(value) };
    }
    if (columnType.type === "Double" || columnType.type === "Integer") {
      return { type: columnType.type, value };
    }
  }
  if (typeof value === "bigint") {
    if (columnType.type !== "BigInt") {
      throw new Error("Unexpected bigint value for non-BIGINT column");
    }
    return { type: "BigInt", value };
  }
  if (typeof value === "string") {
    if (columnType?.type === "BigInt") {
      try {
        return { type: "BigInt", value: BigInt(value) };
      } catch {
        throw new Error("BIGINT query values must be signed integer strings");
      }
    }
    if (columnType?.type === "Timestamp") {
      return { type: "Timestamp", value: toRuntimeTimestampValue(value) };
    }
    if (columnType?.type === "Uuid") {
      return { type: "Uuid", value };
    }
    if (columnType?.type === "Enum" && !columnType.variants.includes(value)) {
      throw new Error(
        `Invalid enum value "${value}". Expected one of: ${columnType.variants.join(", ")}`,
      );
    }
    return { type: "Text", value };
  }
  throw new Error(`Unsupported value type: ${typeof value}`);
}

function includeRequirementForRelation(
  relation: Relation,
  requireIncludes: boolean,
): "AtLeastOne" | "MatchCorrelationCardinality" | undefined {
  if (!requireIncludes || relation.type !== "forward" || relation.nullable) {
    return undefined;
  }

  return relation.isArray ? "MatchCorrelationCardinality" : "AtLeastOne";
}

type WireSelectProjection =
  | { kind: "full"; column: string }
  | { kind: "bytes"; column: string; from: number; to: number }
  | { kind: "text_utf16"; column: string; from: number; to: number }
  | { kind: "text_utf8"; column: string; from: number; to: number }
  | { kind: "json_pointer"; column: string; at: string };

function partialProjection(
  column: string,
  descriptor: LargeValueSelectDescriptor,
  schema: WasmSchema,
  table: string,
): WireSelectProjection {
  const columnType = getColumnType(schema, table, column);
  if (!columnType) throw new Error(`Unknown column "${column}" in partial select.`);
  if ("at" in descriptor) {
    if (columnType.type !== "Json") {
      throw new Error(`JSON pointer selection requires a JSON column, got "${column}".`);
    }
    return { kind: "json_pointer", column, at: descriptor.at };
  }
  if ("fromUtf8" in descriptor) {
    if (descriptor.toUtf8 < descriptor.fromUtf8) {
      throw new Error(`Invalid UTF-8 range for column "${column}": toUtf8 must be >= fromUtf8.`);
    }
    if (columnType.type !== "Text") {
      throw new Error(`UTF-8 text selection requires a Text column, got "${column}".`);
    }
    return { kind: "text_utf8", column, from: descriptor.fromUtf8, to: descriptor.toUtf8 };
  }
  if (descriptor.to < descriptor.from) {
    throw new Error(`Invalid range for column "${column}": to must be >= from.`);
  }
  if (columnType.type === "Bytea") {
    return { kind: "bytes", column, from: descriptor.from, to: descriptor.to };
  }
  if (columnType.type === "Text") {
    return { kind: "text_utf16", column, from: descriptor.from, to: descriptor.to };
  }
  throw new Error(`Range selection requires a bytes or Text column, got "${column}".`);
}

function visibleSelectColumns(
  resolvedSelect: readonly string[],
  partialSelect: Record<string, LargeValueSelectDescriptor> = {},
  schema?: WasmSchema,
  table?: string,
): WireSelectProjection[] | null {
  // Object-form `select` already supplies a typed projection descriptor for
  // each selected column. Do not also emit a whole-column projection for the
  // same carrier: apart from duplicating the request, the native collector
  // would be left to choose which occurrence wins.
  const full = resolvedSelect
    .filter((column) => !Object.hasOwn(partialSelect, column))
    .map((column) => ({ kind: "full" as const, column }));
  const partial =
    schema && table
      ? Object.entries(partialSelect).map(([column, descriptor]) =>
          partialProjection(column, descriptor, schema, table),
        )
      : [];
  return full.length + partial.length > 0 ? [...full, ...partial] : null;
}

function visibleFullSelectColumns(resolvedSelect: readonly string[]): string[] | null {
  return resolvedSelect.length > 0 ? [...resolvedSelect] : null;
}

function validateIncludeBuilderSpec(
  relation: Relation,
  spec: NormalizedIncludeEntry,
  relationName: string,
): void {
  if (spec.table && spec.table !== relation.toTable) {
    throw new Error(
      `Include builder for relation "${relationName}" must target table "${relation.toTable}", got "${spec.table}".`,
    );
  }
  if (typeof spec.offset === "number" && spec.offset !== 0) {
    throw new Error(`Include builder for relation "${relationName}" does not support offset().`);
  }
  if (spec.hops.length > 0) {
    throw new Error(`Include builder for relation "${relationName}" does not support hopTo(...).`);
  }
  if (spec.gather) {
    throw new Error(`Include builder for relation "${relationName}" does not support gather(...).`);
  }
  if (Object.keys(spec.partialSelect).length > 0) {
    throw new Error(
      `Include builder for relation "${relationName}" does not support partial large-value selections.`,
    );
  }
}

function toArraySubqueries(
  includes: NormalizedIncludeSpec,
  tableName: string,
  relations: Map<string, Relation[]>,
  schema: WasmSchema,
  options?: { requireIncludes?: boolean },
): object[] {
  const tableRels = relations.get(tableName) || [];
  const subqueries: object[] = [];
  const requireCurrentLevelIncludes = options?.requireIncludes === true;

  for (const [relName, spec] of Object.entries(includes)) {
    const rel = tableRels.find((r) => r.name === relName);
    if (!rel) {
      throw new Error(`Unknown relation "${relName}" on table "${tableName}"`);
    }
    validateIncludeBuilderSpec(rel, spec, relName);

    const hasExplicitSelect = spec.select.length > 0;
    const resolvedSelectColumns = hasExplicitSelect
      ? resolveSelectedColumns(rel.toTable, schema, spec.select)
      : [];
    // Root and included filters use the same public predicate IR. The native
    // codec translates that IR once into the core Predicate representation.
    const filters = spec.conditions.map((condition) =>
      conditionToRelPredicate(condition, schema, rel.toTable),
    );
    const orderBy = spec.orderBy.map(([column, direction]) => [
      stripQualifier(column),
      direction === "desc" ? "Descending" : "Ascending",
    ]);
    const nestedArrays = toArraySubqueries(spec.includes, rel.toTable, relations, schema, {
      requireIncludes: spec.requireIncludes,
    });
    const selectColumns = visibleFullSelectColumns(resolvedSelectColumns);
    const outputColumnName = schema[tableName]?.columns.some((column) => column.name === relName)
      ? hiddenIncludeColumnName(relName)
      : relName;

    // Build the subquery based on relation type
    if (rel.type === "forward") {
      // Forward relation: todos.owner_id -> users.id
      // We join from the FK column to the target table's id
      const requirement = includeRequirementForRelation(rel, requireCurrentLevelIncludes);
      subqueries.push({
        column_name: outputColumnName,
        table: rel.toTable,
        inner_column: "id",
        outer_column: `${tableName}.${rel.fromColumn}`,
        filters,
        joins: [],
        select_columns: selectColumns,
        order_by: orderBy,
        limit: spec.limit ?? null,
        ...(requirement ? { requirement } : {}),
        nested_arrays: nestedArrays,
      });
    } else {
      // Reverse relation: users -> todos via todos.owner_id
      // We join from the target table's FK column to our id
      subqueries.push({
        column_name: outputColumnName,
        table: rel.toTable,
        inner_column: rel.toColumn,
        outer_column: `${tableName}.id`,
        filters,
        joins: [],
        select_columns: selectColumns,
        order_by: orderBy,
        limit: spec.limit ?? null,
        nested_arrays: nestedArrays,
      });
    }
  }

  return subqueries;
}

function conditionToRelPredicate(
  cond: { column: string; op: string; value: unknown },
  schema: WasmSchema,
  table: string,
  scope?: string,
): RelPredicateExpr {
  const columnRef = relColumn(stripQualifier(cond.column), scope);
  const column = stripQualifier(cond.column);
  const columnType = getColumnType(schema, table, column);
  if (!columnType) {
    throw new Error(`Unknown column "${column}" in table "${table}"`);
  }
  if (columnType.type === "Row" && magicColumnType(column)) {
    if (cond.op !== "eq" && cond.op !== "ne") {
      throw new Error(`Structured author column "${column}" only supports eq/ne operators.`);
    }
    const object = (value: unknown, keys: string[]): Record<string, unknown> => {
      if (
        value === null ||
        typeof value !== "object" ||
        Array.isArray(value) ||
        Object.keys(value).length !== keys.length ||
        keys.some((key) => !Object.hasOwn(value, key))
      ) {
        throw new Error(`Invalid structured author condition for "${column}".`);
      }
      return value as Record<string, unknown>;
    };
    const wholeAuthor = column === "$createdBy" || column === "$updatedBy";
    const author = wholeAuthor ? object(cond.value, ["account", "identity"]) : undefined;
    const identity = object(author ? author.identity : cond.value, ["issuer", "subject"]);
    if (
      typeof identity.issuer !== "string" ||
      typeof identity.subject !== "string" ||
      (author && typeof author.account !== "string")
    ) {
      throw new Error(`Invalid structured author condition for "${column}".`);
    }
    canonicalAuthorSubject(
      identity.issuer,
      identity.subject,
      author?.account as string | undefined,
    );
    const fields = [
      ...(author ? [{ column: `${column}.account`, op: "eq", value: author.account }] : []),
      {
        column: `${column}${wholeAuthor ? ".identity" : ""}.issuer`,
        op: "eq",
        value: identity.issuer,
      },
      {
        column: `${column}${wholeAuthor ? ".identity" : ""}.subject`,
        op: "eq",
        value: identity.subject,
      },
    ];
    const equality: RelPredicateExpr = {
      And: fields.map((field) => conditionToRelPredicate(field, schema, table, scope)),
    };
    return cond.op === "eq" ? equality : { Not: equality };
  }
  if (cond.op === "match") {
    if (columnType.type !== "EnumPayload") {
      throw new Error(`match is only supported on payload enum column "${column}".`);
    }
    if (typeof cond.value !== "object" || cond.value === null || Array.isArray(cond.value)) {
      throw new Error('"match" requires { type, where } input.');
    }
    const match = cond.value as { type?: unknown; where?: unknown };
    if (typeof match.type !== "string") throw new Error('"match.type" must be a string.');
    const entry = columnType.cases.find((candidate) => candidate.name === match.type);
    if (!entry) throw new Error(`unknown payload enum case "${match.type}".`);
    if (
      match.where !== undefined &&
      (typeof match.where !== "object" || match.where === null || Array.isArray(match.where))
    ) {
      throw new Error('"match.where" must be an object.');
    }
    const filters = Object.entries((match.where ?? {}) as Record<string, unknown>).map(
      ([field, value]) => {
        const descriptor = entry.fields.find((candidate) => candidate.name === field);
        if (!descriptor)
          throw new Error(`unknown payload enum field "${field}" for case "${match.type}".`);
        return {
          Cmp: {
            left: relColumn(field),
            op: "Eq" as const,
            right: { Literal: toRuntimeValue(value, descriptor.column_type) },
          },
        } satisfies RelPredicateExpr;
      },
    );
    return {
      EnumMatch: {
        column: columnRef,
        case: match.type,
        payload:
          filters.length === 0 ? "True" : filters.length === 1 ? filters[0]! : { And: filters },
      },
    };
  }
  if (cond.op === "in" || cond.op === "notIn") {
    if (!Array.isArray(cond.value)) {
      throw new Error(`"${cond.op}" operator requires an array value.`);
    }
    if (cond.value.some((value) => value === null)) {
      throw new Error(
        `"${cond.op}" does not accept null membership values; use isNull or isNotNull separately.`,
      );
    }
    const membership: RelPredicateExpr = {
      In: {
        left: columnRef,
        values: cond.value.map((value) => ({
          Literal: toRuntimeValue(value, columnType),
        })),
      },
    };
    return cond.op === "notIn" ? { Not: membership } : membership;
  }
  const valueTypeForCondition =
    cond.op === "contains" && columnType.type === "Array" ? columnType.element : columnType;
  const rightLiteral =
    isFrontierRowIdToken(cond.value) && cond.op === "eq"
      ? { RowId: "Frontier" as const }
      : {
          Literal: toRuntimeValue(cond.value, valueTypeForCondition),
        };
  const isNullValue = cond.value === undefined ? true : cond.value;
  if (columnType.type === "Bytea" && ["gt", "gte", "lt", "lte"].includes(cond.op)) {
    throw new Error(`BYTEA column "${column}" only supports eq/ne operators.`);
  }
  if (columnType.type === "Bytea" && cond.op === "contains") {
    throw new Error(`BYTEA column "${column}" does not support contains filters.`);
  }
  if (columnType.type === "Json" && ["gt", "gte", "lt", "lte", "contains"].includes(cond.op)) {
    throw new Error(`JSON column "${column}" only supports eq/ne/in/isNull operators.`);
  }
  switch (cond.op) {
    case "eq":
      if (cond.value === null) {
        return { IsNull: { column: columnRef } };
      }
      return { Cmp: { left: columnRef, op: "Eq", right: rightLiteral } };
    case "ne":
      if (cond.value === null) {
        return { IsNotNull: { column: columnRef } };
      }
      return {
        Cmp: {
          left: columnRef,
          op: "Ne",
          right: rightLiteral,
        },
      };
    case "gt":
      return {
        Cmp: {
          left: columnRef,
          op: "Gt",
          right: rightLiteral,
        },
      };
    case "gte":
      return {
        Cmp: {
          left: columnRef,
          op: "Ge",
          right: rightLiteral,
        },
      };
    case "lt":
      return {
        Cmp: {
          left: columnRef,
          op: "Lt",
          right: rightLiteral,
        },
      };
    case "lte":
      return {
        Cmp: {
          left: columnRef,
          op: "Le",
          right: rightLiteral,
        },
      };
    case "isNull":
      if (typeof isNullValue !== "boolean") {
        throw new Error('"isNull" operator requires a boolean value.');
      }
      return isNullValue ? { IsNull: { column: columnRef } } : { IsNotNull: { column: columnRef } };
    case "contains":
      return { Contains: { left: columnRef, right: rightLiteral } };
    default:
      throw new Error(`Unknown operator: ${cond.op}`);
  }
}

function isFrontierRowIdToken(value: unknown): value is { __jazz_ir_frontier_row_id: true } {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const marker = value as { __jazz_ir_frontier_row_id?: unknown };
  return marker.__jazz_ir_frontier_row_id === true;
}

function conditionsToRelPredicate(
  conditions: Array<{ column: string; op: string; value: unknown }>,
  schema: WasmSchema,
  table: string,
  scope?: string,
): RelPredicateExpr {
  if (conditions.length === 0) {
    return "True";
  }
  if (conditions.length === 1) {
    return conditionToRelPredicate(conditions[0]!, schema, table, scope);
  }
  return {
    And: conditions.map((condition) => conditionToRelPredicate(condition, schema, table, scope)),
  };
}

function applyFilter(input: RelExpr, predicate: RelPredicateExpr): RelExpr {
  if (predicate === "True") {
    return input;
  }
  return { Filter: { input, predicate } };
}

function lowerHopsToRelExpr(
  input: RelExpr,
  seedTable: string,
  hops: readonly string[],
  relations: Map<string, Relation[]>,
  schema: WasmSchema,
): RelExpr {
  if (hops.length === 0) {
    return input;
  }

  let currentExpr = input;
  let currentTable = seedTable;
  let currentScope = seedTable;

  for (let i = 0; i < hops.length; i += 1) {
    const hopName = hops[i];
    const tableRelations = relations.get(currentTable) ?? [];
    const relation = tableRelations.find((candidate) => candidate.name === hopName);
    if (!relation) {
      throw new Error(`Unknown relation "${hopName}" on table "${currentTable}"`);
    }

    const hopAlias = `__hop_${i}`;
    const joinOn: RelJoinCondition =
      relation.type === "forward"
        ? {
            left: relColumn(relation.fromColumn, currentScope),
            right: relColumn("id", hopAlias),
          }
        : {
            left: relColumn("id", currentScope),
            right: relColumn(relation.toColumn, hopAlias),
          };
    currentExpr = {
      Join: {
        left: currentExpr,
        right: { TableScan: { table: relation.toTable, alias: hopAlias } },
        on: [joinOn],
        join_kind: "Inner",
      },
    };

    currentTable = relation.toTable;
    currentScope = hopAlias;
  }

  return {
    Project: {
      input: currentExpr,
      columns: relationColumnsForTable(currentTable, currentScope, schema),
    },
  };
}

function gatherToRelExpr(
  gather: BuiltGather,
  seedTable: string,
  seedExpr: RelExpr,
  relations: Map<string, Relation[]>,
  schema: WasmSchema,
): RelExpr {
  if (!schema[gather.step_table]) {
    throw new Error(`Unknown gather step table "${gather.step_table}"`);
  }
  if (!Number.isInteger(gather.max_depth) || gather.max_depth < 0) {
    throw new Error("gather(...) max_depth must be a non-negative integer.");
  }

  const stepHops = Array.isArray(gather.step_hops)
    ? gather.step_hops.filter((hop): hop is string => typeof hop === "string")
    : [];
  if (stepHops.length !== 1) {
    throw new Error("gather(...) currently requires exactly one hopTo(...) step.");
  }

  const stepRelations = relations.get(gather.step_table) ?? [];
  const hopName = stepHops[0];
  const hopRelation = stepRelations.find((rel) => rel.name === hopName);
  if (!hopRelation) {
    throw new Error(`Unknown relation "${hopName}" on table "${gather.step_table}"`);
  }
  if (hopRelation.type !== "forward") {
    throw new Error("gather(...) currently only supports forward hopTo(...) relations.");
  }
  if (hopRelation.toTable !== seedTable) {
    throw new Error(
      `gather(...) step must hop back to "${seedTable}" rows, got "${hopRelation.toTable}".`,
    );
  }

  const stepBase: RelExpr = { TableScan: { table: gather.step_table } };
  const stepConditions = Array.isArray(gather.step_conditions) ? gather.step_conditions : [];
  const stepScope = gather.step_table;
  const stepPredicateConditions = [
    ...stepConditions,
    {
      column: stripQualifier(gather.step_current_column),
      op: "eq",
      value: { __jazz_ir_frontier_row_id: true },
    },
  ];
  const stepPredicate = conditionsToRelPredicate(
    stepPredicateConditions,
    schema,
    gather.step_table,
    stepScope,
  );
  const stepFiltered = applyFilter(stepBase, stepPredicate);

  const recursiveHopAlias = "__recursive_hop_0";
  const stepJoined: RelExpr = {
    Join: {
      left: stepFiltered,
      right: { TableScan: { table: hopRelation.toTable, alias: recursiveHopAlias } },
      on: [
        {
          left: relColumn(hopRelation.fromColumn, gather.step_table),
          right: relColumn("id", recursiveHopAlias),
        },
      ],
      join_kind: "Inner",
    },
  };

  const stepProjected: RelExpr = {
    Project: {
      input: stepJoined,
      columns: relationColumnsForTable(seedTable, recursiveHopAlias, schema),
    },
  };

  return {
    Gather: {
      seed: seedExpr,
      step: stepProjected,
      frontier_key: { RowId: "Current" },
      bound: { MaxDepth: gather.max_depth },
      dedupe_key: [{ RowId: "Current" }],
    },
  };
}

function resolveHopsOutputTable(
  seedTable: string,
  hops: readonly string[],
  relations: Map<string, Relation[]>,
): string {
  let currentTable = seedTable;
  for (const hopName of hops) {
    const tableRelations = relations.get(currentTable) ?? [];
    const relation = tableRelations.find((candidate) => candidate.name === hopName);
    if (!relation) {
      throw new Error(`Unknown relation "${hopName}" on table "${currentTable}"`);
    }
    currentTable = relation.toTable;
  }
  return currentTable;
}

function translateBuiltRelationToRelExpr(
  relation: BuiltRelation,
  relations: Map<string, Relation[]>,
  schema: WasmSchema,
): { expr: RelExpr; outputTable: string } {
  if (relation.union) {
    const inputs = relation.union.inputs.map((arm) =>
      translateBuiltRelationToRelExpr(arm.input, relations, schema),
    );
    const first = inputs[0];
    if (!first) {
      throw new Error("union(...) requires at least one seed relation.");
    }
    if (inputs.some((input) => input.outputTable !== first.outputTable)) {
      throw new Error("union(...) requires all seed relations to output the same table.");
    }
    return {
      expr: {
        Union: {
          inputs: relation.union.inputs.map((arm, index) => ({
            label: arm.label,
            input: inputs[index]!.expr,
          })),
        },
      },
      outputTable: first.outputTable,
    };
  }

  if (!relation.table) {
    throw new Error("gather(...) seed relation is missing table metadata.");
  }

  let expr: RelExpr = { TableScan: { table: relation.table } };
  expr = applyFilter(
    expr,
    conditionsToRelPredicate(relation.conditions ?? [], schema, relation.table, relation.table),
  );

  let outputTable = relation.table;
  if (relation.gather) {
    const seed = relation.gather.seed
      ? translateBuiltRelationToRelExpr(relation.gather.seed, relations, schema)
      : { expr, outputTable };
    expr = gatherToRelExpr(relation.gather, seed.outputTable, seed.expr, relations, schema);
    outputTable = seed.outputTable;
  }

  const hops = relation.hops ?? [];
  expr = lowerHopsToRelExpr(expr, outputTable, hops, relations, schema);
  outputTable = resolveHopsOutputTable(outputTable, hops, relations);

  return { expr, outputTable };
}

/**
 * Translate QueryBuilder JSON to relation IR.
 *
 * This emits the canonical compositional form:
 * - hopTo => Join + Project
 * - gather => Gather with step Join + Project
 */
function translateBuilderToRelationIr(
  builderJson: string,
  schema: WasmSchema,
): { relation: RelExpr; outputTable: string } {
  const builder = normalizeBuiltQuery(JSON.parse(builderJson));
  const relations = analyzeRelations(schema);
  const hops = builder.hops;

  if (builder.gather && Object.keys(builder.includes).length > 0) {
    throw new Error("gather(...) does not yet support include(...).");
  }
  if (hops.length > 0 && Object.keys(builder.includes).length > 0) {
    throw new Error("hopTo(...) does not yet support include(...).");
  }

  let relation: RelExpr;
  let relationTable: string;

  if (builder.gather?.seed) {
    const seed = translateBuiltRelationToRelExpr(builder.gather.seed, relations, schema);
    relation = gatherToRelExpr(builder.gather, seed.outputTable, seed.expr, relations, schema);
    relationTable = seed.outputTable;
    relation = applyFilter(
      relation,
      conditionsToRelPredicate(builder.conditions, schema, relationTable, relationTable),
    );
    relation = lowerHopsToRelExpr(relation, relationTable, hops, relations, schema);
    relationTable = resolveHopsOutputTable(relationTable, hops, relations);
  } else {
    const translated = translateBuiltRelationToRelExpr(
      {
        table: builder.table,
        conditions: builder.conditions,
        hops: builder.hops,
        gather: builder.gather,
        union: builder.union,
      },
      relations,
      schema,
    );
    relation = translated.expr;
    relationTable = translated.outputTable;
  }

  // The Rust relation facade identifies its output scope through Project. A
  // payload match can be the only relation feature, so preserve the ordinary
  // root-table result shape with an explicit identity projection in that case.
  if (builder.hops.length === 0 && builder.gather === undefined) {
    const columns = schema[relationTable]?.columns;
    if (!columns) throw new Error(`Unknown table "${relationTable}" in relation query.`);
    relation = {
      Project: {
        input: relation,
        columns: columns.map((column) => ({
          alias: column.name,
          expr: { Column: relColumn(column.name, relationTable) },
        })),
      },
    };
  }

  if (Array.isArray(builder.orderBy) && builder.orderBy.length > 0) {
    for (const [column] of builder.orderBy) {
      const columnType = getColumnType(schema, relationTable, stripQualifier(column));
      if (columnType?.type === "Bytea") {
        throw new Error(`BYTEA column "${column}" cannot be used in orderBy().`);
      }
      if (columnType?.type === "Json") {
        throw new Error(`JSON column "${column}" cannot be used in orderBy().`);
      }
    }
    relation = {
      OrderBy: {
        input: relation,
        terms: builder.orderBy.map(([column, direction]) => ({
          column: relColumn(
            stripQualifier(column),
            hops.length > 0 ? `__hop_${hops.length - 1}` : relationTable,
          ),
          direction: direction === "desc" ? "Desc" : "Asc",
        })),
      },
    };
  }

  if (typeof builder.offset === "number" && builder.offset > 0) {
    relation = {
      Offset: {
        input: relation,
        offset: builder.offset,
      },
    };
  }
  if (typeof builder.limit === "number") {
    relation = {
      Limit: {
        input: relation,
        limit: builder.limit,
      },
    };
  }

  return { relation, outputTable: relationTable };
}

function usesNativeRelationFeatures(builder: ReturnType<typeof normalizeBuiltQuery>): boolean {
  // Flat queries predate payload enum matching and have no representation for
  // its nested predicate. Route it through the relation IR even without a hop
  // so the public enum-match node reaches the Rust query compiler.
  return (
    builder.union !== undefined ||
    builder.hops.length > 0 ||
    builder.gather !== undefined ||
    builder.conditions.some((condition) => condition.op === "match")
  );
}

function toRuntimeOrderBy(
  orderBy: Array<[string, "asc" | "desc"]>,
  schema: WasmSchema,
  table: string,
): Array<{ column: string; direction: "Asc" | "Desc" }> {
  return orderBy.map(([column, direction]) => {
    const strippedColumn = stripQualifier(column);
    const columnType = getColumnType(schema, table, strippedColumn);
    if (columnType?.type === "Bytea") {
      throw new Error(`BYTEA column "${column}" cannot be used in orderBy().`);
    }
    if (columnType?.type === "Json") {
      throw new Error(`JSON column "${column}" cannot be used in orderBy().`);
    }
    return {
      column: strippedColumn,
      direction: direction === "desc" ? "Desc" : "Asc",
    };
  });
}

function stringifyRuntimeQuery(value: unknown): string {
  // JSON has no bigint token. Preserve the exact decimal spelling for the
  // typed relation literal; Rust accepts that string only for a BigInt value.
  return JSON.stringify(value, (_key, candidate) =>
    typeof candidate === "bigint" ? candidate.toString() : candidate,
  );
}

function toFlatConditions(
  conditions: BuiltCondition[],
  schema: WasmSchema,
  table: string,
): RelPredicateExpr[] {
  // Keep the legacy query envelope for ordinary table reads, but make its
  // predicates the same canonical IR used by included relations. In
  // particular, notIn is represented as Not(In(...)), never expanded in JS.
  return conditions.map((condition) => conditionToRelPredicate(condition, schema, table));
}

/**
 * Translate QueryBuilder JSON to runtime query JSON.
 *
 * @param builderJson JSON string from QueryBuilder._build()
 * @param schema WasmSchema for relation analysis
 * @returns JSON string for runtime query()
 */
export function translateQuery(builderJson: string, schema: WasmSchema): string {
  const builder = normalizeBuiltQuery(JSON.parse(builderJson));
  const relations = analyzeRelations(schema);
  const selectColumns =
    builder.select.length > 0 ? resolveSelectedColumns(builder.table, schema, builder.select) : [];
  const projectedColumns = visibleSelectColumns(
    selectColumns,
    builder.partialSelect,
    schema,
    builder.table,
  );
  const arraySubqueries = toArraySubqueries(builder.includes, builder.table, relations, schema, {
    requireIncludes: builder.requireIncludes,
  });

  if (usesNativeRelationFeatures(builder)) {
    const { relation, outputTable } = translateBuilderToRelationIr(builderJson, schema);
    return stringifyRuntimeQuery({
      table: outputTable,
      array_subqueries: arraySubqueries,
      relation_ir: relation,
      ...(builder.includeDeleted ? { include_deleted: true } : {}),
      ...(projectedColumns ? { select_columns: projectedColumns } : {}),
    });
  }

  const orderBy = toRuntimeOrderBy(builder.orderBy, schema, builder.table);
  const clientLimit = typeof builder.limit === "number" ? builder.limit : undefined;
  const clientOffset = typeof builder.offset === "number" ? builder.offset : undefined;
  const query = {
    table: builder.table,
    conditions: toFlatConditions(builder.conditions, schema, builder.table),
    array_subqueries: arraySubqueries,
    ...(builder.includeDeleted ? { include_deleted: true } : {}),
    ...(projectedColumns ? { select_columns: projectedColumns } : {}),
    ...(orderBy.length > 0 ? { order_by: orderBy } : {}),
    ...(clientLimit !== undefined ? { limit: clientLimit } : {}),
    ...(clientOffset !== undefined ? { offset: clientOffset } : {}),
  };

  return stringifyRuntimeQuery(query);
}

/**
 * Translate QueryBuilder JSON to WASM Query format.
 *
 * QueryBuilder produces a compact JSON structure:
 * { table, conditions, includes, orderBy, limit, offset, hops?, gather? }
 *
 * Runtime semantics are driven by `relation_ir`. The wire payload keeps only
 * fields required for execution (`table`, `relation_ir`, and `array_subqueries`).
 */

import type { ColumnType, WasmSchema } from "../drivers/types.js";
import { toJsonText } from "./json-text.js";
import { analyzeRelations, type Relation } from "../codegen/relation-analyzer.js";
import { isProvenanceMagicTimestampColumn, magicColumnType } from "../magic-columns.js";
import {
  normalizeBuiltQuery,
  type BuiltCondition,
  type BuiltGather,
  type BuiltRelation,
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

function stripQualifier(column: string): string {
  const parts = column.split(".");
  return parts[parts.length - 1] ?? column;
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

function toRuntimeTimestampValue(value: unknown, columnName?: string): number {
  const timestampMs = toTimestampMs(value);
  return columnName && isProvenanceMagicTimestampColumn(columnName)
    ? timestampMs * 1_000
    : timestampMs;
}

/**
 * Translate a JavaScript value to WasmValue format.
 */
function toWasmValue(value: unknown, columnType: ColumnType, columnName?: string): object {
  if (value === null || value === undefined) {
    return { type: "Null" };
  }
  if (columnType.type === "Json") {
    return { type: "Text", value: toJsonText(value) };
  }
  if (columnType.type === "Timestamp" && value instanceof Date) {
    return { type: "Timestamp", value: toRuntimeTimestampValue(value, columnName) };
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
      value: value.map((item) => toWasmValue(item, columnType.element)),
    };
  }
  if (typeof value === "boolean") {
    return { type: "Boolean", value };
  }
  if (typeof value === "number") {
    if (columnType?.type === "Timestamp") {
      return { type: "Timestamp", value: toRuntimeTimestampValue(value, columnName) };
    }
    // Use Integer for all numbers - WASM will handle type coercion
    return { type: "Integer", value };
  }
  if (typeof value === "string") {
    if (columnType?.type === "Timestamp") {
      return { type: "Timestamp", value: toRuntimeTimestampValue(value, columnName) };
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

function visibleSelectColumns(
  resolvedSelect: readonly string[],
  includeProjectionColumns: readonly string[] = [],
): string[] | null {
  const columns = [...resolvedSelect, ...includeProjectionColumns];
  return columns.length > 0 ? columns : null;
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
}

function conditionToArraySubqueryFilter(
  cond: BuiltCondition,
  schema: WasmSchema,
  table: string,
): object {
  const column = stripQualifier(cond.column);
  const columnType = getColumnType(schema, table, column);
  if (!columnType) {
    throw new Error(`Unknown column "${column}" in table "${table}"`);
  }

  if (columnType.type === "Bytea" && ["gt", "gte", "lt", "lte"].includes(cond.op)) {
    throw new Error(`BYTEA column "${column}" only supports eq/ne operators.`);
  }
  if (columnType.type === "Bytea" && cond.op === "contains") {
    throw new Error(`BYTEA column "${column}" does not support contains filters.`);
  }
  if (columnType.type === "Json" && ["gt", "gte", "lt", "lte", "contains"].includes(cond.op)) {
    throw new Error(`JSON column "${column}" only supports eq/ne/in/isNull operators.`);
  }

  const valueTypeForCondition =
    cond.op === "contains" && columnType.type === "Array" ? columnType.element : columnType;
  const literalValue = toWasmValue(cond.value, valueTypeForCondition, column);
  const isNullValue = cond.value === undefined ? true : cond.value;

  switch (cond.op) {
    case "eq":
      if (cond.value === null) {
        return { IsNull: { column } };
      }
      return { Eq: { column, value: literalValue } };
    case "ne":
      if (cond.value === null) {
        return { IsNotNull: { column } };
      }
      return { Ne: { column, value: literalValue } };
    case "gt":
      return { Gt: { column, value: literalValue } };
    case "gte":
      return { Ge: { column, value: literalValue } };
    case "lt":
      return { Lt: { column, value: literalValue } };
    case "lte":
      return { Le: { column, value: literalValue } };
    case "isNull":
      if (typeof isNullValue !== "boolean") {
        throw new Error('"isNull" operator requires a boolean value.');
      }
      return isNullValue ? { IsNull: { column } } : { IsNotNull: { column } };
    case "contains":
      return { Contains: { column, value: literalValue } };
    default:
      throw new Error(
        `Include builder for table "${table}" does not support "${cond.op}" filters.`,
      );
  }
}

function toArraySubqueries(
  includes: NormalizedIncludeSpec,
  tableName: string,
  relations: Map<string, Relation[]>,
  schema: WasmSchema,
  options?: { hideCurrentLevelColumnNames?: boolean; requireIncludes?: boolean },
): object[] {
  const tableRels = relations.get(tableName) || [];
  const subqueries: object[] = [];
  const hideCurrentLevelColumnNames = options?.hideCurrentLevelColumnNames === true;
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
    const includeProjectionColumns = hasExplicitSelect
      ? Object.keys(spec.includes).map((relationName) => hiddenIncludeColumnName(relationName))
      : [];
    const filters = spec.conditions.map((condition) =>
      conditionToArraySubqueryFilter(condition, schema, rel.toTable),
    );
    const orderBy = spec.orderBy.map(([column, direction]) => [
      stripQualifier(column),
      direction === "desc" ? "Descending" : "Ascending",
    ]);
    const nestedArrays = toArraySubqueries(spec.includes, rel.toTable, relations, schema, {
      hideCurrentLevelColumnNames: hasExplicitSelect,
      requireIncludes: spec.requireIncludes,
    });
    const selectColumns = visibleSelectColumns(resolvedSelectColumns, includeProjectionColumns);

    // Build the subquery based on relation type
    if (rel.type === "forward") {
      // Forward relation: todos.owner_id -> users.id
      // We join from the FK column to the target table's id
      const requirement = includeRequirementForRelation(rel, requireCurrentLevelIncludes);
      subqueries.push({
        column_name: hideCurrentLevelColumnNames ? hiddenIncludeColumnName(relName) : relName,
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
        column_name: hideCurrentLevelColumnNames ? hiddenIncludeColumnName(relName) : relName,
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
  const valueTypeForCondition =
    cond.op === "contains" && columnType.type === "Array" ? columnType.element : columnType;
  const rightLiteral =
    isFrontierRowIdToken(cond.value) && cond.op === "eq"
      ? { RowId: "Frontier" as const }
      : {
          Literal: toWasmValue(cond.value, valueTypeForCondition, column),
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
    case "in":
      if (!Array.isArray(cond.value)) {
        throw new Error('"in" operator requires an array value');
      }
      return {
        In: {
          left: columnRef,
          values: cond.value.map((value) => ({
            Literal: toWasmValue(value, columnType, column),
          })),
        },
      };
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
        right: { TableScan: { table: relation.toTable } },
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
  if (!Number.isInteger(gather.max_depth) || gather.max_depth <= 0) {
    throw new Error("gather(...) max_depth must be a positive integer.");
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
      right: { TableScan: { table: hopRelation.toTable } },
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
      max_depth: gather.max_depth,
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
    const inputs = relation.union.inputs.map((input) =>
      translateBuiltRelationToRelExpr(input, relations, schema),
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
          inputs: inputs.map((input) => input.expr),
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
export function translateBuilderToRelationIr(builderJson: string, schema: WasmSchema): RelExpr {
  const builder = normalizeBuiltQuery(JSON.parse(builderJson), "");
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
      },
      relations,
      schema,
    );
    relation = translated.expr;
    relationTable = translated.outputTable;
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
          column: relColumn(column),
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

  return relation;
}

/**
 * Translate QueryBuilder JSON to WASM Query JSON.
 *
 * @param builderJson JSON string from QueryBuilder._build()
 * @param schema WasmSchema for relation analysis
 * @returns JSON string for WASM runtime query()
 */
export function translateQuery(builderJson: string, schema: WasmSchema): string {
  const builder = normalizeBuiltQuery(JSON.parse(builderJson), "");
  const relations = analyzeRelations(schema);
  const relation = translateBuilderToRelationIr(builderJson, schema);
  const hasExplicitSelect = builder.select.length > 0;
  const selectColumns = hasExplicitSelect
    ? resolveSelectedColumns(builder.table, schema, builder.select)
    : [];
  const includeProjectionColumns = hasExplicitSelect
    ? Object.keys(builder.includes).map((relationName) => hiddenIncludeColumnName(relationName))
    : [];
  const projectedColumns = visibleSelectColumns(selectColumns, includeProjectionColumns);
  const query = {
    table: builder.table,
    array_subqueries: toArraySubqueries(builder.includes, builder.table, relations, schema, {
      hideCurrentLevelColumnNames: hasExplicitSelect,
      requireIncludes: builder.requireIncludes,
    }),
    relation_ir: relation,
    ...(projectedColumns ? { select_columns: projectedColumns } : {}),
  };

  return JSON.stringify(query);
}

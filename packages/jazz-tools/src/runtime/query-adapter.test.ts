/**
 * Tests for query-adapter.
 */

import { describe, it, expect } from "vitest";
import { translateBuilderToRelationIr, translateQuery } from "./query-adapter.js";
import type { WasmSchema } from "../drivers/types.js";
import { toLegacyRelExprForTest } from "../testing/relation-ir-test-helpers.js";

describe("translateQuery", () => {
  function parseTranslatedQuery(builderJson: string, schema: WasmSchema): any {
    const parsed = JSON.parse(translateQuery(builderJson, schema));
    parsed.relation_ir = toLegacyRelExprForTest(parsed.relation_ir);
    return parsed;
  }

  function expectFilterPredicate(result: any): any {
    expect(result.relation_ir?.type).toBe("Filter");
    if (result.relation_ir?.type !== "Filter") {
      throw new Error("Expected relation_ir Filter node.");
    }
    return result.relation_ir.predicate;
  }

  const basicSchema: WasmSchema = {
    todos: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "done", column_type: { type: "Boolean" }, nullable: false },
        { name: "priority", column_type: { type: "Integer" }, nullable: true },
        {
          name: "status",
          column_type: { type: "Enum", variants: ["done", "in_progress", "todo"] },
          nullable: false,
        },
        { name: "project", column_type: { type: "Uuid" }, nullable: true },
        {
          name: "tags",
          column_type: { type: "Array", element: { type: "Text" } },
          nullable: false,
        },
        { name: "metadata", column_type: { type: "Json" }, nullable: true },
        { name: "created_at", column_type: { type: "Timestamp" }, nullable: true },
      ],
    },
  };

  describe("basic query structure", () => {
    it("translates empty query", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);

      expect(result.table).toBe("todos");
      expect(result.array_subqueries).toEqual([]);
      expect(result.relation_ir).toEqual({ type: "TableScan", table: "todos" });
      expect(result.branches).toBeUndefined();
      expect(result.disjuncts).toBeUndefined();
      expect(result.order_by).toBeUndefined();
      expect(result.offset).toBeUndefined();
      expect(result.limit).toBeUndefined();
      expect(result.include_deleted).toBeUndefined();
      expect(result.joins).toBeUndefined();
    });

    it("translates limit and offset", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
        limit: 10,
        offset: 5,
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);

      expect(result.relation_ir?.type).toBe("Limit");
      if (result.relation_ir?.type !== "Limit") {
        throw new Error("Expected relation_ir Limit node.");
      }
      expect(result.relation_ir.limit).toBe(10);
      expect(result.relation_ir.input?.type).toBe("Offset");
      if (result.relation_ir.input?.type !== "Offset") {
        throw new Error("Expected relation_ir Offset node.");
      }
      expect(result.relation_ir.input.offset).toBe(5);
    });

    it("pushes select columns into the runtime query payload", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        select: ["title", "done"],
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);

      expect(result.select_columns).toEqual(["title", "done"]);
      expect(result.relation_ir).toEqual({ type: "TableScan", table: "todos" });
    });

    it("pushes magic select columns into the runtime query payload", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        select: ["title", "$canRead", "$canEdit"],
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);

      expect(result.select_columns).toEqual(["title", "$canRead", "$canEdit"]);
      expect(result.relation_ir).toEqual({ type: "TableScan", table: "todos" });
    });

    it("pushes provenance magic select columns into the runtime query payload", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        select: ["title", "$createdBy", "$updatedAt"],
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);

      expect(result.select_columns).toEqual(["title", "$createdBy", "$updatedAt"]);
      expect(result.relation_ir).toEqual({ type: "TableScan", table: "todos" });
    });

    it('treats select(["*"]) as selecting all columns', () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        select: ["*"],
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);

      expect(result.select_columns).toEqual([
        "title",
        "done",
        "priority",
        "status",
        "project",
        "tags",
        "metadata",
        "created_at",
      ]);
      expect(result.relation_ir).toEqual({ type: "TableScan", table: "todos" });
    });

    it('expands mixed select(["*", "$canDelete"]) into explicit runtime columns', () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        select: ["*", "$canDelete"],
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);

      expect(result.select_columns).toEqual([
        "title",
        "done",
        "priority",
        "status",
        "project",
        "tags",
        "metadata",
        "created_at",
        "$canDelete",
      ]);
      expect(result.relation_ir).toEqual({ type: "TableScan", table: "todos" });
    });
  });

  describe("condition translation", () => {
    it("translates eq condition with string", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "title", op: "eq", value: "Buy milk" }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "title" },
        op: "Eq",
        right: { type: "Literal", value: { Text: "Buy milk" } },
      });
    });

    it("translates eq condition with provenance magic text columns", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "$createdBy", op: "eq", value: "alice" }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "$createdBy" },
        op: "Eq",
        right: { type: "Literal", value: { Text: "alice" } },
      });
    });

    it("translates eq condition with enum value", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "status", op: "eq", value: "todo" }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "status" },
        op: "Eq",
        right: { type: "Literal", value: { Text: "todo" } },
      });
    });

    it("translates eq condition with UUID string for Uuid columns", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [
          { column: "project", op: "eq", value: "00000000-0000-0000-0000-000000000123" },
        ],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "project" },
        op: "Eq",
        right: {
          type: "Literal",
          value: { Uuid: "00000000-0000-0000-0000-000000000123" },
        },
      });
    });

    it("treats implicit id column as UUID", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "id", op: "eq", value: "00000000-0000-0000-0000-000000000abc" }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "id" },
        op: "Eq",
        right: {
          type: "Literal",
          value: { Uuid: "00000000-0000-0000-0000-000000000abc" },
        },
      });
    });

    it("translates eq condition with boolean", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "done", op: "eq", value: false }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "done" },
        op: "Eq",
        right: { type: "Literal", value: { Boolean: false } },
      });
    });

    it("translates eq condition with magic boolean column", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "$canEdit", op: "eq", value: true }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "$canEdit" },
        op: "Eq",
        right: { type: "Literal", value: { Boolean: true } },
      });
    });

    it("translates eq condition with number", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "eq", value: 5 }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "priority" },
        op: "Eq",
        right: { type: "Literal", value: { Integer: 5 } },
      });
    });

    it("translates eq condition with number for Timestamp columns", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "created_at", op: "eq", value: 1712345678 }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "created_at" },
        op: "Eq",
        right: { type: "Literal", value: { Timestamp: 1712345678 } },
      });
    });

    it("translates eq condition with ISO string for Timestamp columns", () => {
      const iso = "2024-01-01T00:00:00.000Z";
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "created_at", op: "eq", value: iso }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "created_at" },
        op: "Eq",
        right: { type: "Literal", value: { Timestamp: Date.parse(iso) } },
      });
    });

    it("throws for invalid timestamp string condition", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "created_at", op: "eq", value: "not-a-date" }],
        includes: {},
        orderBy: [],
      });

      expect(() => parseTranslatedQuery(builderJson, basicSchema)).toThrow(
        "Invalid timestamp condition",
      );
    });

    it("translates numeric string for Timestamp columns as epoch number", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "created_at", op: "eq", value: "1712345678" }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "created_at" },
        op: "Eq",
        right: { type: "Literal", value: { Timestamp: 1712345678 } },
      });
    });

    it("translates eq condition with array value", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [
          {
            column: "tags",
            op: "eq",
            value: ["tag1", "tag2"],
          },
        ],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "tags" },
        op: "Eq",
        right: {
          type: "Literal",
          value: {
            Array: [{ Text: "tag1" }, { Text: "tag2" }],
          },
        },
      });
    });

    it("translates eq condition with Json object value", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "metadata", op: "eq", value: { phase: "alpha", retries: 1 } }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "metadata" },
        op: "Eq",
        right: {
          type: "Literal",
          value: { Text: '{"phase":"alpha","retries":1}' },
        },
      });
    });

    it("translates in condition with Json values", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [
          {
            column: "metadata",
            op: "in",
            value: [{ phase: "alpha" }, { phase: "beta" }],
          },
        ],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "In",
        left: { scope: "todos", column: "metadata" },
        values: [
          { type: "Literal", value: { Text: '{"phase":"alpha"}' } },
          { type: "Literal", value: { Text: '{"phase":"beta"}' } },
        ],
      });
    });

    it("translates contains condition with array element value", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [
          {
            column: "tags",
            op: "contains",
            value: "tag1",
          },
        ],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Contains",
        left: { scope: "todos", column: "tags" },
        value: { type: "Literal", value: { Text: "tag1" } },
      });
    });

    it("translates ne condition", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "done", op: "ne", value: true }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "done" },
        op: "Ne",
        right: { type: "Literal", value: { Boolean: true } },
      });
    });

    it("translates gt condition", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "gt", value: 3 }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "priority" },
        op: "Gt",
        right: { type: "Literal", value: { Integer: 3 } },
      });
    });

    it("translates gte condition", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "gte", value: 3 }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "priority" },
        op: "Ge",
        right: { type: "Literal", value: { Integer: 3 } },
      });
    });

    it("translates gte condition with provenance magic timestamp columns", () => {
      const iso = "2026-03-31T00:00:00.000Z";
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "$updatedAt", op: "gte", value: iso }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "$updatedAt" },
        op: "Ge",
        right: { type: "Literal", value: { Timestamp: Date.parse(iso) * 1_000 } },
      });
    });

    it("translates lt condition", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "lt", value: 3 }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "priority" },
        op: "Lt",
        right: { type: "Literal", value: { Integer: 3 } },
      });
    });

    it("translates lte condition", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "lte", value: 3 }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "Cmp",
        left: { scope: "todos", column: "priority" },
        op: "Le",
        right: { type: "Literal", value: { Integer: 3 } },
      });
    });

    it("translates isNull condition", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "isNull", value: true }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "IsNull",
        column: { scope: "todos", column: "priority" },
      });
    });

    it("translates isNull=false condition to IsNotNull", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "isNull", value: false }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "IsNotNull",
        column: { scope: "todos", column: "priority" },
      });
    });

    it("rejects non-boolean values for isNull condition", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "isNull", value: "false" }],
        includes: {},
        orderBy: [],
      });

      expect(() => translateQuery(builderJson, basicSchema)).toThrow(
        '"isNull" operator requires a boolean value.',
      );
    });

    it("translates multiple conditions", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [
          { column: "done", op: "eq", value: false },
          { column: "priority", op: "gt", value: 3 },
        ],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "And",
        exprs: [
          {
            type: "Cmp",
            left: { scope: "todos", column: "done" },
            op: "Eq",
            right: { type: "Literal", value: { Boolean: false } },
          },
          {
            type: "Cmp",
            left: { scope: "todos", column: "priority" },
            op: "Gt",
            right: { type: "Literal", value: { Integer: 3 } },
          },
        ],
      });
    });

    it("translates eq null value to IsNull", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "eq", value: null }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "IsNull",
        column: { scope: "todos", column: "priority" },
      });
    });

    it("translates ne null value to IsNotNull", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "ne", value: null }],
        includes: {},
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(expectFilterPredicate(result)).toEqual({
        type: "IsNotNull",
        column: { scope: "todos", column: "priority" },
      });
    });

    it("throws for unknown operator", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "done", op: "unknown", value: true }],
        includes: {},
        orderBy: [],
      });

      expect(() => translateQuery(builderJson, basicSchema)).toThrow("Unknown operator: unknown");
    });

    it("throws for invalid enum value", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "status", op: "eq", value: "invalid" }],
        includes: {},
        orderBy: [],
      });

      expect(() => translateQuery(builderJson, basicSchema)).toThrow("Invalid enum value");
    });

    it("rejects unsupported Json comparison operators", () => {
      const gtBuilderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "metadata", op: "gt", value: { retries: 1 } }],
        includes: {},
        orderBy: [],
      });
      expect(() => translateQuery(gtBuilderJson, basicSchema)).toThrow(
        'JSON column "metadata" only supports eq/ne/in/isNull operators.',
      );

      const containsBuilderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "metadata", op: "contains", value: { retries: 1 } }],
        includes: {},
        orderBy: [],
      });
      expect(() => translateQuery(containsBuilderJson, basicSchema)).toThrow(
        'JSON column "metadata" only supports eq/ne/in/isNull operators.',
      );
    });
  });

  describe("orderBy translation", () => {
    it("translates ascending order", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [["priority", "asc"]],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(result.relation_ir?.type).toBe("OrderBy");
      expect(result.relation_ir?.terms).toEqual([
        { column: { column: "priority" }, direction: "Asc" },
      ]);
    });

    it("translates descending order", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [["priority", "desc"]],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(result.relation_ir?.type).toBe("OrderBy");
      expect(result.relation_ir?.terms).toEqual([
        { column: { column: "priority" }, direction: "Desc" },
      ]);
    });

    it("translates multiple orderBy clauses", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [
          ["priority", "desc"],
          ["title", "asc"],
        ],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(result.relation_ir?.type).toBe("OrderBy");
      expect(result.relation_ir?.terms).toEqual([
        { column: { column: "priority" }, direction: "Desc" },
        { column: { column: "title" }, direction: "Asc" },
      ]);
    });

    it("translates magic columns in orderBy", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [["$canEdit", "desc"]],
      });

      const result = parseTranslatedQuery(builderJson, basicSchema);
      expect(result.relation_ir?.type).toBe("OrderBy");
      expect(result.relation_ir?.terms).toEqual([
        { column: { column: "$canEdit" }, direction: "Desc" },
      ]);
    });

    it("rejects Json columns in orderBy", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [["metadata", "asc"]],
      });

      expect(() => translateQuery(builderJson, basicSchema)).toThrow(
        'JSON column "metadata" cannot be used in orderBy().',
      );
    });
  });

  describe("include translation", () => {
    const schemaWithRelations: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          {
            name: "owner_id",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "users",
          },
        ],
      },
      users: {
        columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
      },
    };

    it("translates forward relation include", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: { owner: true },
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, schemaWithRelations);

      expect(result.array_subqueries).toEqual([
        {
          column_name: "owner",
          table: "users",
          inner_column: "id",
          outer_column: "todos.owner_id",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          nested_arrays: [],
        },
      ]);
    });

    it("marks forward scalar relation include as required when requested", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: { owner: true },
        __jazz_requireIncludes: true,
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, schemaWithRelations);

      expect(result.array_subqueries).toEqual([
        {
          column_name: "owner",
          table: "users",
          inner_column: "id",
          outer_column: "todos.owner_id",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          requirement: "AtLeastOne",
          nested_arrays: [],
        },
      ]);
    });

    it("hides top-level include column names when projecting selected columns", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: { owner: true },
        select: ["title"],
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, schemaWithRelations);

      expect(result.select_columns).toEqual(["title", "__jazz_include_owner"]);
      expect(result.array_subqueries).toEqual([
        {
          column_name: "__jazz_include_owner",
          table: "users",
          inner_column: "id",
          outer_column: "todos.owner_id",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          nested_arrays: [],
        },
      ]);
    });

    it("translates reverse relation include", () => {
      const builderJson = JSON.stringify({
        table: "users",
        conditions: [],
        includes: { todosViaOwner: true },
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, schemaWithRelations);

      expect(result.array_subqueries).toEqual([
        {
          column_name: "todosViaOwner",
          table: "todos",
          inner_column: "owner_id",
          outer_column: "users.id",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          nested_arrays: [],
        },
      ]);
    });

    it("translates UUID[] forward and reverse includes using membership columns", () => {
      const arrayFkSchema: WasmSchema = {
        files: {
          columns: [
            {
              name: "parts",
              column_type: { type: "Array", element: { type: "Uuid" } },
              nullable: false,
              references: "file_parts",
            },
          ],
        },
        file_parts: {
          columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
        },
      };

      const forward = JSON.parse(
        translateQuery(
          JSON.stringify({
            table: "files",
            conditions: [],
            includes: { parts: true },
            orderBy: [],
          }),
          arrayFkSchema,
        ),
      );
      expect(forward.array_subqueries).toEqual([
        {
          column_name: "parts",
          table: "file_parts",
          inner_column: "id",
          outer_column: "files.parts",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          nested_arrays: [],
        },
      ]);

      const reverse = JSON.parse(
        translateQuery(
          JSON.stringify({
            table: "file_parts",
            conditions: [],
            includes: { filesViaParts: true },
            orderBy: [],
          }),
          arrayFkSchema,
        ),
      );
      expect(reverse.array_subqueries).toEqual([
        {
          column_name: "filesViaParts",
          table: "files",
          inner_column: "parts",
          outer_column: "file_parts.id",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          nested_arrays: [],
        },
      ]);
    });

    it("marks UUID[] forward includes with cardinality requirement when requested", () => {
      const arrayFkSchema: WasmSchema = {
        files: {
          columns: [
            {
              name: "parts",
              column_type: { type: "Array", element: { type: "Uuid" } },
              nullable: false,
              references: "file_parts",
            },
          ],
        },
        file_parts: {
          columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
        },
      };

      const forward = JSON.parse(
        translateQuery(
          JSON.stringify({
            table: "files",
            conditions: [],
            includes: { parts: true },
            __jazz_requireIncludes: true,
            orderBy: [],
          }),
          arrayFkSchema,
        ),
      );
      expect(forward.array_subqueries).toEqual([
        {
          column_name: "parts",
          table: "file_parts",
          inner_column: "id",
          outer_column: "files.parts",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          requirement: "MatchCorrelationCardinality",
          nested_arrays: [],
        },
      ]);
    });

    it("skips false includes", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: { owner: false },
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, schemaWithRelations);

      expect(result.array_subqueries).toEqual([]);
    });

    it("throws for unknown relation", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: { nonexistent: true },
        orderBy: [],
      });

      expect(() => translateQuery(builderJson, schemaWithRelations)).toThrow(
        'Unknown relation "nonexistent" on table "todos"',
      );
    });

    it("translates nested includes", () => {
      const nestedSchema: WasmSchema = {
        comments: {
          columns: [
            { name: "text", column_type: { type: "Text" }, nullable: false },
            {
              name: "todo_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "todos",
            },
          ],
        },
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            {
              name: "owner_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "users",
            },
          ],
        },
        users: {
          columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
        },
      };

      const builderJson = JSON.stringify({
        table: "comments",
        conditions: [],
        includes: {
          todo: {
            owner: true,
          },
        },
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, nestedSchema);

      expect(result.array_subqueries).toEqual([
        {
          column_name: "todo",
          table: "todos",
          inner_column: "id",
          outer_column: "comments.todo_id",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          nested_arrays: [
            {
              column_name: "owner",
              table: "users",
              inner_column: "id",
              outer_column: "todos.owner_id",
              filters: [],
              joins: [],
              select_columns: null,
              order_by: [],
              limit: null,
              nested_arrays: [],
            },
          ],
        },
      ]);
    });

    it("translates nested required includes", () => {
      const nestedSchema: WasmSchema = {
        comments: {
          columns: [
            { name: "text", column_type: { type: "Text" }, nullable: false },
            {
              name: "todo_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "todos",
            },
          ],
        },
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            {
              name: "owner_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "users",
            },
          ],
        },
        users: {
          columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
        },
      };

      const builderJson = JSON.stringify({
        table: "comments",
        conditions: [],
        includes: {
          todo: {
            table: "todos",
            conditions: [],
            includes: { owner: true },
            __jazz_requireIncludes: true,
            select: [],
            orderBy: [],
            hops: [],
          },
        },
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, nestedSchema);

      expect(result.array_subqueries).toEqual([
        {
          column_name: "todo",
          table: "todos",
          inner_column: "id",
          outer_column: "comments.todo_id",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          nested_arrays: [
            {
              column_name: "owner",
              table: "users",
              inner_column: "id",
              outer_column: "todos.owner_id",
              filters: [],
              joins: [],
              select_columns: null,
              order_by: [],
              limit: null,
              requirement: "AtLeastOne",
              nested_arrays: [],
            },
          ],
        },
      ]);
    });

    it("translates include builders with projected nested columns", () => {
      const nestedSchema: WasmSchema = {
        comments: {
          columns: [
            { name: "text", column_type: { type: "Text" }, nullable: false },
            {
              name: "todo_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "todos",
            },
          ],
        },
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            {
              name: "owner_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "users",
            },
          ],
        },
        users: {
          columns: [
            { name: "name", column_type: { type: "Text" }, nullable: false },
            { name: "email", column_type: { type: "Text" }, nullable: false },
          ],
        },
      };

      const builderJson = JSON.stringify({
        table: "comments",
        conditions: [],
        includes: {
          todo: {
            table: "todos",
            conditions: [],
            includes: {
              owner: {
                table: "users",
                conditions: [],
                includes: {},
                select: ["name"],
                orderBy: [],
                hops: [],
              },
            },
            select: ["title"],
            orderBy: [],
            hops: [],
          },
        },
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, nestedSchema);

      expect(result.array_subqueries).toEqual([
        {
          column_name: "todo",
          table: "todos",
          inner_column: "id",
          outer_column: "comments.todo_id",
          filters: [],
          joins: [],
          select_columns: ["title", "__jazz_include_owner"],
          order_by: [],
          limit: null,
          nested_arrays: [
            {
              column_name: "__jazz_include_owner",
              table: "users",
              inner_column: "id",
              outer_column: "todos.owner_id",
              filters: [],
              joins: [],
              select_columns: ["name"],
              order_by: [],
              limit: null,
              nested_arrays: [],
            },
          ],
        },
      ]);
    });

    it("translates include builders with mixed wildcard and magic projections", () => {
      const nestedSchema: WasmSchema = {
        comments: {
          columns: [
            { name: "text", column_type: { type: "Text" }, nullable: false },
            {
              name: "todo_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "todos",
            },
          ],
        },
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            {
              name: "owner_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "users",
            },
          ],
        },
        users: {
          columns: [
            { name: "name", column_type: { type: "Text" }, nullable: false },
            { name: "email", column_type: { type: "Text" }, nullable: false },
          ],
        },
      };

      const builderJson = JSON.stringify({
        table: "comments",
        conditions: [],
        includes: {
          todo: {
            table: "todos",
            conditions: [],
            includes: {
              owner: {
                table: "users",
                conditions: [],
                includes: {},
                select: ["*", "$canEdit"],
                orderBy: [],
                hops: [],
              },
            },
            select: ["*", "$canDelete"],
            orderBy: [],
            hops: [],
          },
        },
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, nestedSchema);

      expect(result.array_subqueries).toEqual([
        {
          column_name: "todo",
          table: "todos",
          inner_column: "id",
          outer_column: "comments.todo_id",
          filters: [],
          joins: [],
          select_columns: ["title", "owner_id", "$canDelete", "__jazz_include_owner"],
          order_by: [],
          limit: null,
          nested_arrays: [
            {
              column_name: "__jazz_include_owner",
              table: "users",
              inner_column: "id",
              outer_column: "todos.owner_id",
              filters: [],
              joins: [],
              select_columns: ["name", "email", "$canEdit"],
              order_by: [],
              limit: null,
              nested_arrays: [],
            },
          ],
        },
      ]);
    });

    it("omits implicit id from include builder projections", () => {
      const builderJson = JSON.stringify({
        table: "users",
        conditions: [],
        includes: {
          todosViaOwner: {
            table: "todos",
            conditions: [],
            includes: {},
            select: ["id"],
            orderBy: [],
            hops: [],
          },
        },
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, schemaWithRelations);

      expect(result.array_subqueries).toEqual([
        {
          column_name: "todosViaOwner",
          table: "todos",
          inner_column: "owner_id",
          outer_column: "users.id",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          nested_arrays: [],
        },
      ]);
    });

    it('keeps projected include mode for include builders that select only "id"', () => {
      const nestedSchema: WasmSchema = {
        users: {
          columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
        },
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            {
              name: "owner_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "users",
            },
          ],
        },
        comments: {
          columns: [
            { name: "text", column_type: { type: "Text" }, nullable: false },
            {
              name: "todo_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "todos",
            },
          ],
        },
      };

      const builderJson = JSON.stringify({
        table: "comments",
        conditions: [],
        includes: {
          todo: {
            table: "todos",
            conditions: [],
            includes: {
              owner: true,
            },
            select: ["id"],
            orderBy: [],
            hops: [],
          },
        },
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, nestedSchema);

      expect(result.array_subqueries).toEqual([
        {
          column_name: "todo",
          table: "todos",
          inner_column: "id",
          outer_column: "comments.todo_id",
          filters: [],
          joins: [],
          select_columns: ["__jazz_include_owner"],
          order_by: [],
          limit: null,
          nested_arrays: [
            {
              column_name: "__jazz_include_owner",
              table: "users",
              inner_column: "id",
              outer_column: "todos.owner_id",
              filters: [],
              joins: [],
              select_columns: null,
              order_by: [],
              limit: null,
              nested_arrays: [],
            },
          ],
        },
      ]);
    });
  });

  describe("self-referential relations", () => {
    const selfRefSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          {
            name: "parent_id",
            column_type: { type: "Uuid" },
            nullable: true,
            references: "todos",
          },
        ],
      },
    };

    it("translates forward self-reference", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: { parent: true },
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, selfRefSchema);

      expect(result.array_subqueries).toEqual([
        {
          column_name: "parent",
          table: "todos",
          inner_column: "id",
          outer_column: "todos.parent_id",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          nested_arrays: [],
        },
      ]);
    });

    it("translates reverse self-reference", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: { todosViaParent: true },
        orderBy: [],
      });

      const result = parseTranslatedQuery(builderJson, selfRefSchema);

      expect(result.array_subqueries).toEqual([
        {
          column_name: "todosViaParent",
          table: "todos",
          inner_column: "parent_id",
          outer_column: "todos.id",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          nested_arrays: [],
        },
      ]);
    });
  });

  describe("full query translation", () => {
    const fullSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
          { name: "priority", column_type: { type: "Integer" }, nullable: true },
          {
            name: "owner_id",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "users",
          },
        ],
      },
      users: {
        columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
      },
    };

    it("translates complex query", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [
          { column: "done", op: "eq", value: false },
          { column: "priority", op: "gte", value: 3 },
        ],
        includes: { owner: true },
        orderBy: [
          ["priority", "desc"],
          ["title", "asc"],
        ],
        limit: 10,
        offset: 5,
      });

      const result = parseTranslatedQuery(builderJson, fullSchema);

      expect(result).toMatchObject({
        table: "todos",
        array_subqueries: [
          {
            column_name: "owner",
            table: "users",
            inner_column: "id",
            outer_column: "todos.owner_id",
            filters: [],
            joins: [],
            select_columns: null,
            order_by: [],
            limit: null,
            nested_arrays: [],
          },
        ],
      });
      expect(result.branches).toBeUndefined();
      expect(result.disjuncts).toBeUndefined();
      expect(result.order_by).toBeUndefined();
      expect(result.offset).toBeUndefined();
      expect(result.limit).toBeUndefined();
      expect(result.include_deleted).toBeUndefined();
      expect(result.joins).toBeUndefined();

      expect(result.relation_ir?.type).toBe("Limit");
      if (result.relation_ir?.type !== "Limit") {
        throw new Error("Expected top-level relation_ir Limit node.");
      }
      expect(result.relation_ir.limit).toBe(10);
      expect(result.relation_ir.input.type).toBe("Offset");
      if (result.relation_ir.input.type !== "Offset") {
        throw new Error("Expected relation_ir Offset input node.");
      }
      expect(result.relation_ir.input.offset).toBe(5);
      expect(result.relation_ir.input.input.type).toBe("OrderBy");
      if (result.relation_ir.input.input.type !== "OrderBy") {
        throw new Error("Expected relation_ir OrderBy input node.");
      }
      expect(result.relation_ir.input.input.terms).toEqual([
        { column: { column: "priority" }, direction: "Desc" },
        { column: { column: "title" }, direction: "Asc" },
      ]);
    });
  });

  it("keeps gather semantics in relation_ir payload", () => {
    const schema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          {
            name: "parent_id",
            column_type: { type: "Uuid" },
            nullable: true,
            references: "todos",
          },
        ],
      },
    };

    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [],
      gather: {
        max_depth: 10,
        step_table: "todos",
        step_current_column: "id",
        step_conditions: [],
        step_hops: ["parent"],
      },
    });

    const result = parseTranslatedQuery(builderJson, schema);
    expect(result.recursive).toBeUndefined();
    expect(result.joins).toBeUndefined();
    expect(result.relation_ir?.type).toBe("Gather");
  });

  it("keeps hop semantics in relation_ir payload", () => {
    const schema: WasmSchema = {
      teams: {
        columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
      },
      team_edges: {
        columns: [
          {
            name: "child_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
          {
            name: "parent_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
        ],
      },
    };

    const builderJson = JSON.stringify({
      table: "team_edges",
      conditions: [
        { column: "child_team", op: "eq", value: "00000000-0000-0000-0000-000000000001" },
      ],
      includes: {},
      orderBy: [],
      hops: ["parent_team"],
    });

    const result = parseTranslatedQuery(builderJson, schema);
    expect(result.joins).toBeUndefined();
    expect(result.result_element_index).toBeUndefined();
    expect(result.recursive).toBeUndefined();
    expect(result.relation_ir?.type).toBe("Project");
  });

  it("keeps multi-hop semantics in relation_ir payload", () => {
    const schema: WasmSchema = {
      users: {
        columns: [
          { name: "name", column_type: { type: "Text" }, nullable: false },
          { name: "team_id", column_type: { type: "Uuid" }, nullable: true, references: "teams" },
        ],
      },
      teams: {
        columns: [
          { name: "name", column_type: { type: "Text" }, nullable: false },
          { name: "org_id", column_type: { type: "Uuid" }, nullable: true, references: "orgs" },
        ],
      },
      orgs: {
        columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
      },
    };

    const builderJson = JSON.stringify({
      table: "users",
      conditions: [],
      includes: {},
      orderBy: [],
      hops: ["team", "org"],
    });

    const result = parseTranslatedQuery(builderJson, schema);
    expect(result.joins).toBeUndefined();
    expect(result.result_element_index).toBeUndefined();
    expect(result.recursive).toBeUndefined();
    expect(result.relation_ir?.type).toBe("Project");
  });

  it("lowers hop metadata to relation IR join + project", () => {
    const schema: WasmSchema = {
      teams: {
        columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
      },
      team_edges: {
        columns: [
          {
            name: "child_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
          {
            name: "parent_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
        ],
      },
    };

    const builderJson = JSON.stringify({
      table: "team_edges",
      conditions: [
        { column: "child_team", op: "eq", value: "00000000-0000-0000-0000-000000000001" },
      ],
      includes: {},
      orderBy: [],
      hops: ["parent_team"],
    });

    const ir = toLegacyRelExprForTest(translateBuilderToRelationIr(builderJson, schema));
    expect(ir.type).toBe("Project");
    if (ir.type !== "Project") {
      throw new Error("Expected project relation IR.");
    }
    expect(ir.input.type).toBe("Join");
    if (ir.input.type !== "Join") {
      throw new Error("Expected join input relation IR.");
    }
    expect(ir.input.on).toEqual([
      {
        left: { scope: "team_edges", column: "parent_team" },
        right: { scope: "__hop_0", column: "id" },
      },
    ]);
    expect(ir.columns[0]).toEqual({
      alias: "id",
      expr: { type: "Column", column: { scope: "__hop_0", column: "id" } },
    });
  });

  it("lowers gather metadata to relation IR gather node", () => {
    const schema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          {
            name: "parent_id",
            column_type: { type: "Uuid" },
            nullable: true,
            references: "todos",
          },
        ],
      },
    };

    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [{ column: "title", op: "ne", value: "archived" }],
      includes: {},
      orderBy: [],
      gather: {
        max_depth: 10,
        step_table: "todos",
        step_current_column: "id",
        step_conditions: [],
        step_hops: ["parent"],
      },
    });

    const ir = toLegacyRelExprForTest(translateBuilderToRelationIr(builderJson, schema));
    expect(ir.type).toBe("Gather");
    if (ir.type !== "Gather") {
      throw new Error("Expected gather relation IR.");
    }
    expect(ir.frontierKey).toEqual({ type: "RowId", source: "Current" });
    expect(ir.step.type).toBe("Project");
    if (ir.step.type !== "Project") {
      throw new Error("Expected gather step project relation.");
    }
    expect(ir.step.input.type).toBe("Join");
    if (ir.step.input.type !== "Join") {
      throw new Error("Expected gather step join relation.");
    }
    expect(ir.step.input.left.type).toBe("Filter");
    if (ir.step.input.left.type !== "Filter") {
      throw new Error("Expected gather step filter relation.");
    }
    expect(ir.step.input.left.predicate).toEqual({
      type: "Cmp",
      left: { scope: "todos", column: "id" },
      op: "Eq",
      right: { type: "RowId", source: "Frontier" },
    });
  });

  it("lowers gather metadata seeded from a hop relation", () => {
    const schema: WasmSchema = {
      teams: {
        columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
      },
      team_edges: {
        columns: [
          {
            name: "child_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
          {
            name: "parent_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
        ],
      },
    };

    const builderJson = JSON.stringify({
      table: "team_edges",
      conditions: [],
      includes: {},
      orderBy: [],
      gather: {
        seed: {
          table: "team_edges",
          conditions: [{ column: "child_team", op: "eq", value: "team-a" }],
          hops: ["parent_team"],
        },
        max_depth: 3,
        step_table: "team_edges",
        step_current_column: "child_team",
        step_conditions: [],
        step_hops: ["parent_team"],
      },
    });

    const ir = toLegacyRelExprForTest(translateBuilderToRelationIr(builderJson, schema));
    expect(ir.type).toBe("Gather");
    if (ir.type !== "Gather") {
      throw new Error("Expected gather relation IR.");
    }
    expect(ir.seed.type).toBe("Project");
    if (ir.seed.type !== "Project") {
      throw new Error("Expected projected seed relation IR.");
    }
    expect(ir.seed.input.type).toBe("Join");
    if (ir.seed.input.type !== "Join") {
      throw new Error("Expected joined seed relation IR.");
    }
    expect(ir.seed.input.left.type).toBe("Filter");
    if (ir.seed.input.left.type !== "Filter") {
      throw new Error("Expected filtered seed relation IR.");
    }
  });

  it("lowers gather metadata seeded from a union relation", () => {
    const schema: WasmSchema = {
      teams: {
        columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
      },
      team_edges: {
        columns: [
          {
            name: "child_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
          {
            name: "parent_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
        ],
      },
    };

    const builderJson = JSON.stringify({
      table: "team_edges",
      conditions: [],
      includes: {},
      orderBy: [],
      gather: {
        seed: {
          union: {
            inputs: [
              {
                table: "team_edges",
                conditions: [{ column: "child_team", op: "eq", value: "team-a" }],
                hops: ["parent_team"],
              },
              {
                table: "teams",
                conditions: [],
                hops: [],
                gather: {
                  seed: {
                    table: "teams",
                    conditions: [{ column: "name", op: "eq", value: "admins" }],
                    hops: [],
                  },
                  max_depth: 2,
                  step_table: "team_edges",
                  step_current_column: "child_team",
                  step_conditions: [],
                  step_hops: ["parent_team"],
                },
              },
            ],
          },
        },
        max_depth: 4,
        step_table: "team_edges",
        step_current_column: "child_team",
        step_conditions: [],
        step_hops: ["parent_team"],
      },
    });

    const ir = toLegacyRelExprForTest(translateBuilderToRelationIr(builderJson, schema));
    expect(ir.type).toBe("Gather");
    if (ir.type !== "Gather") {
      throw new Error("Expected gather relation IR.");
    }
    expect(ir.seed.type).toBe("Union");
    if (ir.seed.type !== "Union") {
      throw new Error("Expected union seed relation IR.");
    }
    expect(ir.seed.inputs).toHaveLength(2);
    expect(ir.seed.inputs[0]?.type).toBe("Project");
    expect(ir.seed.inputs[1]?.type).toBe("Gather");
  });

  describe("error handling", () => {
    it("throws for unknown column", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "unknown", op: "eq", value: "test" }],
      });

      expect(() => translateQuery(builderJson, basicSchema)).toThrow(
        'Unknown column "unknown" in table "todos"',
      );
    });

    it("throws for array value in scalar column", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "title", op: "eq", value: ["test"] }],
      });

      expect(() => translateQuery(builderJson, basicSchema)).toThrow(
        "Unexpected array value for scalar column",
      );
    });

    it("throws when gather step does not use a forward hop", () => {
      const schema: WasmSchema = {
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            {
              name: "parent_id",
              column_type: { type: "Uuid" },
              nullable: true,
              references: "todos",
            },
          ],
        },
      };

      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
        gather: {
          max_depth: 10,
          step_table: "todos",
          step_current_column: "id",
          step_conditions: [],
          step_hops: ["todosViaParent"],
        },
      });

      expect(() => translateQuery(builderJson, schema)).toThrow(
        "gather(...) currently only supports forward hopTo(...) relations.",
      );
    });

    it("throws when gather query also includes include(...)", () => {
      const schema: WasmSchema = {
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            {
              name: "parent_id",
              column_type: { type: "Uuid" },
              nullable: true,
              references: "todos",
            },
          ],
        },
      };

      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: { parent: true },
        orderBy: [],
        gather: {
          max_depth: 10,
          step_table: "todos",
          step_current_column: "id",
          step_conditions: [],
          step_hops: ["parent"],
        },
      });

      expect(() => translateQuery(builderJson, schema)).toThrow(
        "gather(...) does not yet support include(...).",
      );
    });

    it("lowers gather query followed by hopTo(...)", () => {
      const schema: WasmSchema = {
        teams: {
          columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
        },
        team_edges: {
          columns: [
            {
              name: "child_team",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "teams",
            },
            {
              name: "parent_team",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "teams",
            },
          ],
        },
      };

      const builderJson = JSON.stringify({
        table: "teams",
        conditions: [],
        includes: {},
        orderBy: [],
        hops: ["team_edgesViaChild_team"],
        gather: {
          max_depth: 10,
          step_table: "team_edges",
          step_current_column: "child_team",
          step_conditions: [],
          step_hops: ["parent_team"],
        },
      });

      const result = parseTranslatedQuery(builderJson, schema);
      expect(result.relation_ir?.type).toBe("Project");
      if (result.relation_ir?.type !== "Project") {
        throw new Error("Expected projected relation IR.");
      }
      expect(result.relation_ir.input.type).toBe("Join");
      if (result.relation_ir.input.type !== "Join") {
        throw new Error("Expected gather hop join relation IR.");
      }
      expect(result.relation_ir.input.left.type).toBe("Gather");
    });

    it("throws when hop query also includes include(...)", () => {
      const schema: WasmSchema = {
        teams: {
          columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
        },
        team_edges: {
          columns: [
            {
              name: "child_team",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "teams",
            },
            {
              name: "parent_team",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "teams",
            },
          ],
        },
      };

      const builderJson = JSON.stringify({
        table: "team_edges",
        conditions: [],
        includes: { parent_team: true },
        orderBy: [],
        hops: ["parent_team"],
      });

      expect(() => translateQuery(builderJson, schema)).toThrow(
        "hopTo(...) does not yet support include(...).",
      );
    });
  });
});

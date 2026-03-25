import { describe, test, expect } from "bun:test";
import type { IntrospectedTable, IntrospectedIndex, IntrospectedColumn } from "../../src/migration/types";
import { Differ, type DesiredTable } from "../../src/migration/diff";
import { col } from "./helpers";

function makeExisting(
	tables: Record<
		string,
		{
			columns?: (string | Partial<IntrospectedColumn>)[];
			indexes?: IntrospectedIndex[];
			hasData?: boolean;
		}
	>,
) {
	return new Map<string, IntrospectedTable>(
		Object.entries(tables).map(([name, info]) => [
			name,
			{
				columns: new Map(
					(info.columns ?? []).map((raw) => {
						const partial = typeof raw === "string" ? { name: raw } : raw;

						const c: IntrospectedColumn = {
							name: partial.name!,
							type: partial.type ?? "TEXT",
							notnull: partial.notnull ?? false,
							defaultValue: partial.defaultValue ?? null,
							unique: partial.unique ?? false,
							references: partial.references ?? null,
							onDelete: partial.onDelete ?? null,
							hasNulls: partial.hasNulls ?? false,
						};

						return [c.name, c];
					}),
				),
				indexes: info.indexes ?? [],
				hasData: info.hasData ?? false,
			},
		]),
	);
}

describe("diff", () => {
	test("produces CreateTable for all missing tables", () => {
		const desired: DesiredTable[] = [
			{ name: "users", sql: 'CREATE TABLE "users" (id INTEGER PRIMARY KEY)', columns: [] },
			{ name: "posts", sql: 'CREATE TABLE "posts" (id INTEGER PRIMARY KEY)', columns: [] },
		];

		const ops = new Differ(desired, new Map()).diff();

		expect(ops).toEqual([
			{ type: "CreateTable", table: "users", sql: desired[0]!.sql },
			{ type: "CreateTable", table: "posts", sql: desired[1]!.sql },
		]);
	});

	test("produces empty list when all tables and columns match", () => {
		const desired: DesiredTable[] = [
			{ name: "users", sql: "...", columns: [col("id", { type: "INTEGER" })] },
		];

		const ops = new Differ(
			desired,
			makeExisting({ users: { columns: [{ name: "id", type: "INTEGER" }] } }),
		).diff();

		expect(ops).toHaveLength(0);
	});

	test("only produces CreateTable for missing tables", () => {
		const desired: DesiredTable[] = [
			{ name: "users", sql: 'CREATE TABLE "users" (...)', columns: [col("id")] },
			{ name: "posts", sql: 'CREATE TABLE "posts" (...)', columns: [] },
		];

		const ops = new Differ(desired, makeExisting({ users: { columns: ["id"] } })).diff();

		expect(ops).toEqual([{ type: "CreateTable", table: "posts", sql: desired[1]!.sql }]);
	});

	test("produces DropTable for orphan tables", () => {
		const desired: DesiredTable[] = [{ name: "users", sql: "...", columns: [col("id")] }];

		const ops = new Differ(
			desired,
			makeExisting({ users: { columns: ["id"] }, orphan: {} }),
		).diff();

		expect(ops).toEqual([{ type: "DropTable", table: "orphan" }]);
	});

	test("produces DropTable for multiple orphan tables", () => {
		const ops = new Differ([], makeExisting({ orphan1: {}, orphan2: {} })).diff();

		expect(ops).toHaveLength(2);
		expect(ops.every((op) => op.type === "DropTable")).toBe(true);
	});

	test("produces both CreateTable and DropTable", () => {
		const desired: DesiredTable[] = [
			{ name: "users", sql: 'CREATE TABLE "users" (...)', columns: [] },
		];

		const ops = new Differ(desired, makeExisting({ orphan: {} })).diff();

		expect(ops).toHaveLength(2);
		expect(ops[0]).toEqual({ type: "CreateTable", table: "users", sql: desired[0]!.sql });
		expect(ops[1]).toEqual({ type: "DropTable", table: "orphan" });
	});

	test("produces AddColumn for new nullable column", () => {
		const desired: DesiredTable[] = [
			{ name: "users", sql: "...", columns: [col("id"), col("bio", { columnDef: '"bio" TEXT' })] },
		];

		const ops = new Differ(desired, makeExisting({ users: { columns: ["id"] } })).diff();

		expect(ops).toEqual([{ type: "AddColumn", table: "users", columnDef: '"bio" TEXT' }]);
	});

	test("produces AddColumn for new NOT NULL column with DEFAULT", () => {
		const desired: DesiredTable[] = [
			{
				name: "users",
				sql: "...",
				columns: [
					col("id"),
					col("status", {
						notnull: true,
						defaultValue: "'active'",
						columnDef: "\"status\" TEXT NOT NULL DEFAULT 'active'",
					}),
				],
			},
		];

		const ops = new Differ(desired, makeExisting({ users: { columns: ["id"] } })).diff();

		expect(ops).toEqual([
			{ type: "AddColumn", table: "users", columnDef: "\"status\" TEXT NOT NULL DEFAULT 'active'" },
		]);
	});

	test("throws for NOT NULL column without DEFAULT on table with data", () => {
		const desired: DesiredTable[] = [
			{
				name: "users",
				sql: "...",
				columns: [col("id"), col("email", { addable: false, notnull: true })],
			},
		];

		expect(() =>
			new Differ(desired, makeExisting({ users: { columns: ["id"], hasData: true } })).diff(),
		).toThrow(/Cannot add NOT NULL column "email" without DEFAULT to table "users"/);
	});

	test("recreates empty table for NOT NULL column without DEFAULT", () => {
		const desired: DesiredTable[] = [
			{
				name: "users",
				sql: 'CREATE TABLE "users" ("id" TEXT, "email" TEXT NOT NULL)',
				columns: [col("id"), col("email", { addable: false, notnull: true })],
			},
		];

		const ops = new Differ(
			desired,
			makeExisting({ users: { columns: ["id"], hasData: false } }),
		).diff();

		expect(ops).toEqual([
			{ type: "DropTable", table: "users" },
			{ type: "CreateTable", table: "users", sql: desired[0]!.sql },
		]);
	});

	test("produces RebuildTable when column removed", () => {
		const desired: DesiredTable[] = [
			{
				name: "users",
				sql: 'CREATE TABLE IF NOT EXISTS "users" ("id" TEXT, "name" TEXT)',
				columns: [col("id"), col("name")],
			},
		];

		const ops = new Differ(
			desired,
			makeExisting({ users: { columns: ["id", "name", "bio"] } }),
		).diff();

		expect(ops).toEqual([
			{
				type: "RebuildTable",
				table: "users",
				createSql: desired[0]!.sql,
				columnCopies: [
					{ name: "id", expr: '"id"' },
					{ name: "name", expr: '"name"' },
				],
			},
		]);
	});

	test("RebuildTable takes precedence over AddColumn when columns both added and removed", () => {
		const desired: DesiredTable[] = [
			{
				name: "users",
				sql: "...",
				columns: [col("id"), col("email")],
			},
		];

		const ops = new Differ(desired, makeExisting({ users: { columns: ["id", "name"] } })).diff();

		expect(ops).toHaveLength(1);
		expect(ops[0]!.type).toBe("RebuildTable");
	});

	test("produces AddColumn for multiple new columns", () => {
		const desired: DesiredTable[] = [
			{
				name: "users",
				sql: "...",
				columns: [
					col("id"),
					col("bio", { columnDef: '"bio" TEXT' }),
					col("age", { columnDef: '"age" INTEGER', type: "INTEGER" }),
				],
			},
		];

		const ops = new Differ(desired, makeExisting({ users: { columns: ["id"] } })).diff();

		expect(ops).toHaveLength(2);
		expect(ops[0]).toEqual({ type: "AddColumn", table: "users", columnDef: '"bio" TEXT' });
		expect(ops[1]).toEqual({ type: "AddColumn", table: "users", columnDef: '"age" INTEGER' });
	});

	test("detects type change and produces RebuildTable", () => {
		const desired: DesiredTable[] = [
			{ name: "t", sql: "...", columns: [col("id"), col("age", { type: "INTEGER" })] },
		];

		const ops = new Differ(
			desired,
			makeExisting({ t: { columns: [{ name: "id" }, { name: "age", type: "TEXT" }] } }),
		).diff();

		expect(ops[0]!.type).toBe("RebuildTable");
	});

	test("detects nullability change and produces RebuildTable", () => {
		const desired: DesiredTable[] = [
			{ name: "t", sql: "...", columns: [col("id"), col("name", { notnull: true })] },
		];

		const ops = new Differ(
			desired,
			makeExisting({ t: { columns: [{ name: "id" }, { name: "name", notnull: false }] } }),
		).diff();

		expect(ops[0]!.type).toBe("RebuildTable");
	});

	test("detects DEFAULT change and produces RebuildTable", () => {
		const desired: DesiredTable[] = [
			{ name: "t", sql: "...", columns: [col("id"), col("status", { defaultValue: "'active'" })] },
		];

		const ops = new Differ(
			desired,
			makeExisting({ t: { columns: [{ name: "id" }, { name: "status", defaultValue: null }] } }),
		).diff();

		expect(ops[0]!.type).toBe("RebuildTable");
	});

	test("detects UNIQUE change and produces RebuildTable", () => {
		const desired: DesiredTable[] = [
			{ name: "t", sql: "...", columns: [col("id"), col("email", { unique: true })] },
		];

		const ops = new Differ(
			desired,
			makeExisting({ t: { columns: [{ name: "id" }, { name: "email", unique: false }] } }),
		).diff();

		expect(ops[0]!.type).toBe("RebuildTable");
	});

	test("detects FK change and produces RebuildTable", () => {
		const desired: DesiredTable[] = [
			{ name: "t", sql: "...", columns: [col("id"), col("user_id", { references: "users.id" })] },
		];

		const ops = new Differ(
			desired,
			makeExisting({ t: { columns: [{ name: "id" }, { name: "user_id", references: null }] } }),
		).diff();

		expect(ops[0]!.type).toBe("RebuildTable");
	});

	test("detects onDelete change and produces RebuildTable", () => {
		const desired: DesiredTable[] = [
			{
				name: "t",
				sql: "...",
				columns: [col("id"), col("user_id", { references: "users.id", onDelete: "CASCADE" })],
			},
		];

		const existing = makeExisting({
			t: { columns: [{ name: "id" }, { name: "user_id", references: "users.id", onDelete: null }] },
		});
		const ops = new Differ(desired, existing).diff();

		expect(ops[0]!.type).toBe("RebuildTable");
	});

	test("type changed nullable column excluded from columnCopies", () => {
		const desired: DesiredTable[] = [
			{ name: "t", sql: "...", columns: [col("id"), col("val", { type: "INTEGER" })] },
		];

		const existing = makeExisting({
			t: { columns: [{ name: "id" }, { name: "val", type: "TEXT" }] },
		});
		const ops = new Differ(desired, existing).diff();

		expect(ops).toEqual([
			{
				type: "RebuildTable",
				table: "t",
				createSql: "...",
				columnCopies: [{ name: "id", expr: '"id"' }],
			},
		]);
	});

	test("type changed NOT NULL with DEFAULT excluded from columnCopies", () => {
		const desired: DesiredTable[] = [
			{
				name: "t",
				sql: "...",
				columns: [col("id"), col("val", { type: "INTEGER", notnull: true, defaultValue: "0" })],
			},
		];

		const existing = makeExisting({
			t: { columns: [{ name: "id" }, { name: "val", type: "TEXT" }] },
		});
		const ops = new Differ(desired, existing).diff();

		expect(ops).toEqual([
			{
				type: "RebuildTable",
				table: "t",
				createSql: "...",
				columnCopies: [{ name: "id", expr: '"id"' }],
			},
		]);
	});

	test("type changed NOT NULL without DEFAULT throws on table with data", () => {
		const desired: DesiredTable[] = [
			{
				name: "t",
				sql: "...",
				columns: [col("id"), col("val", { type: "INTEGER", notnull: true })],
			},
		];

		const existing = makeExisting({
			t: { columns: [{ name: "id" }, { name: "val", type: "TEXT" }], hasData: true },
		});

		expect(() => new Differ(desired, existing).diff()).toThrow(
			/Cannot change type of NOT NULL column "val"/,
		);
	});

	test("nullable to required with DEFAULT uses COALESCE", () => {
		const desired: DesiredTable[] = [
			{
				name: "t",
				sql: "...",
				columns: [col("id"), col("status", { notnull: true, defaultValue: "'active'" })],
			},
		];

		const existing = makeExisting({
			t: { columns: [{ name: "id" }, { name: "status", notnull: false }] },
		});
		const ops = new Differ(desired, existing).diff();

		expect(ops).toEqual([
			{
				type: "RebuildTable",
				table: "t",
				createSql: "...",
				columnCopies: [
					{ name: "id", expr: '"id"' },
					{ name: "status", expr: "COALESCE(\"status\", 'active')" },
				],
			},
		]);
	});

	test("required to optional copies normally", () => {
		const desired: DesiredTable[] = [
			{ name: "t", sql: "...", columns: [col("id"), col("name", { notnull: false })] },
		];

		const existing = makeExisting({
			t: { columns: [{ name: "id" }, { name: "name", notnull: true }] },
		});
		const ops = new Differ(desired, existing).diff();

		expect(ops).toEqual([
			{
				type: "RebuildTable",
				table: "t",
				createSql: "...",
				columnCopies: [
					{ name: "id", expr: '"id"' },
					{ name: "name", expr: '"name"' },
				],
			},
		]);
	});

	test("multiple changes in same table produce single RebuildTable", () => {
		const desired: DesiredTable[] = [
			{
				name: "t",
				sql: "...",
				columns: [col("id"), col("name", { notnull: true }), col("email", { unique: true })],
			},
		];

		const existing = makeExisting({
			t: {
				columns: [
					{ name: "id" },
					{ name: "name", notnull: false },
					{ name: "email", unique: false },
				],
			},
		});
		const ops = new Differ(desired, existing).diff();

		expect(ops).toHaveLength(1);
		expect(ops[0]!.type).toBe("RebuildTable");
	});
});

describe("diff indexes", () => {
	test("produces CreateIndex for new index on existing table", () => {
		const desired: DesiredTable[] = [
			{
				name: "users",
				sql: "...",
				columns: [col("id"), col("name")],
				indexes: [
					{ name: "ix_users_name", columns: ["name"], sql: 'CREATE INDEX "ix_users_name" ON "users" ("name")' },
				],
			},
		];

		const ops = new Differ(desired, makeExisting({ users: { columns: ["id", "name"] } })).diff();

		expect(ops).toEqual([
			{ type: "CreateIndex", table: "users", columns: ["name"], sql: 'CREATE INDEX "ix_users_name" ON "users" ("name")' },
		]);
	});

	test("produces DropIndex for removed index", () => {
		const desired: DesiredTable[] = [
			{ name: "users", sql: "...", columns: [col("id"), col("name")] },
		];

		const existing = makeExisting({
			users: {
				columns: ["id", "name"],
				indexes: [{ name: "ix_users_name", columns: ["name"], unique: false }],
			},
		});
		const ops = new Differ(desired, existing).diff();

		expect(ops).toEqual([{ type: "DropIndex", index: "ix_users_name" }]);
	});

	test("no index ops when indexes match", () => {
		const desired: DesiredTable[] = [
			{
				name: "users",
				sql: "...",
				columns: [col("id"), col("name")],
				indexes: [{ name: "ix_users_name", columns: ["name"], sql: "..." }],
			},
		];

		const existing = makeExisting({
			users: {
				columns: ["id", "name"],
				indexes: [{ name: "ix_users_name", columns: ["name"], unique: false }],
			},
		});
		const ops = new Differ(desired, existing).diff();

		expect(ops).toHaveLength(0);
	});

	test("produces CreateIndex for all indexes on new table", () => {
		const desired: DesiredTable[] = [
			{
				name: "users",
				sql: 'CREATE TABLE "users" ("id" TEXT, "name" TEXT)',
				columns: [],
				indexes: [
					{ name: "ix_users_name", columns: ["name"], sql: 'CREATE INDEX "ix_users_name" ON "users" ("name")' },
				],
			},
		];

		const ops = new Differ(desired, new Map()).diff();

		expect(ops).toHaveLength(2);
		expect(ops[0]!.type).toBe("CreateTable");
		expect(ops[1]).toEqual({
			type: "CreateIndex",
			table: "users",
			columns: ["name"],
			sql: 'CREATE INDEX "ix_users_name" ON "users" ("name")',
		});
	});

	test("produces CreateIndex for all indexes on rebuilt table", () => {
		const desired: DesiredTable[] = [
			{
				name: "users",
				sql: 'CREATE TABLE IF NOT EXISTS "users" ("id" TEXT, "name" TEXT)',
				columns: [col("id"), col("name")],
				indexes: [
					{ name: "ix_users_name", columns: ["name"], sql: 'CREATE INDEX "ix_users_name" ON "users" ("name")' },
				],
			},
		];

		const existing = makeExisting({ users: { columns: ["id", "name", "bio"] } });
		const ops = new Differ(desired, existing).diff();

		expect(ops).toHaveLength(2);
		expect(ops[0]!.type).toBe("RebuildTable");
		expect(ops[1]).toEqual({
			type: "CreateIndex",
			table: "users",
			columns: ["name"],
			sql: 'CREATE INDEX "ix_users_name" ON "users" ("name")',
		});
	});

	test("does not produce DropIndex for indexes on rebuilt table", () => {
		const desired: DesiredTable[] = [
			{
				name: "users",
				sql: 'CREATE TABLE IF NOT EXISTS "users" ("id" TEXT, "name" TEXT)',
				columns: [col("id"), col("name")],
			},
		];

		const existing = makeExisting({
			users: {
				columns: ["id", "name", "bio"],
				indexes: [{ name: "ix_users_bio", columns: ["bio"], unique: false }],
			},
		});
		const ops = new Differ(desired, existing).diff();

		expect(ops).toHaveLength(1);
		expect(ops[0]!.type).toBe("RebuildTable");
	});

	test("produces both CreateIndex and DropIndex when index changed", () => {
		const desired: DesiredTable[] = [
			{
				name: "users",
				sql: "...",
				columns: [col("id"), col("name"), col("email")],
				indexes: [
					{ name: "ix_users_email", columns: ["email"], sql: 'CREATE INDEX "ix_users_email" ON "users" ("email")' },
				],
			},
		];

		const existing = makeExisting({
			users: {
				columns: ["id", "name", "email"],
				indexes: [{ name: "ix_users_name", columns: ["name"], unique: false }],
			},
		});
		const ops = new Differ(desired, existing).diff();

		expect(ops).toEqual([
			{ type: "CreateIndex", table: "users", columns: ["email"], sql: 'CREATE INDEX "ix_users_email" ON "users" ("email")' },
			{ type: "DropIndex", index: "ix_users_name" },
		]);
	});
});

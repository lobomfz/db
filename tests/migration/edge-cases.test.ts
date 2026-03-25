import { describe, test, expect } from "bun:test";
import { Database as BunDatabase } from "bun:sqlite";
import { Introspector } from "../../src/migration/introspect";
import { Differ, type DesiredTable } from "../../src/migration/diff";
import { Executor } from "../../src/migration/execute";
import { col } from "./helpers";

describe("nullable to NOT NULL without default", () => {
	test("throws when column has NULLs", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "bio" TEXT)');
		db.run("INSERT INTO \"users\" VALUES (1, 'has bio')");
		db.run("INSERT INTO \"users\" VALUES (2, NULL)");

		const desired: DesiredTable[] = [
			{
				name: "users",
				sql: 'CREATE TABLE IF NOT EXISTS "users" ("id" INTEGER PRIMARY KEY, "bio" TEXT NOT NULL)',
				columns: [col("id", { type: "INTEGER" }), col("bio", { type: "TEXT", notnull: true })],
			},
		];

		const existing = new Introspector(db).introspect();

		expect(() => new Differ(desired, existing).diff()).toThrow(
			/Cannot make column "bio" NOT NULL without DEFAULT in table "users" with existing data/,
		);
	});

	test("succeeds when all rows have the column filled", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "bio" TEXT)');
		db.run("INSERT INTO \"users\" VALUES (1, 'bio A')");
		db.run("INSERT INTO \"users\" VALUES (2, 'bio B')");

		const desired: DesiredTable[] = [
			{
				name: "users",
				sql: 'CREATE TABLE IF NOT EXISTS "users" ("id" INTEGER PRIMARY KEY, "bio" TEXT NOT NULL)',
				columns: [col("id", { type: "INTEGER" }), col("bio", { type: "TEXT", notnull: true })],
			},
		];

		const existing = new Introspector(db).introspect();
		const ops = new Differ(desired, existing).diff();

		new Executor(db, ops).execute();

		const rows = db.prepare('SELECT * FROM "users" ORDER BY id').all() as {
			id: number;
			bio: string;
		}[];

		expect(rows).toEqual([
			{ id: 1, bio: "bio A" },
			{ id: 2, bio: "bio B" },
		]);
	});

	test("succeeds on empty table", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "bio" TEXT)');

		const desired: DesiredTable[] = [
			{
				name: "users",
				sql: 'CREATE TABLE IF NOT EXISTS "users" ("id" INTEGER PRIMARY KEY, "bio" TEXT NOT NULL)',
				columns: [col("id", { type: "INTEGER" }), col("bio", { type: "TEXT", notnull: true })],
			},
		];

		const existing = new Introspector(db).introspect();
		const ops = new Differ(desired, existing).diff();

		expect(ops).toHaveLength(1);
		expect(ops[0]!.type).toBe("RebuildTable");

		new Executor(db, ops).execute();

		const table = new Introspector(db).introspect().get("users")!;

		expect(table.columns.get("bio")!.notnull).toBe(true);
	});
});

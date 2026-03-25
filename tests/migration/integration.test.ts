import { describe, test, expect } from "bun:test";
import { Database as BunDatabase } from "bun:sqlite";
import { type } from "arktype";
import { Introspector } from "../../src/migration/introspect";
import { Executor } from "../../src/migration/execute";
import { Database, generated } from "../../src/index.ts";

describe("migration integration", () => {
	test("creates tables on fresh database", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					users: type({
						id: generated("autoincrement"),
						name: "string",
					}),
					posts: type({
						id: generated("autoincrement"),
						title: "string",
					}),
				},
			},
		});

		const tables = await db.kysely
			.selectFrom("sqlite_master")
			.where("type", "=", "table")
			.where("name", "not like", "sqlite_%")
			.select("name")
			.execute();

		const names = tables.map((t) => t.name);

		expect(names).toContain("users");
		expect(names).toContain("posts");
	});

	test("no-ops when schema matches existing database", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					users: type({
						id: generated("autoincrement"),
						name: "string",
					}),
				},
			},
		});

		await db.kysely.insertInto("users").values({ name: "Alice" }).execute();

		(db as any).migrate();

		const users = await db.kysely.selectFrom("users").selectAll().execute();

		expect(users).toHaveLength(1);
		expect(users[0]!.name).toBe("Alice");
	});

	test("creates indexes on fresh database", () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					users: type({
						id: generated("autoincrement"),
						name: "string",
						email: "string",
					}),
				},
				indexes: {
					users: [{ columns: ["name"] }, { columns: ["email"], unique: true }],
				},
			},
		});

		const sqlite = (db as any).sqlite as BunDatabase;
		const indexes = new Introspector(sqlite).introspect().get("users")!.indexes;

		expect(indexes).toHaveLength(2);
		expect(indexes.find((i) => i.name === "ix_users_name")).toBeTruthy();
		expect(indexes.find((i) => i.name === "ux_users_email")).toBeTruthy();
	});

	test("migrate is no-op on existing database with generated('now')", () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					events: type({
						id: generated("autoincrement"),
						name: "string",
						created_at: generated("now"),
					}),
				},
			},
		});

		const sqlite = (db as any).sqlite as BunDatabase;
		const before = (
			sqlite.prepare("SELECT rootpage FROM sqlite_master WHERE name = 'events'").get() as {
				rootpage: number;
			}
		).rootpage;

		(db as any).migrate();

		const after = (
			sqlite.prepare("SELECT rootpage FROM sqlite_master WHERE name = 'events'").get() as {
				rootpage: number;
			}
		).rootpage;

		expect(after).toBe(before);
	});

	test("composite index is created correctly", () => {
		const sqlite = new BunDatabase(":memory:");
		sqlite.run('CREATE TABLE "events" ("id" INTEGER PRIMARY KEY, "type" TEXT, "date" INTEGER)');

		new Executor(sqlite, [
			{
				type: "CreateIndex",
				sql: 'CREATE INDEX "ix_events_type_date" ON "events" ("type", "date")',
			},
		]).execute();

		const events = new Introspector(sqlite).introspect().get("events")!;

		expect(events.indexes).toHaveLength(1);
		expect(events.indexes[0]!.name).toBe("ix_events_type_date");
		expect(events.indexes[0]!.columns).toEqual(["type", "date"]);
	});
});

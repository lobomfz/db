import { describe, test, expect } from "bun:test";

import { type } from "arktype";

import { Database, generated } from "../src/index.js";

describe("generated", () => {
	test("generated autoincrement creates primary key", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						name: "string",
					}),
				},
			},
		});

		const tableInfo = await db.kysely
			.selectFrom("sqlite_master")
			.where("type", "=", "table")
			.where("name", "=", "items")
			.select("sql")
			.executeTakeFirst();

		expect(tableInfo?.sql).toContain("PRIMARY KEY AUTOINCREMENT");
	});

	test("generated now creates default unixepoch", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						created_at: generated("now"),
					}),
				},
			},
		});

		const tableInfo = await db.kysely
			.selectFrom("sqlite_master")
			.where("type", "=", "table")
			.where("name", "=", "items")
			.select("sql")
			.executeTakeFirst();

		expect(tableInfo?.sql).toContain("DEFAULT (unixepoch())");
	});

	test("arktype default can be overridden on insert", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						status: type("string").default("pending"),
						name: "string",
					}),
				},
			},
		});

		await db.kysely.insertInto("items").values({ name: "Test", status: "active" }).execute();

		const item = await db.kysely.selectFrom("items").selectAll().executeTakeFirst();

		expect(item?.status).toBe("active");
	});

	test("bulk insert with optional date and boolean columns", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					events: type({
						id: generated("autoincrement"),
						name: "string",
						"active?": "boolean",
						"happened_at?": "Date",
					}),
				},
			},
		});

		const date = new Date("2025-06-15T12:00:00Z");

		await db.kysely
			.insertInto("events")
			.values([
				{ name: "with both", active: true, happened_at: date },
				{ name: "with boolean", active: false },
				{ name: "plain" },
			])
			.execute();

		const rows = await db.kysely.selectFrom("events as e").selectAll("e").orderBy("e.id").execute();

		expect(rows[0]!.active).toBe(true);
		expect(rows[0]!.happened_at).toBeInstanceOf(Date);
		expect(rows[0]!.happened_at!.getTime()).toBe(date.getTime());
		expect(rows[1]!.active).toBe(false);
		expect(rows[1]!.happened_at).toBeNull();
		expect(rows[2]!.active).toBeNull();
		expect(rows[2]!.happened_at).toBeNull();
	});

	test("arktype boolean default", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						name: "string",
						active: type("boolean").default(true),
					}),
				},
			},
		});

		await db.kysely.insertInto("items").values({ name: "Test" }).execute();

		const item = await db.kysely.selectFrom("items").selectAll().executeTakeFirst();

		expect(item?.active).toBe(true);
	});
});

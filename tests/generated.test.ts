import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { Database, generated } from "../src/index.ts";

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

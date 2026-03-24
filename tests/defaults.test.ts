import { describe, test, expect, beforeEach } from "bun:test";
import { type } from "arktype";
import { Database, generated } from "../src/index.ts";

describe("defaults", () => {
	test("default string value", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						name: "string",
						status: type("string").default("pending"),
					}),
				},
			},
		});

		await db.kysely.insertInto("items").values({ name: "Test" }).execute();

		const item = await db.kysely.selectFrom("items").selectAll().executeTakeFirst();

		expect(item?.status).toBe("pending");
	});

	test("default now returns Date", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						name: "string",
						created_at: generated("now"),
					}),
				},
			},
		});

		const before = Date.now();
		await db.kysely.insertInto("items").values({ name: "Test" }).execute();
		const after = Date.now();

		const item = await db.kysely.selectFrom("items").selectAll().executeTakeFirst();

		expect(item?.created_at).toBeInstanceOf(Date);
		expect(item!.created_at.getTime()).toBeGreaterThanOrEqual(before - 1000);
		expect(item!.created_at.getTime()).toBeLessThanOrEqual(after + 1000);
	});

	test("date where filter", async () => {
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

		const sqlite = (db as any).sqlite;
		const old = Math.floor(Date.now() / 1000) - 3600;
		const recent = Math.floor(Date.now() / 1000);
		sqlite.run("INSERT INTO events (name, created_at) VALUES (?, ?)", ["old", old]);
		sqlite.run("INSERT INTO events (name, created_at) VALUES (?, ?)", ["recent", recent]);

		const cutoff = new Date((old + 1800) * 1000);

		const results = await db.kysely
			.selectFrom("events")
			.where("created_at", ">", cutoff)
			.selectAll()
			.execute();

		expect(results).toHaveLength(1);
		expect(results[0]!.name).toBe("recent");
		expect(results[0]!.created_at).toBeInstanceOf(Date);
	});

	test("date update roundtrip", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						name: "string",
						updated_at: generated("now"),
					}),
				},
			},
		});

		await db.kysely.insertInto("items").values({ name: "Test" }).execute();

		const newDate = new Date("2025-06-15T12:00:00Z");

		await db.kysely.updateTable("items").set({ updated_at: newDate }).where("id", "=", 1).execute();

		const item = await db.kysely.selectFrom("items").selectAll().executeTakeFirstOrThrow();

		expect(item.updated_at).toBeInstanceOf(Date);
		expect(item.updated_at.getTime()).toBe(newDate.getTime());
	});

	test("manual Date insert roundtrip", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					events: type({
						id: generated("autoincrement"),
						name: "string",
						happened_at: "Date",
					}),
				},
			},
		});

		const date = new Date("2025-06-15T12:00:00Z");

		await db.kysely.insertInto("events").values({ name: "Test", happened_at: date }).execute();

		const event = await db.kysely.selectFrom("events").selectAll().executeTakeFirstOrThrow();

		expect(event.happened_at).toBeInstanceOf(Date);
		expect(event.happened_at.getTime()).toBe(date.getTime());
	});

	test("default number value", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						name: "string",
						count: type("number").default(0),
					}),
				},
			},
		});

		await db.kysely.insertInto("items").values({ name: "Test" }).execute();

		const item = await db.kysely.selectFrom("items").selectAll().executeTakeFirst();

		expect(item?.count).toBe(0);
	});

	test("boolean default generates DEFAULT in DDL", () => {
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

		const sqlite = (db as any).sqlite;
		const ddl = sqlite.query("SELECT sql FROM sqlite_master WHERE name = 'items'").get()
			?.sql as string;

		expect(ddl).toContain('"active" INTEGER DEFAULT true');
	});

	describe("nullable columns", () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						name: "string",
						deleted_at: "string | null",
					}),
				},
			},
		});

		beforeEach(() => {
			(db as any).sqlite.run("DELETE FROM items");
			(db as any).sqlite.run("DELETE FROM sqlite_sequence WHERE name = 'items'");
		});

		test("omits NOT NULL", () => {
			const sqlite = (db as any).sqlite;
			const sql = sqlite.query("SELECT sql FROM sqlite_master WHERE name = 'items'").get()
				?.sql as string;

			expect(sql).toContain('"deleted_at" TEXT');
			expect(sql).not.toContain('"deleted_at" TEXT NOT NULL');
		});

		test("defaults to null on insert", async () => {
			await db.kysely.insertInto("items").values({ name: "Test" }).execute();

			const item = await db.kysely.selectFrom("items").selectAll().executeTakeFirst();

			expect(item?.deleted_at).toBeNull();
		});

		test("accepts string value", async () => {
			await db.kysely
				.insertInto("items")
				.values({ name: "Test", deleted_at: "2026-01-15T12:00:00Z" })
				.execute();

			const item = await db.kysely.selectFrom("items").selectAll().executeTakeFirst();

			expect(item?.deleted_at).toBe("2026-01-15T12:00:00Z");
		});

		test("can be updated to null", async () => {
			await db.kysely
				.insertInto("items")
				.values({ name: "Test", deleted_at: "2026-01-15T12:00:00Z" })
				.execute();
			await db.kysely
				.updateTable("items")
				.set({ deleted_at: null })
				.where("id", "=", 1)
				.execute();

			const item = await db.kysely.selectFrom("items").selectAll().executeTakeFirst();

			expect(item?.deleted_at).toBeNull();
		});

		test("default null generates DEFAULT NULL not DEFAULT 'null'", () => {
			const nullDb = new Database({
				path: ":memory:",
				schema: {
					tables: {
						items: type({
							id: generated("autoincrement"),
							name: "string",
							deleted_at: type("string | null").default(null),
						}),
					},
				},
			});

			const sqlite = (nullDb as any).sqlite;
			const sql = sqlite.query("SELECT sql FROM sqlite_master WHERE name = 'items'").get()
				?.sql as string;

			expect(sql).toContain("DEFAULT NULL");
			expect(sql).not.toContain("DEFAULT 'null'");
		});
	});
});

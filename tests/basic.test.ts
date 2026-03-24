import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { Database } from "../src/index.ts";

describe("basic", () => {
	test("creates tables from schemas", () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					users: type({
						id: type("number.integer").configure({ primaryKey: true }),
						name: "string",
					}),
				},
			},
		});

		const tables = db.kysely
			.selectFrom("sqlite_master" as any)
			.where("type", "=", "table")
			.where("name", "=", "users")
			.selectAll()
			.executeTakeFirst();

		expect(tables).resolves.toBeDefined();
	});

	test("kysely client is typed", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					users: type({
						id: type("number.integer").configure({ primaryKey: true }),
						name: "string",
						"bio?": "string",
					}),
				},
			},
		});

		await db.kysely.insertInto("users").values({ id: 1, name: "John" }).execute();

		const user = await db.kysely
			.selectFrom("users")
			.where("id", "=", 1)
			.selectAll()
			.executeTakeFirst();

		expect(user).toEqual({ id: 1, name: "John", bio: null });
	});

	test("reset clears all tables", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					users: type({
						id: type("number.integer").configure({ primaryKey: true }),
						name: "string",
					}),
					items: type({
						id: type("number.integer").configure({ primaryKey: true }),
						name: "string",
					}),
				},
			},
		});

		await db.kysely.insertInto("users").values({ id: 1, name: "John" }).execute();
		await db.kysely.insertInto("items").values({ id: 1, name: "Item" }).execute();

		db.reset();

		const users = await db.kysely.selectFrom("users").selectAll().execute();
		const items = await db.kysely.selectFrom("items").selectAll().execute();

		expect(users).toEqual([]);
		expect(items).toEqual([]);
	});

	test("reset clears single table", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					users: type({
						id: type("number.integer").configure({ primaryKey: true }),
						name: "string",
					}),
					items: type({
						id: type("number.integer").configure({ primaryKey: true }),
						name: "string",
					}),
				},
			},
		});

		await db.kysely.insertInto("users").values({ id: 1, name: "John" }).execute();
		await db.kysely.insertInto("items").values({ id: 1, name: "Item" }).execute();

		db.reset("users");

		const users = await db.kysely.selectFrom("users").selectAll().execute();
		const items = await db.kysely.selectFrom("items").selectAll().execute();

		expect(users).toEqual([]);
		expect(items).toEqual([{ id: 1, name: "Item" }]);
	});
});

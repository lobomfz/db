import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { Database, generated } from "../src/index.js";

describe("basic", () => {
	test("creates tables from schemas", async () => {
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

		const tables = await db.kysely
			.selectFrom("sqlite_master")
			.where("type", "=", "table")
			.where("name", "=", "users")
			.selectAll()
			.executeTakeFirst();

		expect(tables).toBeDefined();
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

	test("optional non-null field strips null on insert", async () => {
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

		await db.kysely
			.insertInto("users")
			.values({ id: 1, name: "John", bio: null as any })
			.execute();

		const user = await db.kysely
			.selectFrom("users")
			.where("id", "=", 1)
			.selectAll()
			.executeTakeFirstOrThrow();

		expect(user.bio).toBeNull();
	});

	test("optional field can be updated to null", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					users: type({
						id: generated("autoincrement"),
						"name?": "string",
					}),
				},
			},
		});

		const user = await db.kysely
			.insertInto("users")
			.values({ id: 123 as any, name: "John" })
			.returningAll()
			.executeTakeFirstOrThrow();

		await db.kysely
			.updateTable("users")
			.set({ name: null as any })
			.where("id", "=", user.id)
			.execute();

		const updatedUser = await db.kysely
			.selectFrom("users")
			.where("id", "=", user.id)
			.selectAll()
			.executeTakeFirstOrThrow();

		expect(updatedUser.name).toBeNull();
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

	test("reset clears all tables with foreign key dependencies", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					users: type({
						id: type("number.integer").configure({ primaryKey: true }),
						name: "string",
					}),
					posts: type({
						id: type("number.integer").configure({ primaryKey: true }),
						title: "string",
						user_id: type("number.integer").configure({ references: "users.id" }),
					}),
				},
			},
		});

		await db.kysely.insertInto("users").values({ id: 1, name: "Alice" }).execute();
		await db.kysely.insertInto("posts").values({ id: 1, title: "Hello", user_id: 1 }).execute();

		db.reset();

		const users = await db.kysely.selectFrom("users").selectAll().execute();
		const posts = await db.kysely.selectFrom("posts").selectAll().execute();

		expect(users).toEqual([]);
		expect(posts).toEqual([]);
	});

	test("coerces date, boolean and JSON in a single table", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: type("number.integer").configure({ primaryKey: true }),
						active: "boolean",
						created_at: "Date",
						meta: type({ tags: "string[]" }),
					}),
				},
			},
		});

		const date = new Date("2025-06-15T12:00:00Z");

		await db.kysely
			.insertInto("items")
			.values({ id: 1, active: true, created_at: date, meta: { tags: ["a", "b"] } })
			.execute();

		const row = await db.kysely.selectFrom("items as i").selectAll("i").executeTakeFirstOrThrow();

		expect(row.active).toBe(true);
		expect(row.created_at).toBeInstanceOf(Date);
		expect(row.created_at.getTime()).toBe(date.getTime());
		expect(row.meta).toEqual({ tags: ["a", "b"] });
	});

	test("select named columns coerces correctly", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: type("number.integer").configure({ primaryKey: true }),
						active: "boolean",
						created_at: "Date",
						meta: type({ tags: "string[]" }),
					}),
				},
			},
		});

		const date = new Date("2025-06-15T12:00:00Z");

		await db.kysely
			.insertInto("items")
			.values({ id: 1, active: true, created_at: date, meta: { tags: ["x"] } })
			.execute();

		const row = await db.kysely
			.selectFrom("items as i")
			.select(["i.active", "i.created_at"])
			.executeTakeFirstOrThrow();

		expect(row.active).toBe(true);
		expect(row.created_at).toBeInstanceOf(Date);
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

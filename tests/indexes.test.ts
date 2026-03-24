import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { Database, generated } from "../src/index.ts";

describe("indexes", () => {
	test("creates composite index", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					posts: type({
						id: generated("autoincrement"),
						user_id: "number.integer",
						category_id: "number.integer",
						title: "string",
					}),
				},
				indexes: {
					posts: [{ columns: ["user_id", "category_id"] }],
				},
			},
		});

		const indexes = await db.kysely
			.selectFrom("sqlite_master")
			.where("type", "=", "index")
			.where("tbl_name", "=", "posts")
			.where("name", "like", "ix_%")
			.select("name")
			.execute();

		expect(indexes).toHaveLength(1);
		expect(indexes[0]?.name).toBe("ix_posts_user_id_category_id");
	});

	test("creates unique composite index", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					posts: type({
						id: generated("autoincrement"),
						user_id: "number.integer",
						category_id: "number.integer",
						title: "string",
					}),
				},
				indexes: {
					posts: [{ columns: ["user_id", "category_id"], unique: true }],
				},
			},
		});

		await db.kysely
			.insertInto("posts")
			.values({ user_id: 1, category_id: 1, title: "Post 1" })
			.execute();

		await expect(() =>
			db.kysely.insertInto("posts").values({ user_id: 1, category_id: 1, title: "Duplicate" }).execute(),
		).toThrow();
	});

	test("creates multiple indexes on same table", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					posts: type({
						id: generated("autoincrement"),
						user_id: "number.integer",
						category_id: "number.integer",
						title: "string",
					}),
				},
				indexes: {
					posts: [
						{ columns: ["user_id", "category_id"], unique: true },
						{ columns: ["category_id"] },
						{ columns: ["title"] },
					],
				},
			},
		});

		const indexes = await db.kysely
			.selectFrom("sqlite_master")
			.where("type", "=", "index")
			.where("tbl_name", "=", "posts")
			.where("name", "not like", "sqlite_%")
			.select("name")
			.execute();

		expect(indexes).toHaveLength(3);
	});

	test("creates single column index", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					users: type({
						id: generated("autoincrement"),
						email: "string",
					}),
				},
				indexes: {
					users: [{ columns: ["email"] }],
				},
			},
		});

		const indexes = await db.kysely
			.selectFrom("sqlite_master")
			.where("type", "=", "index")
			.where("tbl_name", "=", "users")
			.where("name", "=", "ix_users_email")
			.select("sql")
			.executeTakeFirst();

		expect(indexes?.sql).toContain('"email"');
	});

	test("creates indexes on multiple tables", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					users: type({
						id: generated("autoincrement"),
						email: "string",
					}),
					posts: type({
						id: generated("autoincrement"),
						user_id: "number.integer",
						title: "string",
					}),
				},
				indexes: {
					users: [{ columns: ["email"], unique: true }],
					posts: [{ columns: ["user_id"] }, { columns: ["title"] }],
				},
			},
		});

		const userIndexes = await db.kysely
			.selectFrom("sqlite_master")
			.where("type", "=", "index")
			.where("tbl_name", "=", "users")
			.where("name", "not like", "sqlite_%")
			.select("name")
			.execute();

		const postIndexes = await db.kysely
			.selectFrom("sqlite_master")
			.where("type", "=", "index")
			.where("tbl_name", "=", "posts")
			.where("name", "not like", "sqlite_%")
			.select("name")
			.execute();

		expect(userIndexes).toHaveLength(1);
		expect(userIndexes[0]?.name).toBe("ux_users_email");
		expect(postIndexes).toHaveLength(2);
	});

	test("unique composite index allows different combinations", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					posts: type({
						id: generated("autoincrement"),
						user_id: "number.integer",
						category_id: "number.integer",
						title: "string",
					}),
				},
				indexes: {
					posts: [{ columns: ["user_id", "category_id"], unique: true }],
				},
			},
		});

		await db.kysely.insertInto("posts").values({ user_id: 1, category_id: 1, title: "A" }).execute();
		await db.kysely.insertInto("posts").values({ user_id: 1, category_id: 2, title: "B" }).execute();
		await db.kysely.insertInto("posts").values({ user_id: 2, category_id: 1, title: "C" }).execute();

		const posts = await db.kysely.selectFrom("posts").selectAll().execute();

		expect(posts).toHaveLength(3);
	});

	test("database works without indexes", async () => {
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

		await db.kysely.insertInto("items").values({ name: "Test" }).execute();

		const items = await db.kysely.selectFrom("items").selectAll().execute();

		expect(items).toHaveLength(1);
	});

});

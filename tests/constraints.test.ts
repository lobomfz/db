import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { Database, generated, sql, ValidationError } from "../src/index.js";

describe("constraints", () => {
	test("required fields are NOT NULL", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: type("number.integer").configure({ primaryKey: true }),
						name: "string",
					}),
				},
			},
		});

		await expect(() =>
			db.kysely
				.insertInto("items")
				.values({ id: 1, name: null as any })
				.execute(),
		).toThrow(ValidationError);
	});

	test("write morph validates scalar fields on insert", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: type("number.integer").configure({ primaryKey: true }),
						name: "string",
					}),
				},
			},
		});

		await expect(() =>
			db.kysely
				.insertInto("items")
				.values({ id: 1, name: 123 as any })
				.execute(),
		).toThrow(ValidationError);
	});

	test("write morph validates literal columns when another column uses sql", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						name: "string",
						slug: "string",
					}),
				},
			},
		});

		await expect(() =>
			db.kysely
				.insertInto("items")
				.values({ name: sql<string>`upper(${"valid"})`, slug: 123 as any })
				.execute(),
		).toThrow(ValidationError);
	});

	test("write morph validates scalar fields on update", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: type("number.integer").configure({ primaryKey: true }),
						name: "string",
					}),
				},
			},
		});

		await db.kysely.insertInto("items").values({ id: 1, name: "valid" }).execute();

		await expect(() =>
			db.kysely
				.updateTable("items")
				.set({ name: 123 as any })
				.where("id", "=", 1)
				.execute(),
		).toThrow(ValidationError);
	});

	test("optional fields are nullable", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: type("number.integer").configure({ primaryKey: true }),
						"description?": "string",
					}),
				},
			},
		});

		await db.kysely.insertInto("items").values({ id: 1 }).execute();

		const item = await db.kysely.selectFrom("items").selectAll().executeTakeFirst();

		expect(item?.description).toBeNull();
	});

	test("unique constraint", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					users: type({
						id: type("number.integer").configure({ primaryKey: true }),
						email: type("string").configure({ unique: true }),
					}),
				},
			},
		});

		await db.kysely.insertInto("users").values({ id: 1, email: "a@b.com" }).execute();

		await expect(() =>
			db.kysely.insertInto("users").values({ id: 2, email: "a@b.com" }).execute(),
		).toThrow();
	});

	test("generated autoincrement", async () => {
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

		await db.kysely.insertInto("items").values({ name: "First" }).execute();
		await db.kysely.insertInto("items").values({ name: "Second" }).execute();

		const items = await db.kysely.selectFrom("items").selectAll().execute();

		expect(items).toEqual([
			{ id: 1, name: "First" },
			{ id: 2, name: "Second" },
		]);
	});

	test("foreign key constraint", async () => {
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
						user_id: type("number.integer").configure({ references: "users.id" }),
						title: "string",
					}),
				},
			},
		});

		await expect(() =>
			db.kysely.insertInto("posts").values({ id: 1, user_id: 999, title: "Test" }).execute(),
		).toThrow();
	});

	test("foreign key cascade delete", async () => {
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
						user_id: type("number.integer").configure({
							references: "users.id",
							onDelete: "cascade",
						}),
						title: "string",
					}),
				},
			},
		});

		await db.kysely.insertInto("users").values({ id: 1, name: "John" }).execute();
		await db.kysely.insertInto("posts").values({ id: 1, user_id: 1, title: "Post 1" }).execute();
		await db.kysely.deleteFrom("users").where("id", "=", 1).execute();

		const posts = await db.kysely.selectFrom("posts").selectAll().execute();

		expect(posts).toEqual([]);
	});

	test("foreign key set null on delete", async () => {
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
						"user_id?": type("number.integer").configure({
							references: "users.id",
							onDelete: "set null",
						}),
						title: "string",
					}),
				},
			},
		});

		await db.kysely.insertInto("users").values({ id: 1, name: "John" }).execute();
		await db.kysely.insertInto("posts").values({ id: 1, user_id: 1, title: "Post 1" }).execute();
		await db.kysely.deleteFrom("users").where("id", "=", 1).execute();

		const post = await db.kysely.selectFrom("posts").selectAll().executeTakeFirstOrThrow();

		expect(post.user_id).toBeNull();
	});

	test("foreign key restrict on delete", async () => {
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
						user_id: type("number.integer").configure({
							references: "users.id",
							onDelete: "restrict",
						}),
						title: "string",
					}),
				},
			},
		});

		await db.kysely.insertInto("users").values({ id: 1, name: "John" }).execute();
		await db.kysely.insertInto("posts").values({ id: 1, user_id: 1, title: "Post 1" }).execute();

		await expect(() => db.kysely.deleteFrom("users").where("id", "=", 1).execute()).toThrow();
	});
});

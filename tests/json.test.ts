import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { Database, generated, JsonParseError, JsonValidationError } from "../src/index.ts";
describe("JSON columns", () => {
	test("inserts and selects nested object", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					users: type({
						id: generated("autoincrement"),
						name: "string",
						metadata: type({
							tags: "string[]",
							settings: type({
								theme: "'dark' | 'light'",
								notifications: "boolean",
							}),
						}),
					}),
				},
			},
		});

		await db.kysely
			.insertInto("users")
			.values({
				name: "John",
				metadata: {
					tags: ["admin", "active"],
					settings: {
						theme: "dark",
						notifications: true,
					},
				},
			})
			.execute();

		const user = await db.kysely.selectFrom("users").selectAll().executeTakeFirstOrThrow();

		expect(user.name).toBe("John");
		expect(user.metadata.tags).toEqual(["admin", "active"]);
		expect(user.metadata.settings.theme).toBe("dark");
		expect(user.metadata.settings.notifications).toBe(true);
	});

	test("inserts and selects array column", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					posts: type({
						id: generated("autoincrement"),
						title: "string",
						tags: "string[]",
					}),
				},
			},
		});

		await db.kysely
			.insertInto("posts")
			.values({
				title: "Hello World",
				tags: ["javascript", "typescript"],
			})
			.execute();

		const post = await db.kysely.selectFrom("posts").selectAll().executeTakeFirstOrThrow();

		expect(post.title).toBe("Hello World");
		expect(Array.isArray(post.tags)).toBe(true);
		expect(post.tags).toEqual(["javascript", "typescript"]);
	});

	test("throws JsonParseError for malformed JSON", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						data: type({
							count: "number",
						}),
					}),
				},
			},
		});

		const sqlite = (db as any).sqlite;
		sqlite.run("INSERT INTO items (data) VALUES (?)", ["not valid json"]);

		const error = await db.kysely
			.selectFrom("items")
			.selectAll()
			.execute()
			.catch((e: unknown) => e);

		expect(error).toBeInstanceOf(JsonParseError);
		expect((error as JsonParseError).table).toBe("items");
		expect((error as JsonParseError).column).toBe("data");
		expect((error as JsonParseError).value).toBe("not valid json");
		expect((error as JsonParseError).cause).toBeInstanceOf(SyntaxError);
	});

	test("throws JsonValidationError for invalid data", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						data: type({
							count: "number",
						}),
					}),
				},
			},
		});

		const sqlite = (db as any).sqlite;
		sqlite.run("INSERT INTO items (data) VALUES (?)", ['{"count": "not a number"}']);

		const error = await db.kysely
			.selectFrom("items")
			.selectAll()
			.execute()
			.catch((e: unknown) => e);

		expect(error).toBeInstanceOf(JsonValidationError);
		expect((error as JsonValidationError).table).toBe("items");
		expect((error as JsonValidationError).column).toBe("data");
		expect((error as JsonValidationError).summary).toContain("count");
	});

	test("optional JSON column", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					profiles: type({
						id: generated("autoincrement"),
						name: "string",
						"preferences?": type({
							color: "string",
						}),
					}),
				},
			},
		});

		await db.kysely.insertInto("profiles").values({ name: "Jane" }).execute();

		const profile = await db.kysely.selectFrom("profiles").selectAll().executeTakeFirstOrThrow();

		expect(profile.name).toBe("Jane");
		expect(profile.preferences).toBeNull();
	});

	test("updates JSON column", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					configs: type({
						id: generated("autoincrement"),
						settings: type({
							enabled: "boolean",
						}),
					}),
				},
			},
		});

		await db.kysely
			.insertInto("configs")
			.values({ settings: { enabled: false } })
			.execute();

		await db.kysely
			.updateTable("configs")
			.set({ settings: { enabled: true } })
			.where("id", "=", 1)
			.execute();

		const config = await db.kysely.selectFrom("configs").selectAll().executeTakeFirstOrThrow();

		expect(config.settings.enabled).toBe(true);
	});

	test("discriminated union JSON column", async () => {
		const textBlock = type({ type: "'text'", content: "string" });
		const imageBlock = type({ type: "'image'", url: "string", "alt?": "string" });
		const block = textBlock.or(imageBlock);

		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					entries: type({
						id: generated("autoincrement"),
						block,
					}),
				},
			},
		});

		await db.kysely
			.insertInto("entries")
			.values({ block: { type: "text", content: "hello" } })
			.execute();

		await db.kysely
			.insertInto("entries")
			.values({ block: { type: "image", url: "https://example.com/img.png", alt: "photo" } })
			.execute();

		const entries = await db.kysely.selectFrom("entries").selectAll().orderBy("id").execute();

		expect(entries[0]!.block).toEqual({ type: "text", content: "hello" });
		expect(entries[1]!.block).toEqual({
			type: "image",
			url: "https://example.com/img.png",
			alt: "photo",
		});
	});

	test("discriminated union rejects invalid variant", async () => {
		const block = type({ type: "'text'", content: "string" }).or({
			type: "'image'",
			url: "string",
		});

		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					entries: type({
						id: generated("autoincrement"),
						block,
					}),
				},
			},
		});

		const sqlite = (db as any).sqlite;
		sqlite.run("INSERT INTO entries (block) VALUES (?)", ['{"type":"video","src":"x"}']);

		await expect(() => db.kysely.selectFrom("entries").selectAll().execute()).toThrow(
			JsonValidationError,
		);
	});

	test("delete with returning parses JSON", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						data: type({
							value: "number",
						}),
					}),
				},
			},
		});

		await db.kysely
			.insertInto("items")
			.values({ data: { value: 42 } })
			.execute();

		const deleted = await db.kysely
			.deleteFrom("items")
			.where("id", "=", 1)
			.returning(["id", "data"])
			.executeTakeFirstOrThrow();

		expect(deleted.id).toBe(1);
		expect(deleted.data.value).toBe(42);
	});

	test("insert with returning parses JSON", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						data: type({ value: "number" }),
					}),
				},
			},
		});

		const inserted = await db.kysely
			.insertInto("items")
			.values({ data: { value: 42 } })
			.returning(["id", "data"])
			.executeTakeFirstOrThrow();

		expect(inserted.id).toBe(1);
		expect(inserted.data.value).toBe(42);
	});

	test("update with returning parses JSON", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						data: type({ value: "number" }),
					}),
				},
			},
		});

		await db.kysely.insertInto("items").values({ data: { value: 1 } }).execute();

		const updated = await db.kysely
			.updateTable("items")
			.set({ data: { value: 99 } })
			.where("id", "=", 1)
			.returning(["id", "data"])
			.executeTakeFirstOrThrow();

		expect(updated.id).toBe(1);
		expect(updated.data.value).toBe(99);
	});

	test("nullable JSON column", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						data: type({ value: "number" }).or("null"),
					}),
				},
			},
		});

		await db.kysely.insertInto("items").values({ data: null }).execute();
		await db.kysely.insertInto("items").values({ data: { value: 42 } }).execute();

		const items = await db.kysely.selectFrom("items").selectAll().orderBy("id").execute();

		expect(items[0]!.data).toBeNull();
		expect(items[1]!.data).toEqual({ value: 42 });
	});
});

import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { Database, generated } from "../src/index.ts";

describe("types", () => {
	test("booleans stored as integers", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					flags: type({
						id: type("number.integer").configure({ primaryKey: true }),
						active: "boolean",
					}),
				},
			},
		});

		await db.kysely.insertInto("flags").values({ id: 1, active: true }).execute();
		await db.kysely.insertInto("flags").values({ id: 2, active: false }).execute();

		const flags = await db.kysely.selectFrom("flags").selectAll().orderBy("id").execute();

		expect(flags).toEqual([
			{ id: 1, active: true },
			{ id: 2, active: false },
		]);
	});

	test("boolean select filter where false", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					flags: type({
						id: type("number.integer").configure({ primaryKey: true }),
						active: "boolean",
					}),
				},
			},
		});

		await db.kysely.insertInto("flags").values({ id: 1, active: true }).execute();
		await db.kysely.insertInto("flags").values({ id: 2, active: false }).execute();
		await db.kysely.insertInto("flags").values({ id: 3, active: false }).execute();

		const inactive = await db.kysely
			.selectFrom("flags")
			.where("active", "=", false)
			.selectAll()
			.execute();

		expect(inactive).toEqual([
			{ id: 2, active: false },
			{ id: 3, active: false },
		]);
	});

	test("boolean select filter where true", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					flags: type({
						id: type("number.integer").configure({ primaryKey: true }),
						active: "boolean",
					}),
				},
			},
		});

		await db.kysely.insertInto("flags").values({ id: 1, active: true }).execute();
		await db.kysely.insertInto("flags").values({ id: 2, active: false }).execute();
		await db.kysely.insertInto("flags").values({ id: 3, active: true }).execute();

		const active = await db.kysely
			.selectFrom("flags")
			.where("active", "=", true)
			.selectAll()
			.execute();

		expect(active).toEqual([
			{ id: 1, active: true },
			{ id: 3, active: true },
		]);
	});

	test("enums stored as text", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: type("number.integer").configure({ primaryKey: true }),
						status: type.enumerated("pending", "done"),
					}),
				},
			},
		});

		await db.kysely.insertInto("items").values({ id: 1, status: "pending" }).execute();

		const item = await db.kysely.selectFrom("items").selectAll().executeTakeFirst();

		expect(item?.status).toBe("pending");
	});

	test("nullable boolean", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					flags: type({
						id: type("number.integer").configure({ primaryKey: true }),
						active: "boolean | null",
					}),
				},
			},
		});

		await db.kysely.insertInto("flags").values({ id: 1, active: true }).execute();
		await db.kysely.insertInto("flags").values({ id: 2, active: null }).execute();
		await db.kysely.insertInto("flags").values({ id: 3, active: false }).execute();

		const flags = await db.kysely.selectFrom("flags").selectAll().orderBy("id").execute();

		expect(flags).toEqual([
			{ id: 1, active: true },
			{ id: 2, active: null },
			{ id: 3, active: false },
		]);
	});

	test("nullable date", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					events: type({
						id: type("number.integer").configure({ primaryKey: true }),
						happened_at: "Date | null",
					}),
				},
			},
		});

		const date = new Date("2025-06-15T12:00:00Z");

		await db.kysely.insertInto("events").values({ id: 1, happened_at: date }).execute();
		await db.kysely.insertInto("events").values({ id: 2, happened_at: null }).execute();

		const events = await db.kysely.selectFrom("events").selectAll().orderBy("id").execute();

		expect(events[0]!.happened_at).toBeInstanceOf(Date);
		expect(events[0]!.happened_at!.getTime()).toBe(date.getTime());
		expect(events[1]!.happened_at).toBeNull();
	});

	test("number maps to REAL and roundtrips floats", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					measurements: type({
						id: type("number.integer").configure({ primaryKey: true }),
						value: "number",
					}),
				},
			},
		});

		const sqlite = (db as any).sqlite;
		const ddl = sqlite.query("SELECT sql FROM sqlite_master WHERE name = 'measurements'").get()
			?.sql as string;
		expect(ddl).toContain('"value" REAL');

		await db.kysely.insertInto("measurements").values({ id: 1, value: 3.14 }).execute();

		const item = await db.kysely.selectFrom("measurements").selectAll().executeTakeFirst();
		expect(item?.value).toBeCloseTo(3.14);
	});

	test("insert with returning coerces boolean", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					flags: type({
						id: generated("autoincrement"),
						active: "boolean",
					}),
				},
			},
		});

		const result = await db.kysely
			.insertInto("flags")
			.values({ active: true })
			.returning(["id", "active"])
			.executeTakeFirstOrThrow();

		expect(result.active).toBe(true);
	});

	test("update with returning coerces boolean", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					flags: type({
						id: generated("autoincrement"),
						active: "boolean",
					}),
				},
			},
		});

		await db.kysely.insertInto("flags").values({ active: false }).execute();

		const result = await db.kysely
			.updateTable("flags")
			.set({ active: true })
			.where("id", "=", 1)
			.returning(["id", "active"])
			.executeTakeFirstOrThrow();

		expect(result.active).toBe(true);
	});

	test("coercion routes correctly across multiple tables", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					flags: type({
						id: type("number.integer").configure({ primaryKey: true }),
						active: "boolean",
					}),
					events: type({
						id: type("number.integer").configure({ primaryKey: true }),
						happened_at: "Date",
					}),
				},
			},
		});

		const date = new Date("2025-06-15T12:00:00Z");

		await db.kysely.insertInto("flags").values({ id: 1, active: true }).execute();
		await db.kysely.insertInto("events").values({ id: 1, happened_at: date }).execute();

		const flag = await db.kysely.selectFrom("flags").selectAll().executeTakeFirstOrThrow();
		const event = await db.kysely.selectFrom("events").selectAll().executeTakeFirstOrThrow();

		expect(flag.active).toBe(true);
		expect(event.happened_at).toBeInstanceOf(Date);
		expect(event.happened_at.getTime()).toBe(date.getTime());
	});

	test("partial select skips coercion for absent columns", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						name: "string",
						active: "boolean",
						happened_at: "Date",
						data: type({ value: "number" }),
					}),
				},
			},
		});

		const date = new Date("2025-06-15T12:00:00Z");

		await db.kysely
			.insertInto("items")
			.values({ name: "Test", active: true, happened_at: date, data: { value: 42 } })
			.execute();

		const result = await db.kysely
			.selectFrom("items")
			.select(["id", "name"])
			.executeTakeFirstOrThrow();

		expect(result).toEqual({ id: 1, name: "Test" });
	});

	test("number.integer maps to INTEGER in DDL", () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					counters: type({
						id: type("number.integer").configure({ primaryKey: true }),
						count: "number.integer",
					}),
				},
			},
		});

		const sqlite = (db as any).sqlite;
		const ddl = sqlite.query("SELECT sql FROM sqlite_master WHERE name = 'counters'").get()
			?.sql as string;

		expect(ddl).toContain('"count" INTEGER');
	});

	test("select with table alias coerces results", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					flags: type({
						id: type("number.integer").configure({ primaryKey: true }),
						active: "boolean",
					}),
				},
			},
		});

		await db.kysely.insertInto("flags").values({ id: 1, active: true }).execute();

		const result = await (db.kysely as any)
			.selectFrom("flags as f")
			.selectAll()
			.executeTakeFirstOrThrow();

		expect(result.active).toBe(true);
	});

});

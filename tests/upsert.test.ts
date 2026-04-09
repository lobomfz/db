import { describe, test, expect } from "bun:test";

import { type } from "arktype";

import { Database, JsonValidationError } from "../src/index.js";

describe("upsert", () => {
	test("insert path validates JSON and coerces on read", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: type("number.integer").configure({ primaryKey: true }),
						active: "boolean",
						created_at: "Date",
						data: type({ value: "number" }),
					}),
				},
			},
		});

		const date = new Date("2025-06-15T12:00:00Z");

		await db.kysely
			.insertInto("items")
			.values({ id: 1, active: true, created_at: date, data: { value: 42 } })
			.onConflict((oc) =>
				oc.column("id").doUpdateSet({
					data: (eb) => eb.ref("excluded.data"),
				}),
			)
			.execute();

		const row = await db.kysely.selectFrom("items as i").selectAll("i").executeTakeFirstOrThrow();

		expect(row.active).toBe(true);
		expect(row.created_at).toBeInstanceOf(Date);
		expect(row.data).toEqual({ value: 42 });
	});

	test("conflict path updates and coerces correctly", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: type("number.integer").configure({ primaryKey: true }),
						active: "boolean",
						data: type({ value: "number" }),
					}),
				},
			},
		});

		await db.kysely
			.insertInto("items")
			.values({ id: 1, active: false, data: { value: 1 } })
			.execute();

		await db.kysely
			.insertInto("items")
			.values({ id: 1, active: true, data: { value: 99 } })
			.onConflict((oc) =>
				oc.column("id").doUpdateSet({
					active: (eb) => eb.ref("excluded.active"),
					data: (eb) => eb.ref("excluded.data"),
				}),
			)
			.execute();

		const row = await db.kysely.selectFrom("items as i").selectAll("i").executeTakeFirstOrThrow();

		expect(row.active).toBe(true);
		expect(row.data).toEqual({ value: 99 });
	});

	test("rejects invalid JSON in upsert values", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: type("number.integer").configure({ primaryKey: true }),
						data: type({ value: "number" }),
					}),
				},
			},
		});

		await expect(() =>
			db.kysely
				.insertInto("items")
				.values({ id: 1, data: { value: "not a number" } } as any)
				.onConflict((oc) =>
					oc.column("id").doUpdateSet({
						data: (eb) => eb.ref("excluded.data"),
					}),
				)
				.execute(),
		).toThrow(JsonValidationError);
	});

	test("rejects invalid JSON in doUpdateSet literal values", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					items: type({
						id: type("number.integer").configure({ primaryKey: true }),
						data: type({ value: "number" }),
					}),
				},
			},
		});

		await db.kysely
			.insertInto("items")
			.values({ id: 1, data: { value: 1 } })
			.execute();

		await expect(() =>
			db.kysely
				.insertInto("items")
				.values({ id: 1, data: { value: 2 } })
				.onConflict((oc) => oc.column("id").doUpdateSet({ data: { value: "not a number" } } as any))
				.execute(),
		).toThrow(JsonValidationError);
	});

	test("roundtrip with date, boolean and JSON through conflict", async () => {
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
			.values({ id: 1, active: true, created_at: date, meta: { tags: ["a"] } })
			.execute();

		await db.kysely
			.insertInto("items")
			.values({ id: 1, active: false, created_at: date, meta: { tags: ["b", "c"] } })
			.onConflict((oc) =>
				oc.column("id").doUpdateSet({
					active: (eb) => eb.ref("excluded.active"),
					meta: (eb) => eb.ref("excluded.meta"),
				}),
			)
			.execute();

		const row = await db.kysely.selectFrom("items as i").selectAll("i").executeTakeFirstOrThrow();

		expect(row.active).toBe(false);
		expect(row.created_at).toBeInstanceOf(Date);
		expect(row.created_at.getTime()).toBe(date.getTime());
		expect(row.meta).toEqual({ tags: ["b", "c"] });
	});
});

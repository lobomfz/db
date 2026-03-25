import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { Database, generated } from "../src/index.ts";

describe("stream", () => {
	test("streams with boolean coercion", async () => {
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

		await db.kysely.insertInto("flags").values({ active: true }).execute();
		await db.kysely.insertInto("flags").values({ active: false }).execute();

		const results: unknown[] = [];

		for await (const row of db.kysely.selectFrom("flags").selectAll().orderBy("id").stream()) {
			results.push(row);
		}

		expect(results).toEqual([
			{ id: 1, active: true },
			{ id: 2, active: false },
		]);
	});

	test("streams with date coercion", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					events: type({
						id: generated("autoincrement"),
						happened_at: "Date",
					}),
				},
			},
		});

		const d1 = new Date("2025-06-15T12:00:00Z");
		const d2 = new Date("2025-07-20T18:30:00Z");

		await db.kysely.insertInto("events").values({ happened_at: d1 }).execute();
		await db.kysely.insertInto("events").values({ happened_at: d2 }).execute();

		const results: any[] = [];

		for await (const row of db.kysely.selectFrom("events").selectAll().orderBy("id").stream()) {
			results.push(row);
		}

		expect(results).toHaveLength(2);
		expect(results[0].happened_at).toBeInstanceOf(Date);
		expect(results[0].happened_at.getTime()).toBe(d1.getTime());
		expect(results[1].happened_at).toBeInstanceOf(Date);
		expect(results[1].happened_at.getTime()).toBe(d2.getTime());
	});

	test("streams with JSON coercion", async () => {
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

		await db.kysely
			.insertInto("items")
			.values({ data: { value: 42 } })
			.execute();
		await db.kysely
			.insertInto("items")
			.values({ data: { value: 99 } })
			.execute();

		const results: any[] = [];

		for await (const row of db.kysely.selectFrom("items").selectAll().orderBy("id").stream()) {
			results.push(row);
		}

		expect(results).toHaveLength(2);
		expect(results[0].data).toEqual({ value: 42 });
		expect(results[1].data).toEqual({ value: 99 });
	});

	test("streams query results row by row", async () => {
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

		await db.kysely.insertInto("items").values({ name: "A" }).execute();
		await db.kysely.insertInto("items").values({ name: "B" }).execute();
		await db.kysely.insertInto("items").values({ name: "C" }).execute();

		const results: unknown[] = [];

		for await (const row of db.kysely.selectFrom("items").selectAll().orderBy("id").stream()) {
			results.push(row);
		}

		expect(results).toEqual([
			{ id: 1, name: "A" },
			{ id: 2, name: "B" },
			{ id: 3, name: "C" },
		]);
	});
});

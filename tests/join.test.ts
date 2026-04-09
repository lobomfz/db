import { describe, test, expect, beforeAll } from "bun:test";

import { type } from "arktype";

import { Database, generated } from "../src/index.ts";

describe("JOIN coercion", () => {
	const db = new Database({
		path: ":memory:",
		schema: {
			tables: {
				users: type({
					id: generated("autoincrement"),
					name: "string",
					active: "boolean",
				}),
				orders: type({
					id: generated("autoincrement"),
					user_id: "number",
					created_at: "Date",
					"meta?": type({ note: "string" }),
				}),
			},
		},
	});

	const date = new Date("2025-06-15T12:00:00Z");

	beforeAll(async () => {
		await db.kysely
			.insertInto("users")
			.values([
				{ name: "Alice", active: true },
				{ name: "Lonely", active: false },
			])
			.execute();

		await db.kysely
			.insertInto("orders")
			.values({ user_id: 1, created_at: date, meta: { note: "first" } })
			.execute();
	});

	test("innerJoin selectAll coerces types from both tables", async () => {
		const row = await db.kysely
			.selectFrom("orders as o")
			.innerJoin("users as u", "u.id", "o.user_id")
			.selectAll()
			.executeTakeFirstOrThrow();

		expect(row.active).toBe(true);
		expect(row.created_at).toBeInstanceOf(Date);
		expect(row.meta).toEqual({ note: "first" });
	});

	test("innerJoin select named columns coerces correctly", async () => {
		const row = await db.kysely
			.selectFrom("orders as o")
			.innerJoin("users as u", "u.id", "o.user_id")
			.select(["u.active", "o.created_at", "o.meta"])
			.executeTakeFirstOrThrow();

		expect(row.active).toBe(true);
		expect(row.created_at).toBeInstanceOf(Date);
		expect(row.meta).toEqual({ note: "first" });
	});

	test("left join returns null for coerced columns when no match", async () => {
		const row = await db.kysely
			.selectFrom("users as u")
			.leftJoin("orders as o", "o.user_id", "u.id")
			.where("u.name", "=", "Lonely")
			.select(["u.name", "u.active", "o.created_at", "o.meta"])
			.executeTakeFirstOrThrow();

		expect(row.active).toBe(false);
		expect(row.created_at).toBeNull();
		expect(row.meta).toBeNull();
	});
});

describe("JOIN same column name", () => {
	test("coerces each by table ownership", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					events: type({
						id: generated("autoincrement"),
						updated_at: "Date",
					}),
					logs: type({
						id: generated("autoincrement"),
						event_id: "number",
						updated_at: "number",
					}),
				},
			},
		});

		const date = new Date("2025-06-15T12:00:00Z");

		await db.kysely.insertInto("events").values({ updated_at: date }).execute();
		await db.kysely.insertInto("logs").values({ event_id: 1, updated_at: 12345 }).execute();

		const row = await db.kysely
			.selectFrom("logs as l")
			.innerJoin("events as e", "e.id", "l.event_id")
			.select(["l.updated_at", "e.updated_at as event_updated_at"])
			.executeTakeFirstOrThrow();

		expect(typeof row.updated_at).toBe("number");
		expect(row.updated_at).toBe(12345);
		expect(row.event_updated_at).toBeInstanceOf(Date);
	});
});

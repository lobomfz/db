import { describe, test, expect, beforeAll } from "bun:test";

import { type } from "arktype";

import { Database, generated } from "../src/index.ts";

describe("scalar subquery with generated('now')", () => {
	const db = new Database({
		path: ":memory:",
		schema: {
			tables: {
				parents: type({
					id: generated("autoincrement"),
					name: "string",
				}),
				children: type({
					id: generated("autoincrement"),
					parent_id: "number",
					updated_at: generated("now"),
				}),
			},
		},
	});

	beforeAll(async () => {
		await db.kysely.insertInto("parents").values({ name: "Alice" }).execute();
		await db.kysely.insertInto("children").values({ parent_id: 1 }).execute();
	});

	test("scalar subquery returns Date, not epoch seconds", async () => {
		const result = await db.kysely
			.selectFrom("parents as p")
			.select((eb) => [
				"p.id",
				eb
					.selectFrom("children as c")
					.whereRef("c.parent_id", "=", "p.id")
					.select(["c.updated_at"])
					.orderBy("c.updated_at", "desc")
					.limit(1)
					.as("latest_child_updated_at"),
			])
			.where("p.id", "=", 1)
			.executeTakeFirstOrThrow();

		const direct = await db.kysely.selectFrom("children").selectAll().executeTakeFirstOrThrow();

		expect(result.latest_child_updated_at).toEqual(direct.updated_at);
	});
});

describe("scalar subquery deserialization edge cases", () => {
	test("scalar subquery with join returns Date from joined table", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					parents: type({
						id: generated("autoincrement"),
						name: "string",
					}),
					children: type({
						id: generated("autoincrement"),
						parent_id: "number",
						event_id: "number",
					}),
					events: type({
						id: generated("autoincrement"),
						updated_at: "Date",
					}),
				},
			},
		});

		const date = new Date("2025-06-15T12:00:00Z");

		await db.kysely.insertInto("parents").values({ name: "Alice" }).execute();
		await db.kysely.insertInto("events").values({ updated_at: date }).execute();
		await db.kysely.insertInto("children").values({ parent_id: 1, event_id: 1 }).execute();

		const result = await db.kysely
			.selectFrom("parents as p")
			.select((eb) => [
				"p.id",
				eb
					.selectFrom("children as c")
					.innerJoin("events as e", "e.id", "c.event_id")
					.whereRef("c.parent_id", "=", "p.id")
					.select(["e.updated_at"])
					.limit(1)
					.as("latest_event_updated_at"),
			])
			.where("p.id", "=", 1)
			.executeTakeFirstOrThrow();

		expect(result.latest_event_updated_at).toBeInstanceOf(Date);
		expect(result.latest_event_updated_at!.getTime()).toBe(date.getTime());
	});

	test("scalar subquery with max() returns Date", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					parents: type({
						id: generated("autoincrement"),
						name: "string",
					}),
					children: type({
						id: generated("autoincrement"),
						parent_id: "number",
						updated_at: "Date",
					}),
				},
			},
		});

		const date = new Date("2025-06-15T12:00:00Z");

		await db.kysely.insertInto("parents").values({ name: "Alice" }).execute();
		await db.kysely
			.insertInto("children")
			.values([
				{ parent_id: 1, updated_at: new Date("2025-01-01T00:00:00Z") },
				{ parent_id: 1, updated_at: date },
			])
			.execute();

		const result = await db.kysely
			.selectFrom("parents as p")
			.select((eb) => [
				"p.id",
				eb
					.selectFrom("children as c")
					.whereRef("c.parent_id", "=", "p.id")
					.select((eb2) => eb2.fn.max("c.updated_at").as("value"))
					.as("latest_child_updated_at"),
			])
			.where("p.id", "=", 1)
			.executeTakeFirstOrThrow();

		expect(result.latest_child_updated_at).toBeInstanceOf(Date);
		expect(result.latest_child_updated_at!.getTime()).toBe(date.getTime());
	});

	test("scalar subquery returns boolean with arbitrary alias", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					parents: type({
						id: generated("autoincrement"),
						name: "string",
					}),
					flags: type({
						id: generated("autoincrement"),
						parent_id: "number",
						active: "boolean",
					}),
				},
			},
		});

		await db.kysely.insertInto("parents").values({ name: "Alice" }).execute();
		await db.kysely.insertInto("flags").values({ parent_id: 1, active: true }).execute();

		const result = await db.kysely
			.selectFrom("parents as p")
			.select((eb) => [
				"p.id",
				eb
					.selectFrom("flags as f")
					.whereRef("f.parent_id", "=", "p.id")
					.select(["f.active"])
					.limit(1)
					.as("latest_flag_active"),
			])
			.where("p.id", "=", 1)
			.executeTakeFirstOrThrow();

		expect(result.latest_flag_active).toBe(true);
	});

	test("scalar subquery with min() returns boolean", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					parents: type({
						id: generated("autoincrement"),
						name: "string",
					}),
					flags: type({
						id: generated("autoincrement"),
						parent_id: "number",
						active: "boolean",
					}),
				},
			},
		});

		await db.kysely.insertInto("parents").values({ name: "Alice" }).execute();
		await db.kysely
			.insertInto("flags")
			.values([
				{ parent_id: 1, active: true },
				{ parent_id: 1, active: false },
			])
			.execute();

		const result = await db.kysely
			.selectFrom("parents as p")
			.select((eb) => [
				"p.id",
				eb
					.selectFrom("flags as f")
					.whereRef("f.parent_id", "=", "p.id")
					.select((eb2) => eb2.fn.min("f.active").as("value"))
					.as("all_children_active"),
			])
			.where("p.id", "=", 1)
			.executeTakeFirstOrThrow();

		expect(result.all_children_active).toBe(false);
	});

	test("scalar subquery parses JSON with inner aliased selection", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					parents: type({
						id: generated("autoincrement"),
						name: "string",
					}),
					profiles: type({
						id: generated("autoincrement"),
						parent_id: "number",
						settings: type({
							theme: "'dark' | 'light'",
							notifications: "boolean",
						}),
					}),
				},
			},
		});

		await db.kysely.insertInto("parents").values({ name: "Alice" }).execute();
		await db.kysely
			.insertInto("profiles")
			.values({
				parent_id: 1,
				settings: { theme: "dark", notifications: true },
			})
			.execute();

		const result = await db.kysely
			.selectFrom("parents as p")
			.select((eb) => [
				"p.id",
				eb
					.selectFrom("profiles as pr")
					.whereRef("pr.parent_id", "=", "p.id")
					.select(["pr.settings as value"])
					.limit(1)
					.as("latest_profile_settings"),
			])
			.where("p.id", "=", 1)
			.executeTakeFirstOrThrow();

		expect(result.latest_profile_settings).toEqual({
			theme: "dark",
			notifications: true,
		});
	});

	test("nested scalar subquery propagates coercion recursively", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					parents: type({
						id: generated("autoincrement"),
						name: "string",
					}),
					children: type({
						id: generated("autoincrement"),
						parent_id: "number",
						event_id: "number",
					}),
					events: type({
						id: generated("autoincrement"),
						updated_at: "Date",
					}),
				},
			},
		});

		const date = new Date("2025-06-15T12:00:00Z");

		await db.kysely.insertInto("parents").values({ name: "Alice" }).execute();
		await db.kysely.insertInto("events").values({ updated_at: date }).execute();
		await db.kysely.insertInto("children").values({ parent_id: 1, event_id: 1 }).execute();

		const result = await db.kysely
			.selectFrom("parents as p")
			.select((eb) => [
				"p.id",
				eb
					.selectFrom("children as c")
					.whereRef("c.parent_id", "=", "p.id")
					.select((eb2) =>
						eb2
							.selectFrom("events as e")
							.whereRef("e.id", "=", "c.event_id")
							.select(["e.updated_at"])
							.limit(1)
							.as("value"),
					)
					.limit(1)
					.as("latest_nested_event_updated_at"),
			])
			.where("p.id", "=", 1)
			.executeTakeFirstOrThrow();

		expect(result.latest_nested_event_updated_at).toBeInstanceOf(Date);
		expect(result.latest_nested_event_updated_at!.getTime()).toBe(date.getTime());
	});
});

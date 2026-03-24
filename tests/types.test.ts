import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { Database } from "../src/index.ts";

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

});

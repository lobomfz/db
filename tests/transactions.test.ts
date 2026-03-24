import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { Database, generated } from "../src/index.ts";

describe("transactions", () => {
	test("committed transaction persists data", async () => {
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

		await db.kysely.transaction().execute(async (trx) => {
			await trx.insertInto("items").values({ name: "A" }).execute();
			await trx.insertInto("items").values({ name: "B" }).execute();
		});

		const items = await db.kysely.selectFrom("items").selectAll().orderBy("id").execute();

		expect(items).toEqual([
			{ id: 1, name: "A" },
			{ id: 2, name: "B" },
		]);
	});

	test("rolled back transaction discards data", async () => {
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

		const error = await db.kysely
			.transaction()
			.execute(async (trx) => {
				await trx.insertInto("items").values({ name: "A" }).execute();
				throw new Error("rollback");
			})
			.catch((e: unknown) => e);

		expect(error).toBeInstanceOf(Error);

		const items = await db.kysely.selectFrom("items").selectAll().execute();

		expect(items).toEqual([]);
	});

	test("concurrent queries serialize through mutex", async () => {
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

		await Promise.all(
			Array.from({ length: 50 }, (_, i) =>
				db.kysely.insertInto("items").values({ name: `item-${i}` }).execute(),
			),
		);

		const items = await db.kysely.selectFrom("items").selectAll().execute();

		expect(items).toHaveLength(50);
	});

	test("transaction coerces types on returning", async () => {
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

		const result = await db.kysely.transaction().execute(async (trx) => {
			return trx
				.insertInto("flags")
				.values({ active: true })
				.returning(["id", "active"])
				.executeTakeFirstOrThrow();
		});

		expect(result.active).toBe(true);
	});
});

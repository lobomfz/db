import { afterEach, describe, test, expect } from "bun:test";
import { unlink } from "node:fs/promises";
import { type } from "arktype";
import { sql, Database, generated } from "../src/index.js";

const TEST_DB = "./test.db";

async function cleanup() {
	await unlink(TEST_DB).catch(() => {});
	await unlink(`${TEST_DB}-journal`).catch(() => {});
	await unlink(`${TEST_DB}-wal`).catch(() => {});
	await unlink(`${TEST_DB}-shm`).catch(() => {});
}

describe("pragmas", () => {
	afterEach(cleanup);

	test("applies all pragmas", async () => {
		const db = new Database({
			path: TEST_DB,
			pragmas: {
				journal_mode: "wal",
				synchronous: "normal",
				foreign_keys: true,
				busy_timeout_ms: 5000,
			},
			schema: {
				tables: {
					items: type({
						id: generated("autoincrement"),
						name: "string",
					}),
				},
			},
		});

		const journal = await sql<{ journal_mode: string }>`PRAGMA journal_mode`.execute(db.kysely);
		const sync = await sql<{ synchronous: number }>`PRAGMA synchronous`.execute(db.kysely);
		const fk = await sql<{ foreign_keys: number }>`PRAGMA foreign_keys`.execute(db.kysely);
		const timeout = await sql<{ timeout: number }>`PRAGMA busy_timeout`.execute(db.kysely);

		expect(journal.rows[0]?.journal_mode).toBe("wal");
		expect(sync.rows[0]?.synchronous).toBe(1);
		expect(fk.rows[0]?.foreign_keys).toBe(1);
		expect(timeout.rows[0]?.timeout).toBe(5000);
	});

	test("default pragmas enable foreign_keys", async () => {
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

		const fk = await sql<{ foreign_keys: number }>`PRAGMA foreign_keys`.execute(db.kysely);
		expect(fk.rows[0]?.foreign_keys).toBe(1);
	});

	test("foreign_keys can be disabled", async () => {
		const db = new Database({
			path: ":memory:",
			pragmas: {
				foreign_keys: false,
			},
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

		const fk = await sql<{ foreign_keys: number }>`PRAGMA foreign_keys`.execute(db.kysely);
		expect(fk.rows[0]?.foreign_keys).toBe(0);

		await db.kysely.insertInto("posts").values({ id: 1, user_id: 999, title: "Test" }).execute();

		const post = await db.kysely.selectFrom("posts").selectAll().executeTakeFirst();
		expect(post).toBeDefined();
	});
});

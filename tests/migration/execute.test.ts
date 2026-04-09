import { describe, test, expect } from "bun:test";
import { Database as BunDatabase } from "bun:sqlite";
import { Introspector } from "../../src/migration/introspect";
import { Executor } from "../../src/migration/execute";

describe("execute", () => {
	test("creates tables from CreateTable operations", () => {
		const db = new BunDatabase(":memory:");

		new Executor(db, [
			{
				type: "CreateTable",
				table: "users",
				sql: 'CREATE TABLE "users" (id INTEGER PRIMARY KEY, name TEXT NOT NULL)',
			},
		]).execute();

		expect(new Introspector(db).introspect().has("users")).toBe(true);
	});

	test("does nothing for empty operations", () => {
		const db = new BunDatabase(":memory:");

		new Executor(db, []).execute();

		expect(new Introspector(db).introspect().size).toBe(0);
	});

	test("rolls back all changes on error", () => {
		const db = new BunDatabase(":memory:");

		expect(() => {
			new Executor(db, [
				{
					type: "CreateTable",
					table: "users",
					sql: 'CREATE TABLE "users" (id INTEGER PRIMARY KEY)',
				},
				{ type: "CreateTable", table: "bad", sql: "INVALID SQL STATEMENT" },
			]).execute();
		}).toThrow();

		expect(new Introspector(db).introspect().has("users")).toBe(false);
	});

	test("drops tables from DropTable operations", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" (id INTEGER PRIMARY KEY)');
		db.run('CREATE TABLE "orphan" (id INTEGER PRIMARY KEY)');

		new Executor(db, [{ type: "DropTable", table: "orphan" }]).execute();

		const tables = new Introspector(db).introspect();

		expect(tables.has("users")).toBe(true);
		expect(tables.has("orphan")).toBe(false);
	});

	test("dropped table data ceases to exist", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "items" (id INTEGER PRIMARY KEY, name TEXT)');
		db.run("INSERT INTO \"items\" VALUES (1, 'test')");

		new Executor(db, [{ type: "DropTable", table: "items" }]).execute();

		expect(() => db.prepare('SELECT * FROM "items"').all()).toThrow();
	});

	test("adds column via AddColumn", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL)');

		new Executor(db, [{ type: "AddColumn", table: "users", columnDef: '"bio" TEXT' }]).execute();

		expect(new Introspector(db).introspect().get("users")!.columns.has("bio")).toBe(true);
	});

	test("preserves existing data after AddColumn", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL)');
		db.run("INSERT INTO \"users\" VALUES (1, 'Alice')");
		db.run("INSERT INTO \"users\" VALUES (2, 'Bob')");

		new Executor(db, [{ type: "AddColumn", table: "users", columnDef: '"bio" TEXT' }]).execute();

		const rows = db.prepare('SELECT * FROM "users" ORDER BY id').all() as {
			id: number;
			name: string;
			bio: string | null;
		}[];

		expect(rows).toHaveLength(2);
		expect(rows[0]!.name).toBe("Alice");
		expect(rows[0]!.bio).toBeNull();
	});

	test("new column with DEFAULT fills existing rows", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL)');
		db.run("INSERT INTO \"users\" VALUES (1, 'Alice')");

		new Executor(db, [
			{ type: "AddColumn", table: "users", columnDef: "\"status\" TEXT NOT NULL DEFAULT 'active'" },
		]).execute();

		const row = db.prepare('SELECT status FROM "users"').get() as { status: string };

		expect(row.status).toBe("active");
	});

	test("rebuild preserves remaining column data and removes dropped column", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL, "bio" TEXT)');
		db.run("INSERT INTO \"users\" VALUES (1, 'Alice', 'Bio A')");
		db.run("INSERT INTO \"users\" VALUES (2, 'Bob', 'Bio B')");

		new Executor(db, [
			{
				type: "RebuildTable",
				table: "users",
				createSql:
					'CREATE TABLE IF NOT EXISTS "users" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL)',
				columnCopies: [
					{ name: "id", expr: '"id"' },
					{ name: "name", expr: '"name"' },
				],
			},
		]).execute();

		const rows = db.prepare('SELECT * FROM "users" ORDER BY id').all() as {
			id: number;
			name: string;
		}[];

		expect(rows).toEqual([
			{ id: 1, name: "Alice" },
			{ id: 2, name: "Bob" },
		]);

		expect(new Introspector(db).introspect().get("users")!.columns.has("bio")).toBe(false);
	});

	test("rebuild with COALESCE fills NULLs with default", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "status" TEXT)');
		db.run("INSERT INTO \"users\" VALUES (1, 'active')");
		db.run('INSERT INTO "users" VALUES (2, NULL)');

		new Executor(db, [
			{
				type: "RebuildTable",
				table: "users",
				createSql:
					'CREATE TABLE IF NOT EXISTS "users" ("id" INTEGER PRIMARY KEY, "status" TEXT NOT NULL DEFAULT \'active\')',
				columnCopies: [
					{ name: "id", expr: '"id"' },
					{ name: "status", expr: "COALESCE(\"status\", 'active')" },
				],
			},
		]).execute();

		const rows = db.prepare('SELECT * FROM "users" ORDER BY id').all() as {
			id: number;
			status: string;
		}[];

		expect(rows).toEqual([
			{ id: 1, status: "active" },
			{ id: 2, status: "active" },
		]);
	});

	test("rebuild disables FK during rebuild and re-enables after", () => {
		const db = new BunDatabase(":memory:");
		db.run("PRAGMA foreign_keys = ON");
		db.run('CREATE TABLE "categories" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL)');
		db.run(
			'CREATE TABLE "posts" ("id" INTEGER PRIMARY KEY, "cat_id" INTEGER, "title" TEXT, FOREIGN KEY ("cat_id") REFERENCES "categories"("id"))',
		);
		db.run("INSERT INTO \"categories\" VALUES (1, 'Tech')");
		db.run("INSERT INTO \"posts\" VALUES (1, 1, 'Post')");

		new Executor(db, [
			{
				type: "RebuildTable",
				table: "posts",
				createSql:
					'CREATE TABLE IF NOT EXISTS "posts" ("id" INTEGER PRIMARY KEY, "cat_id" INTEGER, FOREIGN KEY ("cat_id") REFERENCES "categories"("id"))',
				columnCopies: [
					{ name: "id", expr: '"id"' },
					{ name: "cat_id", expr: '"cat_id"' },
				],
			},
		]).execute();

		const { foreign_keys } = db.prepare("PRAGMA foreign_keys").get() as { foreign_keys: number };

		expect(foreign_keys).toBe(1);

		const rows = db.prepare('SELECT * FROM "posts"').all() as { id: number; cat_id: number }[];

		expect(rows).toEqual([{ id: 1, cat_id: 1 }]);
	});

	test("rebuild of table referenced by other tables preserves FK integrity", () => {
		const db = new BunDatabase(":memory:");
		db.run("PRAGMA foreign_keys = ON");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL, "bio" TEXT)');
		db.run(
			'CREATE TABLE "posts" ("id" INTEGER PRIMARY KEY, "user_id" INTEGER, FOREIGN KEY ("user_id") REFERENCES "users"("id"))',
		);
		db.run("INSERT INTO \"users\" VALUES (1, 'Alice', 'Bio')");
		db.run('INSERT INTO "posts" VALUES (1, 1)');

		new Executor(db, [
			{
				type: "RebuildTable",
				table: "users",
				createSql:
					'CREATE TABLE IF NOT EXISTS "users" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL)',
				columnCopies: [
					{ name: "id", expr: '"id"' },
					{ name: "name", expr: '"name"' },
				],
			},
		]).execute();

		const users = db.prepare('SELECT * FROM "users"').all() as { id: number; name: string }[];
		const posts = db.prepare('SELECT * FROM "posts"').all() as { id: number; user_id: number }[];

		expect(users).toEqual([{ id: 1, name: "Alice" }]);
		expect(posts).toEqual([{ id: 1, user_id: 1 }]);
	});

	test("rebuild rolls back on error and restores FK", () => {
		const db = new BunDatabase(":memory:");
		db.run("PRAGMA foreign_keys = ON");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL, "bio" TEXT)');
		db.run("INSERT INTO \"users\" VALUES (1, 'Alice', 'Bio')");

		expect(() => {
			new Executor(db, [
				{
					type: "RebuildTable",
					table: "users",
					createSql: "INVALID SQL",
					columnCopies: [
						{ name: "id", expr: '"id"' },
						{ name: "name", expr: '"name"' },
					],
				},
			]).execute();
		}).toThrow();

		expect(new Introspector(db).introspect().get("users")!.columns.has("bio")).toBe(true);

		const { foreign_keys } = db.prepare("PRAGMA foreign_keys").get() as { foreign_keys: number };

		expect(foreign_keys).toBe(1);
	});

	test("multiple tables rebuilt in same transaction", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "name" TEXT, "bio" TEXT)');
		db.run('CREATE TABLE "posts" ("id" INTEGER PRIMARY KEY, "title" TEXT, "body" TEXT)');
		db.run("INSERT INTO \"users\" VALUES (1, 'Alice', 'Bio')");
		db.run("INSERT INTO \"posts\" VALUES (1, 'Title', 'Body')");

		new Executor(db, [
			{
				type: "RebuildTable",
				table: "users",
				createSql: 'CREATE TABLE IF NOT EXISTS "users" ("id" INTEGER PRIMARY KEY, "name" TEXT)',
				columnCopies: [
					{ name: "id", expr: '"id"' },
					{ name: "name", expr: '"name"' },
				],
			},
			{
				type: "RebuildTable",
				table: "posts",
				createSql: 'CREATE TABLE IF NOT EXISTS "posts" ("id" INTEGER PRIMARY KEY, "title" TEXT)',
				columnCopies: [
					{ name: "id", expr: '"id"' },
					{ name: "title", expr: '"title"' },
				],
			},
		]).execute();

		expect(new Introspector(db).introspect().get("users")!.columns.has("bio")).toBe(false);
		expect(new Introspector(db).introspect().get("posts")!.columns.has("body")).toBe(false);

		const users = db.prepare('SELECT * FROM "users"').all() as { id: number; name: string }[];
		const posts = db.prepare('SELECT * FROM "posts"').all() as { id: number; title: string }[];

		expect(users).toEqual([{ id: 1, name: "Alice" }]);
		expect(posts).toEqual([{ id: 1, title: "Title" }]);
	});

	test("creates index via CreateIndex", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "name" TEXT)');

		new Executor(db, [
			{
				type: "CreateIndex",
				table: "users",
				columns: ["name"],
				sql: 'CREATE INDEX "ix_users_name" ON "users" ("name")',
			},
		]).execute();

		expect(new Introspector(db).introspect().get("users")!.indexes).toHaveLength(1);
	});

	test("drops index via DropIndex", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "name" TEXT)');
		db.run('CREATE INDEX "ix_users_name" ON "users" ("name")');

		new Executor(db, [{ type: "DropIndex", index: "ix_users_name" }]).execute();

		expect(new Introspector(db).introspect().get("users")!.indexes).toHaveLength(0);
	});

	test("unique index prevents duplicates", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "email" TEXT)');

		new Executor(db, [
			{
				type: "CreateIndex",
				table: "users",
				columns: ["email"],
				sql: 'CREATE UNIQUE INDEX "ux_users_email" ON "users" ("email")',
			},
		]).execute();

		db.run("INSERT INTO \"users\" VALUES (1, 'a@b.com')");

		expect(() => db.run("INSERT INTO \"users\" VALUES (2, 'a@b.com')")).toThrow();
	});

	test("rebuild with FK column preserves constraint without duplication", () => {
		const db = new BunDatabase(":memory:");
		db.run("PRAGMA foreign_keys = ON");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY)');
		db.run(
			'CREATE TABLE "posts" ("id" INTEGER PRIMARY KEY, "user_id" INTEGER, "title" TEXT, FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE)',
		);
		db.run('INSERT INTO "users" VALUES (1)');
		db.run("INSERT INTO \"posts\" VALUES (1, 1, 'Title')");

		new Executor(db, [
			{
				type: "RebuildTable",
				table: "posts",
				createSql:
					'CREATE TABLE IF NOT EXISTS "posts" ("id" INTEGER PRIMARY KEY, "user_id" INTEGER, FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE)',
				columnCopies: [
					{ name: "id", expr: '"id"' },
					{ name: "user_id", expr: '"user_id"' },
				],
			},
		]).execute();

		db.run('INSERT INTO "posts" VALUES (2, 1)');
		expect(() => db.run('INSERT INTO "posts" VALUES (3, 999)')).toThrow();

		db.run('DELETE FROM "users" WHERE id = 1');
		const posts = db.prepare('SELECT * FROM "posts"').all();

		expect(posts).toHaveLength(0);
	});

	test("AddColumn with inline FK enforces constraint", () => {
		const db = new BunDatabase(":memory:");
		db.run("PRAGMA foreign_keys = ON");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY)');
		db.run('CREATE TABLE "posts" ("id" INTEGER PRIMARY KEY, "title" TEXT)');
		db.run('INSERT INTO "users" VALUES (1)');

		new Executor(db, [
			{
				type: "AddColumn",
				table: "posts",
				columnDef: '"user_id" INTEGER REFERENCES "users"("id") ON DELETE CASCADE',
			},
		]).execute();

		db.run("INSERT INTO \"posts\" (id, title, user_id) VALUES (1, 'Post', 1)");
		expect(() =>
			db.run("INSERT INTO \"posts\" (id, title, user_id) VALUES (2, 'Bad', 999)"),
		).toThrow();

		db.run('DELETE FROM "users" WHERE id = 1');
		const posts = db.prepare('SELECT * FROM "posts"').all();

		expect(posts).toHaveLength(0);
	});

	test("rebuild throws on FK violation and rolls back", () => {
		const db = new BunDatabase(":memory:");
		db.run("PRAGMA foreign_keys = ON");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL, "bio" TEXT)');
		db.run(
			'CREATE TABLE "posts" ("id" INTEGER PRIMARY KEY, "user_id" INTEGER, FOREIGN KEY ("user_id") REFERENCES "users"("id"))',
		);
		db.run("INSERT INTO \"users\" VALUES (1, 'Alice', 'Bio')");

		db.run("PRAGMA foreign_keys = OFF");
		db.run('INSERT INTO "posts" VALUES (1, 999)');
		db.run("PRAGMA foreign_keys = ON");

		expect(() => {
			new Executor(db, [
				{
					type: "RebuildTable",
					table: "users",
					createSql:
						'CREATE TABLE IF NOT EXISTS "users" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL)',
					columnCopies: [
						{ name: "id", expr: '"id"' },
						{ name: "name", expr: '"name"' },
					],
				},
			]).execute();
		}).toThrow(/Foreign key check failed/);

		expect(new Introspector(db).introspect().get("users")!.columns.has("bio")).toBe(true);

		const { foreign_keys } = db.prepare("PRAGMA foreign_keys").get() as { foreign_keys: number };

		expect(foreign_keys).toBe(1);
	});
});

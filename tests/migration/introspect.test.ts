import { describe, test, expect } from "bun:test";
import { Database as BunDatabase } from "bun:sqlite";
import { Introspector } from "../../src/migration/introspect";

describe("introspect", () => {
	test("returns empty map for database without tables", () => {
		const db = new BunDatabase(":memory:");

		expect(new Introspector(db).introspect().size).toBe(0);
	});

	test("returns existing table names with columns", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" (id INTEGER PRIMARY KEY, name TEXT NOT NULL)');
		db.run('CREATE TABLE "posts" (id INTEGER PRIMARY KEY, title TEXT)');

		const tables = new Introspector(db).introspect();

		expect(tables.has("users")).toBe(true);
		expect(tables.has("posts")).toBe(true);
		expect(tables.size).toBe(2);
		expect(tables.get("users")!.columns.has("id")).toBe(true);
		expect(tables.get("users")!.columns.has("name")).toBe(true);
	});

	test("returns column type, nullability and default", () => {
		const db = new BunDatabase(":memory:");
		db.run(
			'CREATE TABLE "items" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL, "bio" TEXT, "count" INTEGER NOT NULL DEFAULT 0)',
		);

		const items = new Introspector(db).introspect().get("items")!;

		expect(items.columns.get("id")!.type).toBe("INTEGER");

		const name = items.columns.get("name")!;

		expect(name.notnull).toBe(true);
		expect(name.defaultValue).toBeNull();

		const bio = items.columns.get("bio")!;

		expect(bio.notnull).toBe(false);

		const count = items.columns.get("count")!;

		expect(count.notnull).toBe(true);
		expect(count.defaultValue).toBe("0");
	});

	test("returns unique constraint info", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "email" TEXT UNIQUE, "name" TEXT)');

		const users = new Introspector(db).introspect().get("users")!;

		expect(users.columns.get("email")!.unique).toBe(true);
		expect(users.columns.get("name")!.unique).toBe(false);
	});

	test("returns foreign key info", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY)');
		db.run(
			'CREATE TABLE "posts" ("id" INTEGER PRIMARY KEY, "user_id" INTEGER, FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE)',
		);

		const posts = new Introspector(db).introspect().get("posts")!;
		const userId = posts.columns.get("user_id")!;

		expect(userId.references).toBe("users.id");
		expect(userId.onDelete).toBe("CASCADE");
	});

	test("normalizes NO ACTION onDelete to null", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY)');
		db.run(
			'CREATE TABLE "posts" ("id" INTEGER PRIMARY KEY, "user_id" INTEGER, FOREIGN KEY ("user_id") REFERENCES "users"("id"))',
		);

		const userId = new Introspector(db).introspect().get("posts")!.columns.get("user_id")!;

		expect(userId.onDelete).toBeNull();
	});

	test("returns hasData correctly", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" (id INTEGER PRIMARY KEY)');

		expect(new Introspector(db).introspect().get("users")!.hasData).toBe(false);

		db.run('INSERT INTO "users" VALUES (1)');

		expect(new Introspector(db).introspect().get("users")!.hasData).toBe(true);
	});

	test("excludes sqlite internal tables", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" (id INTEGER PRIMARY KEY AUTOINCREMENT)');
		db.run('INSERT INTO "users" DEFAULT VALUES');

		const tables = new Introspector(db).introspect();

		expect(tables.has("users")).toBe(true);

		for (const [name] of tables) {
			expect(name.startsWith("sqlite_")).toBe(false);
		}
	});

	test("returns explicit indexes with columns and uniqueness", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "name" TEXT, "email" TEXT)');
		db.run('CREATE INDEX "ix_users_name" ON "users" ("name")');
		db.run('CREATE UNIQUE INDEX "ux_users_email" ON "users" ("email")');

		const users = new Introspector(db).introspect().get("users")!;

		expect(users.indexes).toHaveLength(2);

		const nameIdx = users.indexes.find((i) => i.name === "ix_users_name")!;

		expect(nameIdx.columns).toEqual(["name"]);
		expect(nameIdx.unique).toBe(false);

		const emailIdx = users.indexes.find((i) => i.name === "ux_users_email")!;

		expect(emailIdx.columns).toEqual(["email"]);
		expect(emailIdx.unique).toBe(true);
	});

	test("excludes autoindex and UNIQUE constraint indexes", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "email" TEXT UNIQUE, "name" TEXT)');
		db.run('CREATE INDEX "ix_users_name" ON "users" ("name")');

		const users = new Introspector(db).introspect().get("users")!;

		expect(users.indexes).toHaveLength(1);
		expect(users.indexes[0]!.name).toBe("ix_users_name");
	});

	test("returns composite index columns in order", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "events" ("id" INTEGER PRIMARY KEY, "type" TEXT, "date" INTEGER)');
		db.run('CREATE INDEX "ix_events_type_date" ON "events" ("type", "date")');

		const events = new Introspector(db).introspect().get("events")!;

		expect(events.indexes).toHaveLength(1);
		expect(events.indexes[0]!.columns).toEqual(["type", "date"]);
	});

	test("returns empty indexes for table without explicit indexes", () => {
		const db = new BunDatabase(":memory:");
		db.run('CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "name" TEXT)');

		expect(new Introspector(db).introspect().get("users")!.indexes).toHaveLength(0);
	});
});

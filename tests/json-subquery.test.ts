import { describe, test, expect, beforeAll } from "bun:test";
import { type } from "arktype";
import { Database, generated } from "../src/index.ts";
import { jsonObjectFrom, jsonArrayFrom } from "kysely/helpers/sqlite";

describe("JSON subqueries", () => {
	const db = new Database({
		path: ":memory:",
		schema: {
			tables: {
				users: type({
					id: generated("autoincrement"),
					name: "string",
				}),
				posts: type({
					id: generated("autoincrement"),
					title: "string",
					user_id: "number",
				}),
				profiles: type({
					id: generated("autoincrement"),
					user_id: "number",
					settings: type({
						theme: "'dark' | 'light'",
						notifications: "boolean",
					}),
				}),
			},
		},
	});

	beforeAll(async () => {
		await db.kysely
			.insertInto("users")
			.values([{ name: "Alice" }, { name: "Bob" }, { name: "Lonely" }])
			.execute();

		await db.kysely
			.insertInto("posts")
			.values([
				{ title: "Post 1", user_id: 1 },
				{ title: "Post 2", user_id: 1 },
				{ title: "Post 3", user_id: 2 },
			])
			.execute();

		await db.kysely
			.insertInto("profiles")
			.values({ user_id: 1, settings: { theme: "dark", notifications: true } })
			.execute();
	});

	test("jsonObjectFrom selects related object", async () => {
		const post = await db.kysely
			.selectFrom("posts")
			.select((eb) => [
				"posts.id",
				"posts.title",
				jsonObjectFrom(
					eb
						.selectFrom("users")
						.select(["users.id", "users.name"])
						.whereRef("users.id", "=", "posts.user_id"),
				).as("author"),
			])
			.where("posts.id", "=", 1)
			.executeTakeFirstOrThrow();

		expect(post.id).toBe(1);
		expect(post.title).toBe("Post 1");
		expect(post.author).toEqual({ id: 1, name: "Alice" });
	});

	test("jsonObjectFrom returns null when no match", async () => {
		const post = await db.kysely
			.selectFrom("posts")
			.select(["posts.id"])
			.select((eb) =>
				jsonObjectFrom(
					eb.selectFrom("users").select(["users.id", "users.name"]).where("users.id", "=", 9999),
				).as("author"),
			)
			.where("posts.id", "=", 1)
			.executeTakeFirstOrThrow();

		expect(post.author).toBeNull();
	});

	test("jsonArrayFrom selects related array", async () => {
		const user = await db.kysely
			.selectFrom("users")
			.select((eb) => [
				"users.id",
				"users.name",
				jsonArrayFrom(
					eb
						.selectFrom("posts")
						.select(["posts.id", "posts.title"])
						.whereRef("posts.user_id", "=", "users.id")
						.orderBy("posts.id"),
				).as("posts"),
			])
			.where("users.id", "=", 1)
			.executeTakeFirstOrThrow();

		expect(user.id).toBe(1);
		expect(user.name).toBe("Alice");
		expect(user.posts).toEqual([
			{ id: 1, title: "Post 1" },
			{ id: 2, title: "Post 2" },
		]);
	});

	test("jsonArrayFrom returns empty array when no matches", async () => {
		const user = await db.kysely
			.selectFrom("users")
			.select((eb) => [
				"users.id",
				jsonArrayFrom(
					eb
						.selectFrom("posts")
						.select(["posts.id", "posts.title"])
						.whereRef("posts.user_id", "=", "users.id"),
				).as("posts"),
			])
			.where("users.id", "=", 3)
			.executeTakeFirstOrThrow();

		expect(user.posts).toEqual([]);
	});

	test("jsonArrayFrom works with multiple rows", async () => {
		const users = await db.kysely
			.selectFrom("users")
			.select((eb) => [
				"users.id",
				"users.name",
				jsonArrayFrom(
					eb
						.selectFrom("posts")
						.select(["posts.id", "posts.title"])
						.whereRef("posts.user_id", "=", "users.id")
						.orderBy("posts.id"),
				).as("posts"),
			])
			.orderBy("users.id")
			.execute();

		expect(users).toHaveLength(3);
		expect(users[0]!.posts).toHaveLength(2);
		expect(users[1]!.posts).toHaveLength(1);
		expect(users[1]!.posts[0]!.title).toBe("Post 3");
		expect(users[2]!.posts).toEqual([]);
	});

	test("jsonObjectFrom coerces date and boolean in subquery", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					users: type({
						id: generated("autoincrement"),
						name: "string",
						active: "boolean",
						created_at: "Date",
					}),
					posts: type({
						id: generated("autoincrement"),
						title: "string",
						user_id: "number",
					}),
				},
			},
		});

		const date = new Date("2025-06-15T12:00:00Z");

		await db.kysely.insertInto("users").values({ name: "Alice", active: true, created_at: date }).execute();
		await db.kysely.insertInto("posts").values({ title: "Post 1", user_id: 1 }).execute();

		const post = await db.kysely
			.selectFrom("posts as p")
			.select((eb) => [
				"p.id",
				jsonObjectFrom(
					eb
						.selectFrom("users as u")
						.select(["u.name", "u.active", "u.created_at"])
						.whereRef("u.id", "=", "p.user_id"),
				).as("author"),
			])
			.executeTakeFirstOrThrow();

		expect(post.author!.active).toBe(true);
		expect(post.author!.created_at).toBeInstanceOf(Date);
	});

	test("jsonArrayFrom coerces date and boolean in subquery", async () => {
		const db = new Database({
			path: ":memory:",
			schema: {
				tables: {
					users: type({
						id: generated("autoincrement"),
						name: "string",
					}),
					tasks: type({
						id: generated("autoincrement"),
						user_id: "number",
						done: "boolean",
						due_at: "Date",
					}),
				},
			},
		});

		const date1 = new Date("2025-06-15T12:00:00Z");
		const date2 = new Date("2025-07-01T00:00:00Z");

		await db.kysely.insertInto("users").values({ name: "Alice" }).execute();
		await db.kysely
			.insertInto("tasks")
			.values([
				{ user_id: 1, done: true, due_at: date1 },
				{ user_id: 1, done: false, due_at: date2 },
			])
			.execute();

		const user = await db.kysely
			.selectFrom("users as u")
			.select((eb) => [
				"u.name",
				jsonArrayFrom(
					eb
						.selectFrom("tasks as t")
						.select(["t.done", "t.due_at"])
						.whereRef("t.user_id", "=", "u.id")
						.orderBy("t.id"),
				).as("tasks"),
			])
			.executeTakeFirstOrThrow();

		expect(user.tasks[0]!.done).toBe(true);
		expect(user.tasks[0]!.due_at).toBeInstanceOf(Date);
		expect(user.tasks[1]!.done).toBe(false);
		expect(user.tasks[1]!.due_at).toBeInstanceOf(Date);
	});

	test("parses nested JSON columns inside subquery", async () => {
		const user = await db.kysely
			.selectFrom("users")
			.select((eb) => [
				"users.id",
				"users.name",
				jsonObjectFrom(
					eb
						.selectFrom("profiles")
						.select(["profiles.id", "profiles.settings"])
						.whereRef("profiles.user_id", "=", "users.id"),
				).as("profile"),
			])
			.where("users.id", "=", 1)
			.executeTakeFirstOrThrow();

		expect(user.profile).toEqual({
			id: 1,
			settings: { theme: "dark", notifications: true },
		});
	});
});

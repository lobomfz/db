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

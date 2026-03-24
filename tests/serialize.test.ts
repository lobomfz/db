import { describe, test, expect } from "bun:test";
import { serializeParam } from "../src/dialect/serialize.ts";

describe("serializeParam", () => {
	test("Date converts to unix seconds", () => {
		const date = new Date("2025-06-15T12:00:00Z");
		expect(serializeParam(date)).toBe(Math.floor(date.getTime() / 1000));
	});

	test("object converts to JSON string", () => {
		expect(serializeParam({ a: 1 })).toBe('{"a":1}');
	});

	test("array converts to JSON string", () => {
		expect(serializeParam([1, 2, 3])).toBe("[1,2,3]");
	});

	test("Uint8Array passes through without stringifying", () => {
		const buf = new Uint8Array([1, 2, 3]);
		expect(serializeParam(buf)).toBe(buf);
	});

	test("Buffer passes through without stringifying", () => {
		const buf = Buffer.from([1, 2, 3]);
		expect(serializeParam(buf)).toBe(buf);
	});

	test("null passes through", () => {
		expect(serializeParam(null)).toBeNull();
	});

	test("string passes through", () => {
		expect(serializeParam("hello")).toBe("hello");
	});

	test("number passes through", () => {
		expect(serializeParam(42)).toBe(42);
	});

	test("boolean passes through", () => {
		expect(serializeParam(true)).toBe(true);
	});
});

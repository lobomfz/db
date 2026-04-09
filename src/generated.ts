import { type } from "arktype";

export type GeneratedPreset = "autoincrement" | "now";

const generatedTypes = {
	autoincrement: () => type("number.integer").configure({ _generated: "autoincrement" }).default(0),
	now: () =>
		type("Date")
			.configure({ _generated: "now" })
			.default(() => new Date(0)),
};

export function generated<P extends GeneratedPreset>(
	preset: P,
): ReturnType<(typeof generatedTypes)[P]> {
	return generatedTypes[preset]() as ReturnType<(typeof generatedTypes)[P]>;
}

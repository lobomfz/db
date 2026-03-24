export class JsonValidationError extends Error {
	constructor(
		readonly table: string,
		readonly column: string,
		readonly summary: string,
	) {
		super(`JSON validation failed for ${table}.${column}: ${summary}`);

		this.name = "JsonValidationError";
	}
}

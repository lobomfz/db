export class JsonParseError extends Error {
	readonly table: string;
	readonly column: string;
	readonly value: string;

	constructor(table: string, column: string, value: string, cause: unknown) {
		super(`Failed to parse JSON in ${table}.${column}`);
		this.name = "JsonParseError";
		this.table = table;
		this.column = column;
		this.value = value;
		this.cause = cause;
	}
}

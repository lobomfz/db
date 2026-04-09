export class ValidationError extends Error {
	constructor(
		readonly table: string,
		readonly summary: string,
		readonly column: string | null = null,
	) {
		super(
			column
				? `Validation failed for ${table}.${column}: ${summary}`
				: `Validation failed for ${table}: ${summary}`,
		);

		this.name = "ValidationError";
	}
}

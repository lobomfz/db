import type { Database } from "bun:sqlite";

import type { CompiledQuery, DatabaseConnection, QueryResult } from "kysely";

import { serializeParam } from "./serialize.js";

export class BunSqliteConnection implements DatabaseConnection {
	readonly #db: Database;

	constructor(db: Database) {
		this.#db = db;
	}

	async executeQuery<O>(compiled: CompiledQuery): Promise<QueryResult<O>> {
		const serializedParams = compiled.parameters.map(serializeParam);

		const stmt = this.#db.query(compiled.sql);

		if (stmt.columnNames.length > 0) {
			return {
				rows: stmt.all(serializedParams as any) as O[],
			};
		}

		const results = stmt.run(serializedParams as any);

		return {
			insertId: BigInt(results.lastInsertRowid),
			numAffectedRows: BigInt(results.changes),
			rows: [],
		};
	}

	async *streamQuery<R>(compiledQuery: CompiledQuery): AsyncIterableIterator<QueryResult<R>> {
		const serializedParams = compiledQuery.parameters.map(serializeParam);

		const stmt = this.#db.prepare(compiledQuery.sql);

		for await (const row of stmt.iterate(serializedParams as any)) {
			yield { rows: [row as R] };
		}
	}
}

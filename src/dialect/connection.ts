import { Database } from "bun:sqlite";
import type { CompiledQuery, DatabaseConnection, QueryResult } from "kysely";
import { serializeParam } from "./serialize";

export class BunSqliteConnection implements DatabaseConnection {
	readonly #db: Database;

	constructor(db: Database) {
		this.#db = db;
	}

	executeQuery<O>(compiledQuery: CompiledQuery): Promise<QueryResult<O>> {
		const { sql, parameters } = compiledQuery;
		const serializedParams = parameters.map(serializeParam);
		const stmt = this.#db.query(sql);

		if (stmt.columnNames.length > 0) {
			return Promise.resolve({
				rows: stmt.all(serializedParams as any) as O[],
			});
		}

		const results = stmt.run(serializedParams as any);

		return Promise.resolve({
			insertId: BigInt(results.lastInsertRowid),
			numAffectedRows: BigInt(results.changes),
			rows: [],
		});
	}

	async *streamQuery<R>(compiledQuery: CompiledQuery): AsyncIterableIterator<QueryResult<R>> {
		const { sql, parameters } = compiledQuery;
		const serializedParams = parameters.map(serializeParam);
		const stmt = this.#db.prepare(sql);

		for await (const row of stmt.iterate(serializedParams as any)) {
			yield { rows: [row as R] };
		}
	}
}

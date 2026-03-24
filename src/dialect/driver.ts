import { Database } from "bun:sqlite";
import { CompiledQuery, type DatabaseConnection, type Driver } from "kysely";
import type { BunSqliteDialectConfig } from "./config";
import { BunSqliteConnection } from "./connection";
import { ConnectionMutex } from "./mutex";

export class BunSqliteDriver implements Driver {
	readonly #config: BunSqliteDialectConfig;
	readonly #connectionMutex = new ConnectionMutex();

	#db?: Database;
	#connection?: DatabaseConnection;

	constructor(config: BunSqliteDialectConfig) {
		this.#config = { ...config };
	}

	async init(): Promise<void> {
		this.#db = this.#config.database;
		this.#connection = new BunSqliteConnection(this.#db);
		await this.#config.onCreateConnection?.(this.#connection);
	}

	async acquireConnection(): Promise<DatabaseConnection> {
		await this.#connectionMutex.lock();
		return this.#connection!;
	}

	async beginTransaction(connection: DatabaseConnection): Promise<void> {
		await connection.executeQuery(CompiledQuery.raw("begin"));
	}

	async commitTransaction(connection: DatabaseConnection): Promise<void> {
		await connection.executeQuery(CompiledQuery.raw("commit"));
	}

	async rollbackTransaction(connection: DatabaseConnection): Promise<void> {
		await connection.executeQuery(CompiledQuery.raw("rollback"));
	}

	// oxlint-disable-next-line require-await
	async releaseConnection(): Promise<void> {
		this.#connectionMutex.unlock();
	}

	// oxlint-disable-next-line require-await
	async destroy(): Promise<void> {
		this.#db?.close();
	}
}

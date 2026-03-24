import type { Database } from "bun:sqlite";
import type { DatabaseConnection } from "kysely";

export interface BunSqliteDialectConfig {
	database: Database;
	onCreateConnection?: (connection: DatabaseConnection) => Promise<void>;
}

import {
	SqliteAdapter,
	SqliteIntrospector,
	SqliteQueryCompiler,
	type DatabaseIntrospector,
	type Dialect,
	type DialectAdapter,
	type Driver,
	type Kysely,
	type QueryCompiler,
} from "kysely";

import type { BunSqliteDialectConfig } from "./config.js";
import { BunSqliteDriver } from "./driver.js";

export class BunSqliteDialect implements Dialect {
	readonly #config: BunSqliteDialectConfig;

	constructor(config: BunSqliteDialectConfig) {
		this.#config = { ...config };
	}

	createDriver(): Driver {
		return new BunSqliteDriver(this.#config);
	}

	createQueryCompiler(): QueryCompiler {
		return new SqliteQueryCompiler();
	}

	createAdapter(): DialectAdapter {
		return new SqliteAdapter();
	}

	createIntrospector(db: Kysely<any>): DatabaseIntrospector {
		return new SqliteIntrospector(db);
	}
}

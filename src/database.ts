import { Database as BunDatabase } from "bun:sqlite";
import { Kysely } from "kysely";
import { BunSqliteDialect } from "./dialect/dialect";
import type { Type } from "arktype";
import type { GeneratedPreset } from "./generated";
import type { DbFieldMeta } from "./env";
import { DeserializePlugin, type ColumnCoercion, type ColumnsMap } from "./plugin";
import type {
	DatabaseOptions,
	IndexDefinition,
	SchemaRecord,
	TablesFromSchemas,
	DatabasePragmas,
} from "./types";

type ArkBranch = {
	domain?: string;
	proto?: unknown;
	unit?: unknown;
	structure?: unknown;
	inner?: { divisor?: unknown };
};

type StructureProp = {
	key: string;
	required: boolean;
	value: Type & {
		branches: ArkBranch[];
		proto?: unknown;
		meta: DbFieldMeta & { _generated?: GeneratedPreset };
	};
	inner: { default?: unknown };
};

type Prop = {
	key: string;
	kind: "required" | "optional";
	domain?: string;
	nullable?: boolean;
	isBoolean?: boolean;
	isInteger?: boolean;
	isDate?: boolean;
	isJson?: boolean;
	jsonSchema?: Type;
	meta?: DbFieldMeta;
	generated?: GeneratedPreset;
	defaultValue?: unknown;
};

const typeMap: Record<string, string> = {
	string: "TEXT",
	number: "REAL",
};

const defaultPragmas: DatabasePragmas = {
	foreign_keys: true,
};

export class Database<T extends SchemaRecord> {
	private sqlite: BunDatabase;

	private columns: ColumnsMap = new Map();

	readonly infer: TablesFromSchemas<T> = undefined as any;

	readonly kysely: Kysely<TablesFromSchemas<T>>;

	constructor(private options: DatabaseOptions<T>) {
		this.sqlite = new BunDatabase(options.path);

		this.applyPragmas();

		this.createTables();

		const validation = {
			onRead: options.validation?.onRead ?? false,
			onWrite: options.validation?.onWrite ?? true,
		};

		this.kysely = new Kysely<TablesFromSchemas<T>>({
			dialect: new BunSqliteDialect({ database: this.sqlite }),
			plugins: [new DeserializePlugin(this.columns, validation)],
		});
	}

	private applyPragmas() {
		const pragmas = { ...defaultPragmas, ...this.options.pragmas };

		if (pragmas.journal_mode) {
			this.sqlite.run(`PRAGMA journal_mode = ${pragmas.journal_mode.toUpperCase()}`);
		}

		if (pragmas.synchronous) {
			this.sqlite.run(`PRAGMA synchronous = ${pragmas.synchronous.toUpperCase()}`);
		}

		this.sqlite.run(`PRAGMA foreign_keys = ${pragmas.foreign_keys ? "ON" : "OFF"}`);

		if (pragmas.busy_timeout_ms !== undefined) {
			this.sqlite.run(`PRAGMA busy_timeout = ${pragmas.busy_timeout_ms}`);
		}
	}

	private normalizeProp(structureProp: StructureProp, parentSchema: Type) {
		const { key, value: v, inner } = structureProp;
		const kind: Prop["kind"] = structureProp.required ? "required" : "optional";
		const generated = v.meta._generated;
		const defaultValue = inner.default;

		const nonNull = v.branches.filter((b) => b.unit !== null);
		const nullable = nonNull.length < v.branches.length;

		if (v.proto === Date || nonNull.some((b) => b.proto === Date)) {
			return { key, kind, nullable, isDate: true, generated, defaultValue };
		}

		if (nonNull.length > 0 && nonNull.every((b) => b.domain === "boolean")) {
			return { key, kind, nullable, isBoolean: true, generated, defaultValue };
		}

		if (nonNull.some((b) => !!b.structure)) {
			return {
				key,
				kind,
				nullable,
				isJson: true,
				jsonSchema: (parentSchema as any).get(key) as Type,
				meta: v.meta,
				generated,
				defaultValue,
			};
		}

		const branch = nonNull[0];

		return {
			key,
			kind,
			nullable,
			domain: branch?.domain,
			isInteger: !!branch?.inner?.divisor,
			meta: v.meta,
			generated,
			defaultValue,
		};
	}

	private sqlType(prop: Prop) {
		if (prop.isJson) {
			return "TEXT";
		}

		if (prop.isDate || prop.isBoolean || prop.isInteger) {
			return "INTEGER";
		}

		if (prop.domain) {
			return typeMap[prop.domain] ?? "TEXT";
		}

		return "TEXT";
	}

	private columnConstraint(prop: Prop) {
		if (prop.generated === "autoincrement") {
			return "PRIMARY KEY AUTOINCREMENT";
		}

		if (prop.meta?.primaryKey) {
			return "PRIMARY KEY";
		}

		if (prop.kind === "required" && !prop.nullable) {
			return "NOT NULL";
		}

		return null;
	}

	private defaultClause(prop: Prop) {
		if (prop.generated === "now") {
			return "DEFAULT (unixepoch())";
		}

		if (prop.defaultValue === undefined || prop.generated === "autoincrement") {
			return null;
		}

		if (prop.defaultValue === null) {
			return "DEFAULT NULL";
		}

		if (typeof prop.defaultValue === "string") {
			return `DEFAULT '${prop.defaultValue}'`;
		}

		if (typeof prop.defaultValue === "number" || typeof prop.defaultValue === "boolean") {
			return `DEFAULT ${prop.defaultValue}`;
		}

		throw new Error(`Unsupported default value type: ${typeof prop.defaultValue}`);
	}

	private columnDef(prop: Prop) {
		return [
			`"${prop.key}"`,
			this.sqlType(prop),
			this.columnConstraint(prop),
			prop.meta?.unique ? "UNIQUE" : null,
			this.defaultClause(prop),
		]
			.filter(Boolean)
			.join(" ");
	}

	private foreignKey(prop: Prop) {
		const ref = prop.meta?.references;

		if (!ref) {
			return null;
		}

		const [table, column] = ref.split(".");

		let fk = `FOREIGN KEY ("${prop.key}") REFERENCES "${table}"("${column}")`;

		const onDelete = prop.meta?.onDelete;

		if (onDelete) {
			fk += ` ON DELETE ${onDelete.toUpperCase()}`;
		}

		return fk;
	}

	private parseSchemaProps(schema: Type) {
		const structureProps = (schema as any).structure?.props as StructureProp[] | undefined;

		if (!structureProps) {
			return [];
		}

		return structureProps.map((p) => this.normalizeProp(p, schema));
	}

	private registerColumns(tableName: string, props: Prop[]) {
		const colMap = new Map<string, ColumnCoercion>();

		for (const prop of props) {
			if (prop.isBoolean) {
				colMap.set(prop.key, "boolean");
				continue;
			}

			if (prop.isDate) {
				colMap.set(prop.key, "date");
				continue;
			}

			if (prop.isJson && prop.jsonSchema) {
				colMap.set(prop.key, { type: "json", schema: prop.jsonSchema });
			}
		}

		if (colMap.size > 0) {
			this.columns.set(tableName, colMap);
		}
	}

	private generateCreateTableSQL(tableName: string, props: Prop[]) {
		const columns: string[] = [];
		const fks: string[] = [];

		for (const prop of props) {
			columns.push(this.columnDef(prop));

			const fk = this.foreignKey(prop);

			if (fk) {
				fks.push(fk);
			}
		}

		return `CREATE TABLE IF NOT EXISTS "${tableName}" (${columns.concat(fks).join(", ")})`;
	}

	private createTables() {
		for (const [name, schema] of Object.entries(this.options.schema.tables)) {
			const props = this.parseSchemaProps(schema);

			this.registerColumns(name, props);
			this.sqlite.run(this.generateCreateTableSQL(name, props));
		}

		this.createIndexes();
	}

	private generateIndexName(tableName: string, columns: string[], unique: boolean) {
		const prefix = unique ? "ux" : "ix";

		return `${prefix}_${tableName}_${columns.join("_")}`;
	}

	private generateCreateIndexSQL(tableName: string, indexDef: IndexDefinition) {
		const indexName = this.generateIndexName(tableName, indexDef.columns, indexDef.unique ?? false);
		const unique = indexDef.unique ? "UNIQUE " : "";
		const columns = indexDef.columns.map((c) => `"${c}"`).join(", ");

		return `CREATE ${unique}INDEX IF NOT EXISTS "${indexName}" ON "${tableName}" (${columns})`;
	}

	private createIndexes() {
		const indexes = this.options.schema.indexes;

		if (!indexes) {
			return;
		}

		for (const [tableName, tableIndexes] of Object.entries(indexes)) {
			if (!tableIndexes) {
				continue;
			}

			for (const indexDef of tableIndexes) {
				this.sqlite.run(this.generateCreateIndexSQL(tableName, indexDef));
			}
		}
	}

	reset(table?: keyof T & string): void {
		const tables = table ? [table] : Object.keys(this.options.schema.tables);

		for (const t of tables) {
			this.sqlite.run(`DELETE FROM "${t}"`);
		}
	}

}

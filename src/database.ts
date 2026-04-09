import { Database as BunDatabase } from "bun:sqlite";

import type { Type } from "arktype";
import { Kysely } from "kysely";

import { BunSqliteDialect } from "./dialect/dialect.js";
import type { DbFieldMeta } from "./env.js";
import type { GeneratedPreset } from "./generated.js";
import { Differ, type DesiredTable } from "./migration/diff.js";
import { Executor } from "./migration/execute.js";
import { Introspector } from "./migration/introspect.js";
import { ResultHydrationPlugin } from "./plugin.js";
import type {
	DatabaseOptions,
	IndexDefinition,
	SchemaRecord,
	TablesFromSchemas,
	DatabasePragmas,
} from "./types.js";
import { WriteValidationPlugin } from "./write-validation-plugin.js";

type ArkBranch = {
	domain?: string;
	proto?: unknown;
	unit?: unknown;
	structure?: unknown;
	inner?: { divisor?: unknown };
	meta?: DbFieldMeta & { _generated?: GeneratedPreset };
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
	isBlob?: boolean;
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

	readonly infer: TablesFromSchemas<T> = undefined as any;

	readonly kysely: Kysely<TablesFromSchemas<T>>;

	constructor(private options: DatabaseOptions<T>) {
		const tableSchemas = new Map<string, Type>(Object.entries(options.schema.tables));
		const writeSchemas = new Map<string, Type>(
			Object.entries(options.schema.tables).map(([table, schema]) => [
				table,
				this.createWriteSchema(schema),
			]),
		);

		this.sqlite = new BunDatabase(options.path);

		this.applyPragmas();

		this.migrate();

		const validation = {
			onRead: options.validation?.onRead ?? false,
		};

		this.kysely = new Kysely<TablesFromSchemas<T>>({
			dialect: new BunSqliteDialect({ database: this.sqlite }),
			plugins: [
				new WriteValidationPlugin(writeSchemas),
				new ResultHydrationPlugin(tableSchemas, validation),
			],
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
		const defaultValue = inner.default;

		const concrete = v.branches.filter((b) => b.unit !== null && b.domain !== "undefined");
		const nullable = concrete.length < v.branches.length;

		const branchMeta = v.branches.find((b) => b.meta && Object.keys(b.meta).length > 0)?.meta;
		const meta = { ...branchMeta, ...v.meta };
		const generated = meta._generated;

		if (v.proto === Date || concrete.some((b) => b.proto === Date)) {
			return { key, kind, nullable, isDate: true, generated, defaultValue };
		}

		if (v.proto === Uint8Array || concrete.some((b) => b.proto === Uint8Array)) {
			return {
				key,
				kind,
				nullable,
				isBlob: true,
				meta,
				generated,
				defaultValue,
			};
		}

		if (concrete.length > 0 && concrete.every((b) => b.domain === "boolean")) {
			return { key, kind, nullable, isBoolean: true, generated, defaultValue };
		}

		if (concrete.some((b) => !!b.structure)) {
			return {
				key,
				kind,
				nullable,
				isJson: true,
				jsonSchema: (parentSchema as any).get(key) as Type,
				meta,
				generated,
				defaultValue,
			};
		}

		const branch = concrete[0];

		return {
			key,
			kind,
			nullable,
			domain: branch?.domain,
			isInteger: !!branch?.inner?.divisor,
			meta,
			generated,
			defaultValue,
		};
	}

	private sqlType(prop: Prop) {
		if (prop.isJson) {
			return "TEXT";
		}

		if (prop.isBlob) {
			return "BLOB";
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

		throw new Error(
			`Unsupported default value type: ${typeof prop.defaultValue} ${JSON.stringify(prop)}`,
		);
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

	private addColumnDef(prop: Prop) {
		let def = this.columnDef(prop);

		const ref = prop.meta?.references;

		if (ref) {
			const [table, column] = ref.split(".");
			def += ` REFERENCES "${table}"("${column}")`;

			if (prop.meta?.onDelete) {
				def += ` ON DELETE ${prop.meta.onDelete.toUpperCase()}`;
			}
		}

		return def;
	}

	private parseSchemaProps(schema: Type) {
		const structureProps = (schema as any).structure?.props as StructureProp[] | undefined;

		if (!structureProps) {
			return [];
		}

		return structureProps.map((p) => this.normalizeProp(p, schema));
	}

	private createWriteSchema(schema: Type) {
		const autoIncrementColumns = this.parseSchemaProps(schema)
			.filter((prop) => prop.generated === "autoincrement")
			.map((prop) => prop.key);

		if (autoIncrementColumns.length === 0) {
			return schema;
		}

		return (schema as any).omit(...autoIncrementColumns) as Type;
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

	private migrate() {
		const desiredTables: DesiredTable[] = [];
		const schemaIndexes = this.options.schema.indexes;

		for (const [name, schema] of Object.entries(this.options.schema.tables)) {
			const props = this.parseSchemaProps(schema);

			const columns = props.map((prop) => {
				const isNotNull = this.columnConstraint(prop) === "NOT NULL";
				const defaultClause = this.defaultClause(prop);
				const hasLiteralDefault = prop.generated !== "now" && defaultClause !== null;

				return {
					name: prop.key,
					addable: !isNotNull || hasLiteralDefault,
					columnDef: this.addColumnDef(prop),
					type: this.sqlType(prop),
					notnull: isNotNull,
					defaultValue: defaultClause
						? defaultClause.replace("DEFAULT ", "").replace(/^\((.+)\)$/, "$1")
						: null,
					unique: !!prop.meta?.unique,
					references: prop.meta?.references ?? null,
					onDelete: prop.meta?.onDelete?.toUpperCase() ?? null,
				};
			});

			const indexes = (schemaIndexes?.[name] ?? []).map((indexDef) => ({
				name: this.generateIndexName(name, indexDef.columns, indexDef.unique ?? false),
				columns: indexDef.columns,
				sql: this.generateCreateIndexSQL(name, indexDef),
			}));

			desiredTables.push({
				name,
				sql: this.generateCreateTableSQL(name, props),
				columns,
				indexes,
			});
		}

		const existing = new Introspector(this.sqlite).introspect();
		const ops = new Differ(desiredTables, existing).diff();

		new Executor(this.sqlite, ops).execute();
	}

	private generateIndexName(tableName: string, columns: string[], unique: boolean) {
		const prefix = unique ? "ux" : "ix";

		return `${prefix}_${tableName}_${columns.join("_")}`;
	}

	private generateCreateIndexSQL(tableName: string, indexDef: IndexDefinition) {
		const indexName = this.generateIndexName(tableName, indexDef.columns, indexDef.unique ?? false);
		const unique = indexDef.unique ? "UNIQUE " : "";
		const columns = indexDef.columns.map((c) => `"${c}"`).join(", ");

		return `CREATE ${unique}INDEX "${indexName}" ON "${tableName}" (${columns})`;
	}

	reset(table?: keyof T & string): void {
		const tables = table ? [table] : Object.keys(this.options.schema.tables);

		this.sqlite.run("PRAGMA foreign_keys = OFF");

		for (const t of tables) {
			this.sqlite.run(`DELETE FROM "${t}"`);
		}

		this.sqlite.run("PRAGMA foreign_keys = ON");
	}
}

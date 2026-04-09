import type { Database } from "bun:sqlite";
import type { IntrospectedColumn, IntrospectedIndex, IntrospectedTable } from "./types";

type TableListRow = {
	name: string;
	type: string;
};

type TableXInfoRow = {
	name: string;
	type: string;
	notnull: number;
	dflt_value: string | null;
};

type IndexListRow = {
	name: string;
	unique: number;
	origin: string;
};

type IndexInfoRow = {
	name: string;
};

type ForeignKeyListRow = {
	from: string;
	table: string;
	to: string;
	on_delete: string;
};

export class Introspector {
	constructor(private db: Database) {}

	introspect() {
		const tables = new Map<string, IntrospectedTable>();
		const tableRows = this.db.prepare("PRAGMA table_list").all() as TableListRow[];

		for (const row of tableRows) {
			if (row.type !== "table" || row.name.startsWith("sqlite_")) {
				continue;
			}

			const indexRows = this.db.prepare(`PRAGMA index_list("${row.name}")`).all() as IndexListRow[];
			const uniqueCols = this.uniqueColumns(indexRows);
			const fkMap = this.foreignKeys(row.name);
			const columns = this.columns(row.name, uniqueCols, fkMap);
			const indexes = this.indexes(indexRows);
			const hasData = this.db.prepare(`SELECT 1 FROM "${row.name}" LIMIT 1`).get() !== null;

			tables.set(row.name, { columns, indexes, hasData });
		}

		return tables;
	}

	private uniqueColumns(indexRows: IndexListRow[]) {
		const unique = new Set<string>();

		for (const idx of indexRows) {
			if (idx.unique !== 1 || idx.origin !== "u") {
				continue;
			}

			const idxCols = this.db.prepare(`PRAGMA index_info("${idx.name}")`).all() as IndexInfoRow[];

			if (idxCols.length === 1) {
				unique.add(idxCols[0]!.name);
			}
		}

		return unique;
	}

	private indexes(indexRows: IndexListRow[]) {
		const indexes: IntrospectedIndex[] = [];

		for (const idx of indexRows) {
			if (idx.origin !== "c") {
				continue;
			}

			const idxCols = this.db.prepare(`PRAGMA index_info("${idx.name}")`).all() as IndexInfoRow[];

			indexes.push({
				name: idx.name,
				columns: idxCols.map((c) => c.name),
				unique: idx.unique === 1,
			});
		}

		return indexes;
	}

	private foreignKeys(table: string) {
		const fkRows = this.db
			.prepare(`PRAGMA foreign_key_list("${table}")`)
			.all() as ForeignKeyListRow[];
		const fkMap = new Map<string, { references: string; onDelete: string | null }>();

		for (const fk of fkRows) {
			fkMap.set(fk.from, {
				references: `${fk.table}.${fk.to}`,
				onDelete: fk.on_delete === "NO ACTION" ? null : fk.on_delete,
			});
		}

		return fkMap;
	}

	private columns(
		table: string,
		uniqueCols: Set<string>,
		fkMap: Map<string, { references: string; onDelete: string | null }>,
	) {
		const colRows = this.db.prepare(`PRAGMA table_xinfo("${table}")`).all() as TableXInfoRow[];
		const columns = new Map<string, IntrospectedColumn>();

		for (const col of colRows) {
			const fk = fkMap.get(col.name);
			const isNotnull = col.notnull === 1;

			columns.set(col.name, {
				name: col.name,
				type: col.type,
				notnull: isNotnull,
				defaultValue: col.dflt_value,
				unique: uniqueCols.has(col.name),
				references: fk?.references ?? null,
				onDelete: fk?.onDelete ?? null,
				hasNulls:
					!isNotnull &&
					this.db.prepare(`SELECT 1 FROM "${table}" WHERE "${col.name}" IS NULL LIMIT 1`).get() !==
						null,
			});
		}

		return columns;
	}
}

import type { IntrospectedColumn, IntrospectedTable, ColumnCopy, MigrationOp } from "./types";

export type DesiredColumn = {
	name: string;
	addable: boolean;
	columnDef: string;
	type: string;
	notnull: boolean;
	defaultValue: string | null;
	unique: boolean;
	references: string | null;
	onDelete: string | null;
};

export type DesiredIndex = {
	name: string;
	sql: string;
};

export type DesiredTable = {
	name: string;
	sql: string;
	columns: DesiredColumn[];
	indexes?: DesiredIndex[];
};

export class Differ {
	private ops: MigrationOp[] = [];
	private desiredNames: Set<string>;
	private rebuiltTables = new Set<string>();

	constructor(
		private desired: DesiredTable[],
		private existing: Map<string, IntrospectedTable>,
	) {
		this.desiredNames = new Set(desired.map((t) => t.name));
	}

	diff() {
		this.diffTables();
		this.dropOrphans();
		this.diffIndexes();
		return this.ops;
	}

	private diffTables() {
		for (const table of this.desired) {
			const existingTable = this.existing.get(table.name);

			if (!existingTable) {
				this.ops.push({ type: "CreateTable", table: table.name, sql: table.sql });
				this.rebuiltTables.add(table.name);
				continue;
			}

			this.diffColumns(table, existingTable);
		}
	}

	private diffColumns(table: DesiredTable, existingTable: IntrospectedTable) {
		const desiredNames = new Set(table.columns.map((c) => c.name));
		const hasRemovedColumns = [...existingTable.columns.keys()].some(
			(name) => !desiredNames.has(name),
		);
		const hasChangedColumns = table.columns.some((col) => {
			const existing = existingTable.columns.get(col.name);

			if (!existing) {
				return false;
			}

			return this.columnChanged(col, existing);
		});

		if (hasRemovedColumns || hasChangedColumns) {
			this.buildRebuild(table, existingTable);
			return;
		}

		this.buildAddColumns(table, existingTable);
	}

	private buildRebuild(table: DesiredTable, existingTable: IntrospectedTable) {
		const columnCopies: ColumnCopy[] = [];

		for (const col of table.columns) {
			const existing = existingTable.columns.get(col.name);

			if (!existing) {
				continue;
			}

			if (col.type !== existing.type) {
				if (col.notnull && col.defaultValue === null && existingTable.hasData) {
					throw new Error(
						`Cannot change type of NOT NULL column "${col.name}" without DEFAULT in table "${table.name}" with existing data`,
					);
				}

				continue;
			}

			if (!existing.notnull && col.notnull && col.defaultValue === null && existing.hasNulls) {
				throw new Error(
					`Cannot make column "${col.name}" NOT NULL without DEFAULT in table "${table.name}" with existing data`,
				);
			}

			if (!existing.notnull && col.notnull && col.defaultValue !== null) {
				columnCopies.push({ name: col.name, expr: `COALESCE("${col.name}", ${col.defaultValue})` });
			} else {
				columnCopies.push({ name: col.name, expr: `"${col.name}"` });
			}
		}

		this.ops.push({ type: "RebuildTable", table: table.name, createSql: table.sql, columnCopies });
		this.rebuiltTables.add(table.name);
	}

	private buildAddColumns(table: DesiredTable, existingTable: IntrospectedTable) {
		const newColumns = table.columns.filter((c) => !existingTable.columns.has(c.name));

		if (newColumns.length === 0) {
			return;
		}

		const nonAddable = newColumns.filter((c) => !c.addable);

		if (nonAddable.length > 0) {
			if (existingTable.hasData) {
				throw new Error(
					`Cannot add NOT NULL column "${nonAddable[0]!.name}" without DEFAULT to table "${table.name}" with existing data`,
				);
			}

			this.ops.push({ type: "DropTable", table: table.name });
			this.ops.push({ type: "CreateTable", table: table.name, sql: table.sql });
			this.rebuiltTables.add(table.name);
			return;
		}

		for (const col of newColumns) {
			this.ops.push({ type: "AddColumn", table: table.name, columnDef: col.columnDef });
		}
	}

	private columnChanged(desired: DesiredColumn, existing: IntrospectedColumn) {
		return (
			desired.type !== existing.type ||
			desired.notnull !== existing.notnull ||
			desired.defaultValue !== existing.defaultValue ||
			desired.unique !== existing.unique ||
			desired.references !== existing.references ||
			desired.onDelete !== existing.onDelete
		);
	}

	private dropOrphans() {
		for (const [name] of this.existing) {
			if (!this.desiredNames.has(name)) {
				this.ops.push({ type: "DropTable", table: name });
			}
		}
	}

	private diffIndexes() {
		for (const table of this.desired) {
			const tableIndexes = table.indexes ?? [];

			if (this.rebuiltTables.has(table.name)) {
				for (const idx of tableIndexes) {
					this.ops.push({ type: "CreateIndex", sql: idx.sql });
				}

				continue;
			}

			const existingTable = this.existing.get(table.name);

			if (!existingTable) {
				continue;
			}

			const existingNames = new Set(existingTable.indexes.map((i) => i.name));
			const desiredNames = new Set(tableIndexes.map((i) => i.name));

			for (const idx of tableIndexes) {
				if (!existingNames.has(idx.name)) {
					this.ops.push({ type: "CreateIndex", sql: idx.sql });
				}
			}

			for (const idx of existingTable.indexes) {
				if (!desiredNames.has(idx.name)) {
					this.ops.push({ type: "DropIndex", index: idx.name });
				}
			}
		}
	}
}

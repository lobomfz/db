import { type, type Type } from "arktype";
import {
	type KyselyPlugin,
	type InsertQueryNode as KyselyInsertQueryNode,
	type UpdateQueryNode as KyselyUpdateQueryNode,
	type OnConflictNode as KyselyOnConflictNode,
	type ColumnUpdateNode as KyselyColumnUpdateNode,
	type ValuesItemNode,
	type OperationNode,
	type RootOperationNode,
	ColumnNode,
	ColumnUpdateNode,
	DefaultInsertValueNode,
	InsertQueryNode,
	OnConflictNode,
	PrimitiveValueListNode,
	ReferenceNode,
	TableNode,
	ValueListNode,
	ValueNode,
	ValuesNode,
} from "kysely";

import type { StructureProp } from "./types.js";
import { ValidationError } from "./validation-error.js";

type TableWriteSchema = {
	schema: Type;
	columns: string[];
	columnSet: Set<string>;
	optionalNonNullColumns: Set<string>;
	insertNullColumns: Set<string>;
};

type InsertRow = {
	values: Record<string, unknown>;
	passthrough: Map<string, OperationNode>;
};

export class WriteValidationPlugin implements KyselyPlugin {
	private schemas = new Map<string, TableWriteSchema>();

	constructor(schemas: Map<string, Type>) {
		this.registerSchemas(schemas);
	}

	transformQuery: KyselyPlugin["transformQuery"] = (args) => {
		return this.transformWriteNode(args.node);
	};

	transformResult: KyselyPlugin["transformResult"] = async (args) => {
		return args.result;
	};

	private registerSchemas(schemas: Map<string, Type>) {
		for (const [table, schema] of schemas) {
			const structureProps = (schema as any).structure?.props as StructureProp[] | undefined;
			const columns = structureProps?.map((prop) => prop.key) ?? [];

			this.schemas.set(table, {
				schema,
				columns,
				columnSet: new Set(columns),
				optionalNonNullColumns: new Set(
					structureProps
						?.filter((prop) => !prop.required && !this.acceptsNull(prop.value))
						.map((prop) => prop.key) ?? [],
				),
				insertNullColumns: new Set(
					structureProps
						?.filter((prop) => this.acceptsNull(prop.value) && prop.inner.default === undefined)
						.map((prop) => prop.key) ?? [],
				),
			});
		}
	}

	private acceptsNull(field: StructureProp["value"]) {
		return field.branches.some((branch) => branch.unit === null || branch.domain === "null");
	}

	private getTableFromNode(node: RootOperationNode) {
		switch (node.kind) {
			case "InsertQueryNode":
				return node.into?.table.identifier.name ?? null;

			case "UpdateQueryNode": {
				if (node.table && TableNode.is(node.table)) {
					return node.table.table.identifier.name;
				}

				return null;
			}

			default:
				return null;
		}
	}

	private firstErrorColumn(result: { [index: number]: { path: ArrayLike<unknown> } | undefined }) {
		const column = result[0]?.path[0];

		if (typeof column === "string") {
			return column;
		}

		return null;
	}

	private morph(table: string, schema: Type, value: Record<string, unknown>) {
		const result = schema(value);

		if (result instanceof type.errors) {
			throw new ValidationError(table, result.summary, this.firstErrorColumn(result));
		}

		return result as Record<string, unknown>;
	}

	private pickSchema(schema: Type, columns: Iterable<string>) {
		return (schema as any).pick(...columns) as Type;
	}

	private stripNullOptionalFields(tableSchema: TableWriteSchema, value: Record<string, unknown>) {
		const stripped = { ...value };

		for (const column of tableSchema.optionalNonNullColumns) {
			if (stripped[column] === null) {
				delete stripped[column];
			}
		}

		return stripped;
	}

	private prepareInsertValue(tableSchema: TableWriteSchema, value: Record<string, unknown>) {
		const prepared = this.stripNullOptionalFields(tableSchema, value);

		for (const column of tableSchema.insertNullColumns) {
			if (!Object.prototype.hasOwnProperty.call(prepared, column)) {
				prepared[column] = null;
			}
		}

		return prepared;
	}

	private transformWriteNode(node: RootOperationNode) {
		const table = this.getTableFromNode(node);

		if (!table) {
			return node;
		}

		if (node.kind === "InsertQueryNode") {
			return this.transformInsert(node, table);
		}

		if (node.kind === "UpdateQueryNode") {
			return this.transformUpdate(node, table);
		}

		return node;
	}

	private getInsertRow(columns: string[], valueList: ValuesItemNode) {
		const row: InsertRow = {
			values: {},
			passthrough: new Map(),
		};

		if (PrimitiveValueListNode.is(valueList)) {
			for (let i = 0; i < columns.length; i++) {
				row.values[columns[i]!] = valueList.values[i];
			}

			return row;
		}

		if (!ValueListNode.is(valueList)) {
			return null;
		}

		for (let i = 0; i < columns.length; i++) {
			const column = columns[i]!;
			const value = valueList.values[i];

			if (!value || DefaultInsertValueNode.is(value)) {
				continue;
			}

			if (ValueNode.is(value)) {
				row.values[column] = value.value;
				continue;
			}

			row.passthrough.set(column, value);
		}

		return row;
	}

	private morphInsertRow(table: string, tableSchema: TableWriteSchema, row: InsertRow) {
		const schemaLiteralValues = Object.fromEntries(
			Object.entries(row.values).filter(([column]) => tableSchema.columnSet.has(column)),
		);
		const nonSchemaLiteralValues = Object.fromEntries(
			Object.entries(row.values).filter(([column]) => !tableSchema.columnSet.has(column)),
		);
		const schemaLiteralColumns = Object.keys(schemaLiteralValues);

		if (schemaLiteralColumns.length === 0) {
			return row;
		}

		const morphedSchemaValues = this.morph(
			table,
			this.pickSchema(tableSchema.schema, schemaLiteralColumns),
			this.prepareInsertValue(tableSchema, schemaLiteralValues),
		);

		return {
			values: { ...nonSchemaLiteralValues, ...morphedSchemaValues },
			passthrough: row.passthrough,
		};
	}

	private getInsertColumns(
		tableSchema: TableWriteSchema,
		originalColumns: string[],
		rows: InsertRow[],
	) {
		const columns = [...originalColumns];

		for (const column of tableSchema.columns) {
			if (columns.includes(column)) {
				continue;
			}

			if (rows.some((row) => Object.prototype.hasOwnProperty.call(row.values, column))) {
				columns.push(column);
			}
		}

		for (const row of rows) {
			for (const column of Object.keys(row.values)) {
				if (!columns.includes(column)) {
					columns.push(column);
				}
			}
		}

		return columns;
	}

	private createInsertValueList(columns: string[], row: InsertRow) {
		return ValueListNode.create(
			columns.map((column) => {
				const passthrough = row.passthrough.get(column);

				if (passthrough) {
					return passthrough;
				}

				if (Object.prototype.hasOwnProperty.call(row.values, column)) {
					return ValueNode.create(row.values[column]);
				}

				return DefaultInsertValueNode.create();
			}),
		);
	}

	private transformInsert(node: KyselyInsertQueryNode, table: string) {
		const onConflict = node.onConflict
			? this.transformOnConflict(table, node.onConflict)
			: undefined;
		const tableSchema = this.schemas.get(table);

		if (!tableSchema) {
			if (onConflict) {
				return InsertQueryNode.cloneWith(node, { onConflict });
			}

			return node;
		}

		const columns = node.columns?.map((column) => column.column.name);

		if (!columns || !node.values || !ValuesNode.is(node.values)) {
			if (onConflict) {
				return InsertQueryNode.cloneWith(node, { onConflict });
			}

			return node;
		}

		const rows: InsertRow[] = [];

		for (const valueList of node.values.values) {
			const row = this.getInsertRow(columns, valueList);

			if (!row) {
				if (onConflict) {
					return InsertQueryNode.cloneWith(node, { onConflict });
				}

				return node;
			}

			rows.push(this.morphInsertRow(table, tableSchema, row));
		}

		const insertColumns = this.getInsertColumns(tableSchema, columns, rows);

		return InsertQueryNode.cloneWith(node, {
			columns: insertColumns.map((column) => ColumnNode.create(column)),
			values: ValuesNode.create(rows.map((row) => this.createInsertValueList(insertColumns, row))),
			onConflict,
		});
	}

	private transformUpdates(table: string, updates: readonly KyselyColumnUpdateNode[]) {
		const tableSchema = this.schemas.get(table);

		if (!tableSchema) {
			return updates;
		}

		const literalColumns = new Set<string>();
		const literalValues: Record<string, unknown> = {};
		const nullOptionalColumns = new Set<string>();

		for (const update of updates) {
			if (!ColumnNode.is(update.column) || !ValueNode.is(update.value)) {
				continue;
			}

			const column = update.column.column.name;

			if (!tableSchema.columnSet.has(column)) {
				continue;
			}

			if (update.value.value === null && tableSchema.optionalNonNullColumns.has(column)) {
				nullOptionalColumns.add(column);
				continue;
			}

			literalColumns.add(column);
			literalValues[column] = update.value.value;
		}

		if (literalColumns.size === 0 && nullOptionalColumns.size === 0) {
			return updates;
		}

		const morphed =
			literalColumns.size === 0
				? {}
				: this.morph(
						table,
						this.pickSchema(tableSchema.schema, literalColumns),
						this.stripNullOptionalFields(tableSchema, literalValues),
					);

		return updates.flatMap((update) => {
			if (!ColumnNode.is(update.column) || !ValueNode.is(update.value)) {
				return [update];
			}

			const column = update.column.column.name;

			if (!literalColumns.has(column) || !tableSchema.columnSet.has(column)) {
				if (nullOptionalColumns.has(column)) {
					return [ColumnUpdateNode.create(update.column, ValueNode.create(null))];
				}

				return [update];
			}

			if (!Object.prototype.hasOwnProperty.call(morphed, column)) {
				return [
					ColumnUpdateNode.create(update.column, ReferenceNode.create(ColumnNode.create(column))),
				];
			}

			return [ColumnUpdateNode.create(update.column, ValueNode.create(morphed[column]))];
		});
	}

	private transformOnConflict(table: string, node: KyselyOnConflictNode) {
		if (!node.updates) {
			return node;
		}

		return OnConflictNode.cloneWith(node, {
			updates: this.transformUpdates(table, node.updates),
		});
	}

	private transformUpdate(node: KyselyUpdateQueryNode, table: string) {
		if (!node.updates) {
			return node;
		}

		return Object.freeze({
			...node,
			updates: this.transformUpdates(table, node.updates),
		});
	}
}

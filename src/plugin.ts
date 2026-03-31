import {
	type KyselyPlugin,
	type RootOperationNode,
	type UnknownRow,
	type QueryId,
	TableNode,
	AliasNode,
	ValuesNode,
	ValueNode,
	ColumnNode,
} from "kysely";
import { type } from "arktype";
import type { Type } from "arktype";
import { JsonParseError } from "./errors";
import { JsonValidationError } from "./validation-error";
import type { JsonValidation } from "./types";

export type ColumnCoercion = "boolean" | "date" | { type: "json"; schema: Type };
export type ColumnsMap = Map<string, Map<string, ColumnCoercion>>;

export class DeserializePlugin implements KyselyPlugin {
	private queryNodes = new WeakMap<QueryId, RootOperationNode>();

	constructor(
		private columns: ColumnsMap,
		private tableColumns: Map<string, Set<string>>,
		private validation: Required<JsonValidation>,
	) {}

	transformQuery: KyselyPlugin["transformQuery"] = (args) => {
		this.queryNodes.set(args.queryId, args.node);

		if (this.validation.onWrite) {
			this.validateWriteNode(args.node);
		}

		return args.node;
	};

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

			case "SelectQueryNode":
			case "DeleteQueryNode": {
				const fromNode = node.from?.froms[0];

				if (!fromNode) {
					return null;
				}

				if (AliasNode.is(fromNode) && TableNode.is(fromNode.node)) {
					return fromNode.node.table.identifier.name;
				}

				if (TableNode.is(fromNode)) {
					return fromNode.table.identifier.name;
				}

				return null;
			}

			default:
				return null;
		}
	}

	private validateJsonValue(table: string, col: string, value: unknown, schema: Type) {
		if (value === null || value === undefined) {
			return;
		}

		const result = schema(value);

		if (result instanceof type.errors) {
			throw new JsonValidationError(table, col, result.summary);
		}
	}

	private validateWriteNode(node: RootOperationNode) {
		if (node.kind !== "InsertQueryNode" && node.kind !== "UpdateQueryNode") {
			return;
		}

		const table = this.getTableFromNode(node);

		if (!table) {
			return;
		}

		const cols = this.columns.get(table);

		if (!cols) {
			return;
		}

		for (const [col, value] of this.writeValues(node)) {
			const coercion = cols.get(col);

			if (!coercion || typeof coercion === "string") {
				continue;
			}

			this.validateJsonValue(table, col, value, coercion.schema);
		}
	}

	private *writeValues(node: RootOperationNode) {
		if (node.kind === "InsertQueryNode") {
			const columns = node.columns?.map((c) => c.column.name);

			if (!columns || !node.values || !ValuesNode.is(node.values)) {
				return;
			}

			for (const valueList of node.values.values) {
				for (let i = 0; i < columns.length; i++) {
					const col = columns[i]!;

					if (valueList.kind === "PrimitiveValueListNode") {
						yield [col, valueList.values[i]] as [string, unknown];
						continue;
					}

					const raw = valueList.values[i];
					yield [col, raw && ValueNode.is(raw) ? raw.value : raw] as [string, unknown];
				}
			}

			return;
		}

		if (node.kind !== "UpdateQueryNode" || !node.updates) {
			return;
		}

		for (const update of node.updates) {
			if (ColumnNode.is(update.column) && ValueNode.is(update.value)) {
				yield [update.column.column.name, update.value.value] as [string, unknown];
			}
		}
	}

	private coerceSingle(table: string, row: UnknownRow, col: string, coercion: ColumnCoercion) {
		if (coercion === "boolean") {
			if (typeof row[col] === "number") {
				row[col] = row[col] === 1;
			}

			return;
		}

		if (coercion === "date") {
			if (typeof row[col] === "number") {
				row[col] = new Date(row[col] * 1000);
			}

			return;
		}

		if (typeof row[col] !== "string") {
			return;
		}

		const value = row[col];

		let parsed: unknown;

		try {
			parsed = JSON.parse(value);
		} catch (e) {
			throw new JsonParseError(table, col, value, e);
		}

		if (this.validation.onRead) {
			this.validateJsonValue(table, col, parsed, coercion.schema);
		}

		row[col] = parsed;
	}

	private coerceRow(table: string, row: UnknownRow, cols: Map<string, ColumnCoercion>) {
		for (const [col, coercion] of cols) {
			if (!(col in row)) {
				continue;
			}

			this.coerceSingle(table, row, col, coercion);
		}
	}

	transformResult: KyselyPlugin["transformResult"] = async (args) => {
		const node = this.queryNodes.get(args.queryId);

		if (!node) {
			return args.result;
		}

		const table = this.getTableFromNode(node);

		if (!table) {
			return args.result;
		}

		const mainCols = this.columns.get(table);
		const mainTableColumns = this.tableColumns.get(table);

		for (const row of args.result.rows) {
			if (mainCols) {
				this.coerceRow(table, row, mainCols);
			}

			for (const col of Object.keys(row)) {
				if (mainTableColumns?.has(col)) {
					continue;
				}

				for (const [otherTable, otherCols] of this.columns) {
					if (otherTable === table) {
						continue;
					}

					const coercion = otherCols.get(col);

					if (coercion) {
						this.coerceSingle(otherTable, row, col, coercion);
						break;
					}
				}
			}
		}

		return { ...args.result };
	};
}

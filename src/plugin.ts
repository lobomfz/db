import { type } from "arktype";
import type { Type } from "arktype";
import {
	type KyselyPlugin,
	type OperationNode,
	type RootOperationNode,
	type UnknownRow,
	type QueryId,
	AggregateFunctionNode,
	TableNode,
	AliasNode,
	ValuesNode,
	ValueNode,
	ColumnNode,
	DefaultInsertValueNode,
	IdentifierNode,
	ReferenceNode,
	ParensNode,
	CastNode,
	SelectQueryNode,
} from "kysely";

import { JsonParseError } from "./errors";
import type { JsonValidation } from "./types";
import { JsonValidationError } from "./validation-error";

export type ColumnCoercion = "boolean" | "date" | { type: "json"; schema: Type };
export type ColumnsMap = Map<string, Map<string, ColumnCoercion>>;

type ResolvedCoercion = { table: string; coercion: ColumnCoercion };

const typePreservingAggregateFunctions = new Set(["max", "min"]);

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

					if (!raw || DefaultInsertValueNode.is(raw)) {
						continue;
					}

					yield [col, ValueNode.is(raw) ? raw.value : raw] as [string, unknown];
				}
			}

			for (const update of node.onConflict?.updates ?? []) {
				if (ColumnNode.is(update.column) && ValueNode.is(update.value)) {
					yield [update.column.column.name, update.value.value] as [string, unknown];
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

	private getIdentifierName(node: OperationNode | undefined) {
		if (!node || !IdentifierNode.is(node)) {
			return null;
		}

		return node.name;
	}

	private addTableScopeEntry(scope: Map<string, string>, node: OperationNode) {
		if (AliasNode.is(node) && TableNode.is(node.node)) {
			const alias = this.getIdentifierName(node.alias);
			const table = node.node.table.identifier.name;

			scope.set(table, table);

			if (alias) {
				scope.set(alias, table);
			}

			return;
		}

		if (TableNode.is(node)) {
			const table = node.table.identifier.name;
			scope.set(table, table);
		}
	}

	private getTableScope(node: SelectQueryNode) {
		const scope = new Map<string, string>();

		for (const fromNode of node.from?.froms ?? []) {
			this.addTableScopeEntry(scope, fromNode);
		}

		for (const join of node.joins ?? []) {
			this.addTableScopeEntry(scope, join.table);
		}

		return scope;
	}

	private resolveReferenceCoercion(node: ReferenceNode | ColumnNode, scope: Map<string, string>) {
		const column = ColumnNode.is(node)
			? node.column.name
			: ColumnNode.is(node.column)
				? node.column.column.name
				: null;

		if (!column) {
			return null;
		}

		if (ReferenceNode.is(node) && node.table) {
			const tableRef = node.table.table.identifier.name;
			const table = scope.get(tableRef) ?? tableRef;
			const coercion = this.columns.get(table)?.get(column);

			if (!coercion) {
				return null;
			}

			return { table, coercion } satisfies ResolvedCoercion;
		}

		let match: ResolvedCoercion | null = null;
		const resolvedTables = new Set<string>();

		for (const table of scope.values()) {
			if (resolvedTables.has(table)) {
				continue;
			}

			resolvedTables.add(table);

			const coercion = this.columns.get(table)?.get(column);

			if (!coercion) {
				continue;
			}

			if (match) {
				return null;
			}

			match = { table, coercion };
		}

		return match;
	}

	private resolveSelectionCoercion(
		node: OperationNode,
		scope: Map<string, string>,
	): ResolvedCoercion | null {
		if (AliasNode.is(node)) {
			return this.resolveSelectionCoercion(node.node, scope);
		}

		if (ReferenceNode.is(node) || ColumnNode.is(node)) {
			return this.resolveReferenceCoercion(node, scope);
		}

		if (SelectQueryNode.is(node)) {
			return this.resolveScalarSubqueryCoercion(node);
		}

		if (AggregateFunctionNode.is(node)) {
			if (
				node.aggregated.length !== 1 ||
				!typePreservingAggregateFunctions.has(node.func.toLowerCase())
			) {
				return null;
			}

			return this.resolveSelectionCoercion(node.aggregated[0]!, scope);
		}

		if (ParensNode.is(node)) {
			return this.resolveSelectionCoercion(node.node, scope);
		}

		if (CastNode.is(node)) {
			return null;
		}

		return null;
	}

	private resolveScalarSubqueryCoercion(node: SelectQueryNode) {
		if (!node.selections || node.selections.length !== 1) {
			return null;
		}

		return this.resolveSelectionCoercion(node.selections[0]!.selection, this.getTableScope(node));
	}

	private getSelectionOutputName(node: OperationNode) {
		if (AliasNode.is(node)) {
			return this.getIdentifierName(node.alias);
		}

		if (ReferenceNode.is(node) && ColumnNode.is(node.column)) {
			return node.column.column.name;
		}

		if (ColumnNode.is(node)) {
			return node.column.name;
		}

		return null;
	}

	private getSelectCoercions(node: RootOperationNode) {
		const result = new Map<string, ResolvedCoercion>();

		if (node.kind !== "SelectQueryNode" || !node.selections) {
			return result;
		}

		const scope = this.getTableScope(node);

		for (const selectionNode of node.selections) {
			const output = this.getSelectionOutputName(selectionNode.selection);

			if (!output) {
				continue;
			}

			const resolved = this.resolveSelectionCoercion(selectionNode.selection, scope);

			if (resolved) {
				result.set(output, resolved);
			}
		}

		return result;
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
		const selectCoercions = this.getSelectCoercions(node);

		for (const row of args.result.rows) {
			if (mainCols) {
				this.coerceRow(table, row, mainCols);
			}

			for (const col of Object.keys(row)) {
				const resolved = selectCoercions.get(col);

				if (resolved) {
					this.coerceSingle(resolved.table, row, col, resolved.coercion);
					continue;
				}

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

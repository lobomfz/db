import { type } from "arktype";
import type { Type } from "arktype";
import {
	type KyselyPlugin,
	type OperationNode,
	type QueryId,
	type RootOperationNode,
	type UnknownRow,
	AggregateFunctionNode,
	AliasNode,
	CastNode,
	ColumnNode,
	IdentifierNode,
	ParensNode,
	ReferenceNode,
	SelectQueryNode,
	TableNode,
} from "kysely";

import { JsonParseError } from "./errors.js";
import type { StructureProp } from "./types.js";
import { ValidationError } from "./validation-error.js";

type ColumnCoercion = "boolean" | "date" | { type: "json"; schema: Type };

type ResolvedCoercion = { table: string; coercion: ColumnCoercion };

type CoercionPlan = {
	kind: "coercion";
	table: string;
	column: string;
	coercion: ColumnCoercion;
};

type ObjectPlan = {
	kind: "object";
	table: string;
	column: string;
	fields: Map<string, ValuePlan>;
};

type ArrayPlan = {
	kind: "array";
	table: string;
	column: string;
	fields: Map<string, ValuePlan>;
};

type ValuePlan = CoercionPlan | ObjectPlan | ArrayPlan;

type QueryPlan = {
	table: string | null;
	selectionPlans: Map<string, ValuePlan>;
};

type JsonHelper = {
	kind: "object" | "array";
	query: SelectQueryNode;
};

type RawOperationNode = OperationNode & {
	kind: "RawNode";
	sqlFragments: readonly string[];
	parameters: readonly OperationNode[];
};

const jsonArrayFromFragments = [
	"(select coalesce(json_group_array(json_object(",
	")), '[]') from ",
	" as agg)",
] as const;

const jsonObjectFromFragments = [
	"(select json_object(",
	") from ",
	" as obj)",
] as const;

const typePreservingAggregateFunctions = new Set(["max", "min"]);

export class ResultHydrationPlugin implements KyselyPlugin {
	private columns = new Map<string, Map<string, ColumnCoercion>>();
	private tableColumns = new Map<string, Set<string>>();
	private queryPlans = new WeakMap<QueryId, QueryPlan>();

	constructor(
		schemas: Map<string, Type>,
		private validation: { onRead: boolean },
	) {
		this.registerSchemas(schemas);
	}

	private registerSchemas(schemas: Map<string, Type>) {
		for (const [table, schema] of schemas) {
			const structureProps = (schema as any).structure?.props as StructureProp[] | undefined;

			if (!structureProps) {
				continue;
			}

			this.tableColumns.set(
				table,
				new Set(structureProps.map((prop) => prop.key)),
			);

			const columns = new Map<string, ColumnCoercion>();

			for (const prop of structureProps) {
				const coercion = this.getColumnCoercion(prop, schema);

				if (!coercion) {
					continue;
				}

				columns.set(prop.key, coercion);
			}

			if (columns.size > 0) {
				this.columns.set(table, columns);
			}
		}
	}

	private getColumnCoercion(prop: StructureProp, parentSchema: Type) {
		const concrete = prop.value.branches.filter(
			(branch) => branch.unit !== null && branch.domain !== "undefined",
		);

		if (prop.value.proto === Date || concrete.some((branch) => branch.proto === Date)) {
			return "date" satisfies ColumnCoercion;
		}

		if (concrete.length > 0 && concrete.every((branch) => branch.domain === "boolean")) {
			return "boolean" satisfies ColumnCoercion;
		}

		if (concrete.some((branch) => !!branch.structure)) {
			return {
				type: "json",
				schema: (parentSchema as any).get(prop.key) as Type,
			} satisfies ColumnCoercion;
		}

		return null;
	}

	transformQuery: KyselyPlugin["transformQuery"] = (args) => {
		this.queryPlans.set(args.queryId, {
			table: this.getTableFromNode(args.node),
			selectionPlans: this.getSelectionPlans(args.node),
		});

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
			throw new ValidationError(table, result.summary, col);
		}
	}

	private parseJson(table: string, column: string, value: string) {
		try {
			return JSON.parse(value);
		} catch (e) {
			throw new JsonParseError(table, column, value, e);
		}
	}

	private hydrateCoercion(plan: CoercionPlan, value: unknown) {
		if (value === null || value === undefined) {
			return value;
		}

		if (plan.coercion === "boolean") {
			if (typeof value === "number") {
				return value === 1;
			}

			return value;
		}

		if (plan.coercion === "date") {
			if (typeof value === "number") {
				return new Date(value * 1000);
			}

			return value;
		}

		const parsed =
			typeof value === "string" ? this.parseJson(plan.table, plan.column, value) : value;

		if (this.validation.onRead) {
			this.validateJsonValue(plan.table, plan.column, parsed, plan.coercion.schema);
		}

		return parsed;
	}

	private parseStructuredValue(table: string, column: string, value: unknown) {
		if (value === null || value === undefined) {
			return value;
		}

		if (typeof value === "string") {
			return this.parseJson(table, column, value);
		}

		return value;
	}

	private isPlainObject(value: unknown): value is Record<string, unknown> {
		return typeof value === "object" && value !== null && !Array.isArray(value);
	}

	private hydrateObject(plan: ObjectPlan, value: unknown) {
		const parsed = this.parseStructuredValue(plan.table, plan.column, value);

		if (!this.isPlainObject(parsed)) {
			return parsed;
		}

		for (const [field, fieldPlan] of plan.fields) {
			if (!(field in parsed)) {
				continue;
			}

			parsed[field] = this.hydrateValue(fieldPlan, parsed[field]);
		}

		return parsed;
	}

	private hydrateArray(plan: ArrayPlan, value: unknown) {
		const parsed = this.parseStructuredValue(plan.table, plan.column, value);

		if (!Array.isArray(parsed)) {
			return parsed;
		}

		for (let i = 0; i < parsed.length; i++) {
			const item = parsed[i];

			if (!this.isPlainObject(item)) {
				continue;
			}

			for (const [field, fieldPlan] of plan.fields) {
				if (!(field in item)) {
					continue;
				}

				item[field] = this.hydrateValue(fieldPlan, item[field]);
			}
		}

		return parsed;
	}

	private hydrateValue(plan: ValuePlan, value: unknown): unknown {
		if (plan.kind === "coercion") {
			return this.hydrateCoercion(plan, value);
		}

		if (plan.kind === "object") {
			return this.hydrateObject(plan, value);
		}

		return this.hydrateArray(plan, value);
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

	private isRawNode(node: OperationNode): node is RawOperationNode {
		return node.kind === "RawNode";
	}

	private matchesFragments(
		fragments: readonly string[],
		expected: readonly [string, string, string],
	) {
		return (
			fragments.length === expected.length &&
			fragments.every((fragment, index) => fragment === expected[index])
		);
	}

	private getJsonHelper(node: RawOperationNode): JsonHelper | null {
		const query = node.parameters[1];

		if (!query || !SelectQueryNode.is(query)) {
			return null;
		}

		if (this.matchesFragments(node.sqlFragments, jsonObjectFromFragments)) {
			return { kind: "object", query };
		}

		if (this.matchesFragments(node.sqlFragments, jsonArrayFromFragments)) {
			return { kind: "array", query };
		}

		return null;
	}

	private getStructuredFieldPlans(node: SelectQueryNode) {
		const result = new Map<string, ValuePlan>();

		if (!node.selections) {
			return result;
		}

		const scope = this.getTableScope(node);

		for (const selectionNode of node.selections) {
			const output = this.getSelectionOutputName(selectionNode.selection);

			if (!output) {
				continue;
			}

			const plan = this.resolveSelectionPlan(selectionNode.selection, scope, output);

			if (plan) {
				result.set(output, plan);
			}
		}

		return result;
	}

	private resolveJsonHelperPlan(node: RawOperationNode, output: string | null): ValuePlan | null {
		if (!output) {
			return null;
		}

		const helper = this.getJsonHelper(node);

		if (!helper) {
			return null;
		}

		const table = this.getTableFromNode(helper.query) ?? output;
		const fields = this.getStructuredFieldPlans(helper.query);

		if (helper.kind === "object") {
			return { kind: "object", table, column: output, fields };
		}

		return { kind: "array", table, column: output, fields };
	}

	private resolveSelectionPlan(
		node: OperationNode,
		scope: Map<string, string>,
		output: string | null,
	): ValuePlan | null {
		if (AliasNode.is(node)) {
			return this.resolveSelectionPlan(node.node, scope, output ?? this.getIdentifierName(node.alias));
		}

		if (ReferenceNode.is(node) || ColumnNode.is(node)) {
			if (!output) {
				return null;
			}

			const resolved = this.resolveReferenceCoercion(node, scope);

			if (!resolved) {
				return null;
			}

			return {
				kind: "coercion",
				table: resolved.table,
				column: output,
				coercion: resolved.coercion,
			};
		}

		if (SelectQueryNode.is(node)) {
			return this.resolveScalarSubqueryPlan(node, output);
		}

		if (AggregateFunctionNode.is(node)) {
			if (
				node.aggregated.length !== 1 ||
				!typePreservingAggregateFunctions.has(node.func.toLowerCase())
			) {
				return null;
			}

			return this.resolveSelectionPlan(node.aggregated[0]!, scope, output);
		}

		if (ParensNode.is(node)) {
			return this.resolveSelectionPlan(node.node, scope, output);
		}

		if (CastNode.is(node)) {
			return null;
		}

		if (this.isRawNode(node)) {
			return this.resolveJsonHelperPlan(node, output);
		}

		return null;
	}

	private resolveScalarSubqueryPlan(node: SelectQueryNode, output: string | null) {
		if (!node.selections || node.selections.length !== 1) {
			return null;
		}

		return this.resolveSelectionPlan(node.selections[0]!.selection, this.getTableScope(node), output);
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

	private getSelectionPlans(node: RootOperationNode) {
		const result = new Map<string, ValuePlan>();

		if (node.kind !== "SelectQueryNode" || !node.selections) {
			return result;
		}

		const scope = this.getTableScope(node);

		for (const selectionNode of node.selections) {
			const output = this.getSelectionOutputName(selectionNode.selection);

			if (!output) {
				continue;
			}

			const plan = this.resolveSelectionPlan(selectionNode.selection, scope, output);

			if (plan) {
				result.set(output, plan);
			}
		}

		return result;
	}

	private coerceMainRow(table: string, row: UnknownRow, cols: Map<string, ColumnCoercion>) {
		for (const [column, coercion] of cols) {
			if (!(column in row)) {
				continue;
			}

			row[column] = this.hydrateCoercion({
				kind: "coercion",
				table,
				column,
				coercion,
			}, row[column]);
		}
	}

	transformResult: KyselyPlugin["transformResult"] = async (args) => {
		const plan = this.queryPlans.get(args.queryId);

		if (!plan) {
			return args.result;
		}

		const mainCols = plan.table ? this.columns.get(plan.table) : null;
		const mainTableColumns = plan.table ? this.tableColumns.get(plan.table) : null;

		for (const row of args.result.rows) {
			if (plan.table && mainCols) {
				this.coerceMainRow(plan.table, row, mainCols);
			}

			for (const [column, selectionPlan] of plan.selectionPlans) {
				if (!(column in row)) {
					continue;
				}

				row[column] = this.hydrateValue(selectionPlan, row[column]);
			}

			if (!plan.table) {
				continue;
			}

			for (const column of Object.keys(row)) {
				if (plan.selectionPlans.has(column) || mainTableColumns?.has(column)) {
					continue;
				}

				for (const [otherTable, otherCols] of this.columns) {
					if (otherTable === plan.table) {
						continue;
					}

					const coercion = otherCols.get(column);

					if (!coercion) {
						continue;
					}

					row[column] = this.hydrateCoercion({
						kind: "coercion",
						table: otherTable,
						column,
						coercion,
					}, row[column]);

					break;
				}
			}
		}

		return args.result;
	};
}

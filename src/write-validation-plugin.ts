import { type, type Type } from "arktype";
import {
	type KyselyPlugin,
	type RootOperationNode,
	ColumnNode,
	DefaultInsertValueNode,
	TableNode,
	ValueNode,
	ValuesNode,
} from "kysely";

import type { ColumnsMap } from "./plugin.js";
import { JsonValidationError } from "./validation-error.js";

export class WriteValidationPlugin implements KyselyPlugin {
	constructor(
		private columns: ColumnsMap,
		private validation: { onWrite: boolean },
	) {}

	transformQuery: KyselyPlugin["transformQuery"] = (args) => {
		if (this.validation.onWrite) {
			this.validateWriteNode(args.node);
		}

		return args.node;
	};

	transformResult: KyselyPlugin["transformResult"] = async (args) => {
		return args.result;
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
			const columns = node.columns?.map((column) => column.column.name);

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
}

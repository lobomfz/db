import { type KyselyPlugin, type RootOperationNode, type UnknownRow } from "kysely";
import { type } from "arktype";
import type { Type } from "arktype";
import { ValueSerializer } from "./serializer";
import { JsonParseError } from "./errors";
import { JsonValidationError } from "./validation-error";

export type ColumnCoercion = "boolean" | "date" | { type: "json"; schema: Type };
export type ColumnsMap = Map<string, Map<string, ColumnCoercion>>;

export class CoercionPlugin implements KyselyPlugin {
	private serializer = new ValueSerializer();
	private queryNodes = new Map<unknown, RootOperationNode>();

	constructor(private columns: ColumnsMap) {}

	transformQuery: KyselyPlugin["transformQuery"] = (args) => {
		this.queryNodes.set(args.queryId, args.node);

		return this.serializer.transformNode(args.node);
	};

	private getTableFromNode(node: RootOperationNode): string | null {
		switch (node.kind) {
			case "InsertQueryNode":
				return (node as any).into?.table?.identifier?.name ?? null;

			case "UpdateQueryNode":
				return (node as any).table?.table?.identifier?.name ?? null;

			case "SelectQueryNode":
			case "DeleteQueryNode": {
				const fromNode = (node as any).from?.froms?.[0];

				if (fromNode?.kind === "AliasNode") {
					return fromNode.node?.table?.identifier?.name ?? null;
				}

				return fromNode?.table?.identifier?.name ?? null;
			}

			default:
				return null;
		}
	}

	private coerceRow(table: string, row: UnknownRow, cols: Map<string, ColumnCoercion>) {
		for (const [col, coercion] of cols) {
			if (!(col in row)) {
				continue;
			}

			if (coercion === "boolean") {
				if (typeof row[col] === "number") {
					row[col] = row[col] === 1;
				}

				continue;
			}

			if (coercion === "date") {
				if (typeof row[col] === "number") {
					row[col] = new Date(row[col] * 1000);
				}

				continue;
			}

			if (typeof row[col] !== "string") {
				continue;
			}

			const value = row[col];

			let parsed: unknown;

			try {
				parsed = JSON.parse(value);
			} catch (e) {
				throw new JsonParseError(table, col, value, e);
			}

			const validated = coercion.schema(parsed);

			if (validated instanceof type.errors) {
				throw new JsonValidationError(table, col, validated.summary);
			}

			row[col] = validated;
		}
	}

	// oxlint-disable-next-line require-await
	transformResult: KyselyPlugin["transformResult"] = async (args) => {
		const node = this.queryNodes.get(args.queryId);
		this.queryNodes.delete(args.queryId);

		if (!node) {
			return args.result;
		}

		const table = this.getTableFromNode(node);

		if (!table) {
			return args.result;
		}

		const cols = this.columns.get(table);

		if (!cols) {
			return args.result;
		}

		for (const row of args.result.rows) {
			this.coerceRow(table, row, cols);
		}

		return { ...args.result };
	};
}

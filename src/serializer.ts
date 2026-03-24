import { OperationNodeTransformer, type ValueNode } from "kysely";

export class ValueSerializer extends OperationNodeTransformer {
	private serializeValue(v: unknown): unknown {
		if (v instanceof Date) {
			return Math.floor(v.getTime() / 1000);
		}

		if (typeof v === "object" && v !== null && !ArrayBuffer.isView(v)) {
			return JSON.stringify(v);
		}

		return v;
	}

	protected override transformValue(node: ValueNode): ValueNode {
		const serialized = this.serializeValue(node.value);

		if (serialized !== node.value) {
			return super.transformValue({ ...node, value: serialized });
		}

		return super.transformValue(node);
	}
}

import type { DesiredColumn } from "../../src/migration/diff";

export function col(name: string, overrides?: Partial<DesiredColumn>): DesiredColumn {
	return {
		name,
		addable: true,
		columnDef: `"${name}" TEXT`,
		type: "TEXT",
		notnull: false,
		defaultValue: null,
		unique: false,
		references: null,
		onDelete: null,
		...overrides,
	};
}

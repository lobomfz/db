export type CreateTableOp = {
	type: "CreateTable";
	table: string;
	sql: string;
};

export type DropTableOp = {
	type: "DropTable";
	table: string;
};

export type AddColumnOp = {
	type: "AddColumn";
	table: string;
	columnDef: string;
};

export type ColumnCopy = {
	name: string;
	expr: string;
};

export type RebuildTableOp = {
	type: "RebuildTable";
	table: string;
	createSql: string;
	columnCopies: ColumnCopy[];
};

export type CreateIndexOp = {
	type: "CreateIndex";
	table: string;
	columns: string[];
	sql: string;
};

export type DropIndexOp = {
	type: "DropIndex";
	index: string;
};

export type MigrationOp =
	| CreateTableOp
	| DropTableOp
	| AddColumnOp
	| RebuildTableOp
	| CreateIndexOp
	| DropIndexOp;

export type ColumnSchema = {
	name: string;
	type: string;
	notnull: boolean;
	defaultValue: string | null;
	unique: boolean;
	references: string | null;
	onDelete: string | null;
};

export interface IntrospectedColumn extends ColumnSchema {
	hasNulls: boolean;
}

export type IntrospectedIndex = {
	name: string;
	columns: string[];
	unique: boolean;
};

export type IntrospectedTable = {
	columns: Map<string, IntrospectedColumn>;
	indexes: IntrospectedIndex[];
	hasData: boolean;
};

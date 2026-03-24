import type { Generated } from "kysely";
import type { Type } from "arktype";

type ExtractInput<T> = T extends { inferIn: infer I } ? I : never;
type ExtractOutput<T> = T extends { infer: infer O } ? O : never;

type IsOptional<T, K extends keyof T> = undefined extends T[K] ? true : false;

type TransformColumn<T> = T extends (infer U)[] ? U[] : T;

type TransformField<TOutput, TInput, K extends keyof TOutput & keyof TInput> =
	IsOptional<TInput, K> extends true
		? IsOptional<TOutput, K> extends true
			? TransformColumn<NonNullable<TOutput[K]>> | null
			: Generated<TransformColumn<NonNullable<TOutput[K]>>>
		: TransformColumn<TOutput[K]>;

type TransformTable<TOutput, TInput> = {
	[K in keyof TOutput & keyof TInput]: TransformField<TOutput, TInput, K>;
};

export type SchemaRecord = Record<string, Type>;

export type InferTableType<T> = TransformTable<ExtractOutput<T>, ExtractInput<T>>;

export type TablesFromSchemas<T extends SchemaRecord> = {
	[K in keyof T]: InferTableType<T[K]>;
};

type TableColumns<T extends SchemaRecord, K extends keyof T> = keyof ExtractOutput<T[K]> & string;

export type IndexDefinition<Columns extends string = string> = {
	columns: Columns[];
	unique?: boolean;
};

export type IndexesConfig<T extends SchemaRecord> = {
	[K in keyof T]?: IndexDefinition<TableColumns<T, K>>[];
};

export type DatabasePragmas = {
	journal_mode?: "delete" | "truncate" | "persist" | "memory" | "wal" | "off";
	synchronous?: "off" | "normal" | "full" | "extra";
	foreign_keys?: boolean;
	busy_timeout_ms?: number;
};

export type DatabaseSchema<T extends SchemaRecord> = {
	tables: T;
	indexes?: IndexesConfig<T>;
};

export type DatabaseOptions<T extends SchemaRecord> = {
	path: string;
	schema: DatabaseSchema<T>;
	pragmas?: DatabasePragmas;
};

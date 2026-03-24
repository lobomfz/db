export type DbFieldMeta = {
	primaryKey?: boolean;
	unique?: boolean;
	references?: `${string}.${string}`;
	onDelete?: "cascade" | "set null" | "restrict";
};

declare global {
	interface ArkEnv {
		meta(): DbFieldMeta;
	}
}

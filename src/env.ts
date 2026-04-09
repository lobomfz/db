export type DbFieldMeta = {
	primaryKey?: boolean;
	unique?: boolean;
	references?: `${string}.${string}`;
	onDelete?: "cascade" | "set null" | "restrict";
	_generated?: "autoincrement" | "now";
};

declare global {
	interface ArkEnv {
		meta(): DbFieldMeta;
	}
}

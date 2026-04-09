import { configure } from "arktype/config";

const presets: Record<string, Parameters<typeof configure>[0]> = {
	loose: {
		onUndeclaredKey: "delete",
		clone: false,
		exactOptionalPropertyTypes: false,
	},
};

const preset = presets[Bun.env.ARKTYPE_PRESET ?? ""];

if (preset) {
	configure(preset);
}

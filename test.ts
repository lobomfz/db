const presets = ["default", "loose"];

type Result = { preset: string; ok: boolean };
const results: Result[] = [];

for (const preset of presets) {
	console.log(`\n${"=".repeat(50)}`);
	console.log(`  ${preset}`);
	console.log("=".repeat(50));

	const proc = Bun.spawn(["bun", "test"], {
		env: { ...Bun.env, ARKTYPE_PRESET: preset },
		stdout: "inherit",
		stderr: "inherit",
	});

	const code = await proc.exited;
	results.push({ preset, ok: code === 0 });
}

console.log(`\n${"=".repeat(50)}`);

for (const r of results) {
	console.log(`  ${r.ok ? "PASS" : "FAIL"}  ${r.preset}`);
}

if (results.some((r) => !r.ok)) {
	process.exit(1);
}

export function serializeParam(p: unknown): unknown {
	if (p instanceof Date) {
		return Math.floor(p.getTime() / 1000);
	}

	if (typeof p === "object" && p !== null && !ArrayBuffer.isView(p)) {
		return JSON.stringify(p);
	}

	return p;
}

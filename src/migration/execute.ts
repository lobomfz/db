import type { Database } from "bun:sqlite";
import type { MigrationOp, RebuildTableOp } from "./types";

export class Executor {
	constructor(
		private db: Database,
		private ops: MigrationOp[],
	) {}

	execute() {
		if (this.ops.length === 0) {
			return;
		}

		const hasRebuild = this.ops.some((op) => op.type === "RebuildTable");
		let restoreFk = false;

		if (hasRebuild) {
			const { foreign_keys } = this.db.prepare("PRAGMA foreign_keys").get() as {
				foreign_keys: number;
			};

			if (foreign_keys === 1) {
				this.db.run("PRAGMA foreign_keys = OFF");
				restoreFk = true;
			}
		}

		try {
			this.db.transaction(() => {
				for (const op of this.ops) {
					this.executeOp(op);
				}

				if (restoreFk) {
					const violations = this.db.prepare("PRAGMA foreign_key_check").all();

					if (violations.length > 0) {
						throw new Error("Foreign key check failed after rebuild");
					}
				}
			})();
		} finally {
			if (restoreFk) {
				this.db.run("PRAGMA foreign_keys = ON");
			}
		}
	}

	private executeOp(op: MigrationOp) {
		if (op.type === "CreateTable") {
			this.db.run(op.sql);
			return;
		}

		if (op.type === "DropTable") {
			this.db.run(`DROP TABLE "${op.table}"`);
			return;
		}

		if (op.type === "AddColumn") {
			this.db.run(`ALTER TABLE "${op.table}" ADD COLUMN ${op.columnDef}`);
			return;
		}

		if (op.type === "RebuildTable") {
			this.rebuildTable(op);
			return;
		}

		if (op.type === "CreateIndex") {
			this.db.run(op.sql);
			return;
		}

		if (op.type === "DropIndex") {
			this.db.run(`DROP INDEX "${op.index}"`);
		}
	}

	private rebuildTable(op: RebuildTableOp) {
		const tempName = `__new_${op.table}`;
		const tempSql = op.createSql.replace(
			`CREATE TABLE IF NOT EXISTS "${op.table}"`,
			`CREATE TABLE "${tempName}"`,
		);

		this.db.run(tempSql);

		if (op.columnCopies.length > 0) {
			const destCols = op.columnCopies.map((c) => `"${c.name}"`).join(", ");
			const srcExprs = op.columnCopies.map((c) => c.expr).join(", ");

			this.db.run(`INSERT INTO "${tempName}" (${destCols}) SELECT ${srcExprs} FROM "${op.table}"`);
		}

		this.db.run(`DROP TABLE "${op.table}"`);
		this.db.run(`ALTER TABLE "${tempName}" RENAME TO "${op.table}"`);
	}
}

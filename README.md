# @lobomfz/db

SQLite database with Arktype schemas and typed Kysely client for Bun.

## Install

```bash
bun add @lobomfz/db arktype kysely
```

## Usage

```typescript
import { Database, generated, type } from "@lobomfz/db";

const db = new Database({
	path: "data.db",
	schema: {
		tables: {
			users: type({
				id: generated("autoincrement"),
				name: "string",
				email: type("string").configure({ unique: true }),
				"bio?": "string", // optional → nullable in SQLite
				active: type("boolean").default(true),
				created_at: generated("now"), // defaults to current time
			}),
			posts: type({
				id: generated("autoincrement"),
				user_id: type("number.integer").configure({ references: "users.id", onDelete: "cascade" }),
				title: "string",
				published_at: "Date", // native Date support
				tags: "string[]", // JSON columns just work
				metadata: type({ source: "string", "priority?": "number" }), // validated on write by default
				status: type.enumerated("draft", "published").default("draft"),
			}),
		},
		indexes: {
			posts: [{ columns: ["user_id", "status"] }, { columns: ["title"], unique: true }],
		},
	},
	pragmas: {
		journal_mode: "wal",
		synchronous: "normal",
	},
});

// Fully typed Kysely client — generated/default fields are optional on insert
await db.kysely.insertInto("users").values({ name: "John", email: "john@example.com" }).execute();

const users = await db.kysely.selectFrom("users").selectAll().execute();
// users[0].active     → true
// users[0].created_at → Date
```

Booleans, dates, objects, arrays — everything round-trips as the type you declared. The schema is the source of truth for table creation, TypeScript types, and runtime coercion.

## API

```typescript
generated("autoincrement"); // auto-incrementing primary key
generated("now"); // defaults to current timestamp, returned as Date
type("string").default("pending"); // SQL DEFAULT
type("string").configure({ unique: true }); // UNIQUE
type("number.integer").configure({ references: "users.id", onDelete: "cascade" }); // FK
```

JSON columns are validated against the schema on write by default. To also validate on read, or to disable write validation:

```typescript
new Database({
	// ...
	validation: { onRead: true }, // default: { onRead: false, onWrite: true }
});
```

## Migrations

Schema changes are applied automatically on startup. Every time `new Database(...)` runs, the library compares your Arktype schema against the actual SQLite database and applies the minimum set of operations to bring them in sync. No migration files, no version tracking — the database itself is the source of truth.

### What's supported

| Change                           | Strategy                 |
| -------------------------------- | ------------------------ |
| New table                        | `CREATE TABLE`           |
| Removed table                    | `DROP TABLE`             |
| New nullable column              | `ALTER TABLE ADD COLUMN` |
| New NOT NULL column with DEFAULT | `ALTER TABLE ADD COLUMN` |
| Removed column                   | Table rebuild            |
| Type change                      | Table rebuild            |
| Nullability change               | Table rebuild            |
| DEFAULT change                   | Table rebuild            |
| UNIQUE added/removed             | Table rebuild            |
| FK added/removed/changed         | Table rebuild            |
| Index added                      | `CREATE INDEX`           |
| Index removed                    | `DROP INDEX`             |

Table rebuilds follow SQLite's [recommended procedure](https://www.sqlite.org/lang_altertable.html#otheralter): create a new table with the target schema, copy data from the old table, drop the old table, rename the new one. Foreign keys are disabled during rebuilds and validated via `PRAGMA foreign_key_check` before committing.

### Safety rules

- Adding a NOT NULL column without DEFAULT to a table **with data** throws an error
- Changing a nullable column to NOT NULL without DEFAULT throws if any row has NULL in that column
- Nullable-to-NOT-NULL with DEFAULT uses `COALESCE` to fill existing NULLs
- Column renames are treated as drop + add (data in the old column is not preserved)

## License

MIT

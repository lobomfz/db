# @lobomfz/db

SQLite database with Arktype schemas and typed Kysely client.

## Install

```bash
bun add @lobomfz/db
```

## Usage

```typescript
import { Database, generated, type } from "@lobomfz/db";

const db = new Database({
	path: "data.db",
	tables: {
		users: type({
			id: generated("autoincrement"),
			name: "string",
			email: type("string").configure({ unique: true }),
			"bio?": "string",
			created_at: generated("now"),
		}),
		posts: type({
			id: generated("autoincrement"),
			user_id: type("number.integer").configure({ references: "users.id", onDelete: "cascade" }),
			title: "string",
			tags: "string[]",
			status: type("string").default("draft"),
		}),
	},
	indexes: {
		posts: [{ columns: ["user_id", "status"] }, { columns: ["title"], unique: true }],
	},
});

// Fully typed Kysely client - fields with defaults are optional on insert
await db.kysely.insertInto("users").values({ name: "John", email: "john@example.com" }).execute();

const users = await db.kysely.selectFrom("users").selectAll().execute();
```

## Features

- Tables auto-created from Arktype schemas
- Full TypeScript inference (insert vs select types)
- JSON columns with validation
- Foreign keys, unique constraints, defaults
- Composite indexes

## Generated Fields

Use `generated()` for SQL-generated values:

```typescript
generated("autoincrement"); // INTEGER PRIMARY KEY AUTOINCREMENT
generated("now"); // DEFAULT (unixepoch()) - Unix timestamp
```

## Default Values

Use Arktype's `.default()` for JS defaults (also creates SQL DEFAULT):

```typescript
type("string").default("pending");
type("number").default(0);
```

## Column Configuration

```typescript
type("string").configure({ unique: true });
type("number.integer").configure({ references: "users.id", onDelete: "cascade" });
```

`onDelete` options: `"cascade"`, `"set null"`, `"restrict"`

## Composite Indexes

```typescript
const db = new Database({
  tables: { ... },
  indexes: {
    posts: [
      { columns: ["user_id", "category_id"], unique: true },
      { columns: ["created_at"] },
    ],
  },
});
```

## Errors

```typescript
import { JsonParseError, JsonValidationError } from "@lobomfz/db";
```

## License

MIT

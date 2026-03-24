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
        "bio?": "string",                    // optional → nullable in SQLite
        active: type("boolean").default(true),
        created_at: generated("now"),          // defaults to current time
      }),
      posts: type({
        id: generated("autoincrement"),
        user_id: type("number.integer").configure({ references: "users.id", onDelete: "cascade" }),
        title: "string",
        published_at: "Date",                   // native Date support
        tags: "string[]",                      // JSON columns just work
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
generated("autoincrement");                                               // auto-incrementing primary key
generated("now");                                                         // defaults to current timestamp, returned as Date
type("string").default("pending");                                        // SQL DEFAULT
type("string").configure({ unique: true });                               // UNIQUE
type("number.integer").configure({ references: "users.id", onDelete: "cascade" }); // FK
```

JSON columns are validated against the schema on write by default. To also validate on read, or to disable write validation:

```typescript
new Database({
  // ...
  validation: { onRead: true },  // default: { onRead: false, onWrite: true }
});
```

> **Note:** Migrations are not supported yet. Tables are created with `CREATE TABLE IF NOT EXISTS`.

## License

MIT

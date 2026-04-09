---
name: lobomfz-db
description: "@lobomfz/db schema and database patterns. Triggers when defining table schemas, creating a Database instance, using generated fields, or working with code that imports from @lobomfz/db."
---

@lobomfz/db is a typed database library. You define table schemas with ArkType, pass them to a Database constructor, and get a fully typed Kysely query client. Tables are created and updated automatically from the schema — there are no migration files.

All field types work natively. Booleans are booleans, Dates are Dates, objects and arrays are JSON. You never convert, serialize, or coerce values yourself. The library handles it.

Imports

Everything that interacts with the library is imported from @lobomfz/db. Never import from arktype, kysely, or kysely/helpers directly when the symbol is available from the lib.

Available exports: Database, generated, type, Type, configure, sql, Selectable, Insertable, Updateable, Kysely, ExpressionBuilder, jsonArrayFrom, jsonObjectFrom, JsonParseError, JsonValidationError.

Field types

"string" — text
"number" — floating point number
"number.integer" — integer
"boolean" — boolean
"Date" — Date
type.instanceOf(Uint8Array) — binary blob
type.enumerated('a', 'b', 'c') — string enum
type({ ... }), "string[]", or any object/array shape — JSON (automatic serialization and deserialization)

Optionality and nullability

A field key ending with ? inside quotes makes the field nullable and optional on insert:

type({
id: generated('autoincrement'),
name: 'string',
// bio is nullable (selects return null when absent) and omissible on insert.
'bio?': 'string',
})

For explicit nullability on a required field, use a union with null:

type({
status: 'string | null',
active: 'boolean | null',
})

Defaults

.default(value) sets a default and makes the field optional on insert. The value can be a string, number, or boolean.

type({
status: type('string').default('pending'),
count: type('number').default(0),
active: type('boolean').default(true),
})

Generated fields

generated('autoincrement') — auto-incrementing integer primary key. Excluded from the insert type entirely.
generated('now') — timestamp set to current time on insert. Returns a Date on select. Excluded from the insert type but can be overridden on update.

type({
id: generated('autoincrement'),
created_at: generated('now'),
})

Constraints via .configure()

type('number.integer').configure({ primaryKey: true })
type('string').configure({ unique: true })
type('number.integer').configure({ references: 'other_table.column' })
type('number.integer').configure({ references: 'other_table.column', onDelete: 'cascade' })

onDelete options: 'cascade', 'set null', 'restrict'.

A nullable FK with set null uses the optional key syntax:

type({
'owner_id?': type('number.integer').configure({
references: 'owners.id',
onDelete: 'set null',
}),
})

Enums

Define an enum once with type.enumerated, reuse across tables, infer the TS type:

const status = type.enumerated('pending', 'active', 'error')
type Status = typeof status.infer

Use it in schemas directly:

type({
id: generated('autoincrement'),
status: status.default('pending'),
})

Indexes

Defined in schema.indexes alongside schema.tables. Support single-column, composite, unique, and non-unique:

indexes: {
users: [{ columns: ['email'], unique: true }],
posts: [
{ columns: ['user_id', 'category_id'], unique: true },
{ columns: ['category_id'] },
],
}

Database constructor

const database = new Database({
path: './data.db',
schema: {
tables: { ... },
indexes: { ... },
},
pragmas: {
journal_mode: 'wal',
synchronous: 'normal',
},
})

Pragmas: journal_mode, synchronous, foreign_keys (defaults to true), busy_timeout_ms. Recommended production pragmas are journal_mode: 'wal' and synchronous: 'normal'.

Tables are created and updated automatically from the schema on construction. No migration files or manual steps.

Queries

database.kysely is the fully typed Kysely client. All query patterns — selects, inserts, updates, deletes, subqueries, transactions, joins — follow the Kysely skill.

jsonArrayFrom, jsonObjectFrom, and sql are re-exported from @lobomfz/db for use in queries.

Derived types

type DB = typeof database.infer

Selectable<DB['table']> — the row type returned by selects
Insertable<DB['table']> — the input type for inserts
Updateable<DB['table']> — the input type for updates

These are re-exported from @lobomfz/db.

JSON validation

Controlled by the validation option in the constructor:

validation: { onWrite: true, onRead: false }

onWrite (default true): validates JSON fields against their ArkType schema before insert/update.
onRead (default false): validates JSON fields after reading from the database.

Testing

database.reset() clears all tables. database.reset('table_name') clears a single table. Use in beforeEach for test isolation.

Anti-patterns

Never manually serialize or deserialize JSON fields. The library handles it.
Never convert booleans to integers or integers to booleans. The library handles it.
Never convert Dates to timestamps or timestamps to Dates. The library handles it.
Never import type, sql, jsonArrayFrom, or jsonObjectFrom from arktype or kysely directly. Import from @lobomfz/db.
Never create migration files. Schema changes are applied automatically.

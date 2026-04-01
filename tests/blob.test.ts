import { describe, test, expect } from 'bun:test'

import { type } from 'arktype'

import { sql, Database, generated } from '../src/index.ts'

describe('blob', () => {
  test('creates column with BLOB type', async () => {
    const db = new Database({
      path: ':memory:',
      schema: {
        tables: {
          files: type({
            id: generated('autoincrement'),
            data: type.instanceOf(Uint8Array),
          }),
        },
      },
    })

    const info = await sql<{
      name: string
      type: string
    }>`PRAGMA table_info(files)`.execute(db.kysely)

    const dataCol = info.rows.find((r) => r.name === 'data')

    expect(dataCol?.type).toBe('BLOB')
  })

  test('round-trips Uint8Array through insert and select', async () => {
    const db = new Database({
      path: ':memory:',
      schema: {
        tables: {
          files: type({
            id: generated('autoincrement'),
            data: type.instanceOf(Uint8Array),
          }),
        },
      },
    })

    const original = new Uint8Array([1, 2, 3, 127, 255])

    await db.kysely.insertInto('files').values({ data: original }).execute()

    const row = await db.kysely
      .selectFrom('files')
      .selectAll()
      .executeTakeFirstOrThrow()

    expect(row.data).toBeInstanceOf(Uint8Array)
    expect(new Uint8Array(row.data)).toEqual(original)
  })

  test('round-trips Int8Array stored as Uint8Array view', async () => {
    const db = new Database({
      path: ':memory:',
      schema: {
        tables: {
          envelopes: type({
            id: generated('autoincrement'),
            data: type.instanceOf(Uint8Array),
          }),
        },
      },
    })

    const signed = new Int8Array([-128, -1, 0, 1, 127])
    const asUint8 = new Uint8Array(signed.buffer)

    await db.kysely.insertInto('envelopes').values({ data: asUint8 }).execute()

    const row = await db.kysely
      .selectFrom('envelopes')
      .selectAll()
      .executeTakeFirstOrThrow()

    const recovered = new Int8Array(
      row.data.buffer,
      row.data.byteOffset,
      row.data.byteLength
    )

    expect(recovered).toEqual(signed)
  })

  test('nullable blob column', async () => {
    const db = new Database({
      path: ':memory:',
      schema: {
        tables: {
          files: type({
            id: generated('autoincrement'),
            name: 'string',
            'data?': type.instanceOf(Uint8Array),
          }),
        },
      },
    })

    await db.kysely.insertInto('files').values({ name: 'test' }).execute()

    const row = await db.kysely
      .selectFrom('files')
      .selectAll()
      .executeTakeFirstOrThrow()

    expect(row.data).toBeNull()
  })
})

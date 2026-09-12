import { describe, expect, test } from "bun:test"
import { Database as BunDatabase } from "bun:sqlite"
import path from "path"
import { sql } from "drizzle-orm"
import { Context, Effect, Layer, Scope } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { tmpdir } from "./fixture/tmpdir"

// Several opencode processes share one database file, so a write can find the
// lock held by another one. Take the busy timeout down to zero here to get
// that failure immediately instead of waiting the real timeout out.
const probe = <A, E>(
  filename: string,
  body: (db: Database.Interface["db"]) => Effect.Effect<A, E, Scope.Scope>,
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(Database.layerFromPath(filename))
        const { db } = Context.get(context, Database.Service)
        yield* db.run(sql`CREATE TABLE lock_probe (id text PRIMARY KEY)`)
        yield* db.run(sql`PRAGMA busy_timeout = 0`)
        return yield* body(db)
      }),
    ),
  )

describe("Database lock", () => {
  test("recognizes a write that lost the lock", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "locked.sqlite")

    const failure = await probe(filename, (db) =>
      Effect.gen(function* () {
        const holder = new BunDatabase(filename)
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            holder.run("ROLLBACK")
            holder.close()
          }),
        )
        holder.run("BEGIN IMMEDIATE")
        return yield* Effect.flip(db.run(sql`INSERT INTO lock_probe (id) VALUES ('first')`))
      }),
    )

    expect(Database.isLocked(failure)).toBe(true)
    expect(Database.isLocked(new Error("syntax error"))).toBe(false)
    expect(Database.isLocked(undefined)).toBe(false)
  })

  test("retries a write until the other connection releases the lock", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "retried.sqlite")

    const rows = await probe(filename, (db) =>
      Effect.gen(function* () {
        const holder = new BunDatabase(filename)
        holder.run("BEGIN IMMEDIATE")
        yield* Effect.forkScoped(
          Effect.gen(function* () {
            yield* Effect.sleep(300)
            holder.run("ROLLBACK")
            holder.close()
          }),
        )

        yield* Database.retryWhenLocked(db.run(sql`INSERT INTO lock_probe (id) VALUES ('second')`))
        return yield* db.get<{ count: number }>(sql`SELECT count(*) AS count FROM lock_probe`)
      }),
    )

    expect(rows?.count).toBe(1)
  })
})

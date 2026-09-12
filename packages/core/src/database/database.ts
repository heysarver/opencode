export * as Database from "./database"

import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { layer as sqliteLayer } from "#sqlite"
import { Cause, Context, Effect, Layer, Schedule } from "effect"
import { isSqlErrorReason } from "effect/unstable/sql/SqlError"
import { Global } from "../global"
import { Flag } from "../flag/flag"
import { isAbsolute, join } from "path"
import { DatabaseMigration } from "./migration"
import { InstallationChannel } from "../installation/version"
import { makeGlobalNode } from "../effect/app-node"

const makeDatabase = EffectDrizzleSqlite.makeWithDefaults()

// Multiple opencode processes routinely share one database file: one server
// per editor thread plus any CLI invocation. Every instance boot upserts the
// project row, and SQLite's busy handler is not fair, so a waiter can starve
// well past a short timeout while another process keeps writing. The boot
// upsert dies on SQLITE_BUSY and takes the whole process down with it, so wait
// generously instead of failing fast.
const BUSY_TIMEOUT = 30_000

// Waiting out the busy timeout is not always enough, because SQLite's busy
// handler is not fair: a write can lose the race again as soon as it gives up
// its place. Writes that would otherwise die get a bounded number of further
// attempts, spread out so competing processes do not wake together.
const LOCK_RETRY_WINDOW = 30_000

const lockRetrySchedule = Schedule.exponential(100, 2).pipe(
  Schedule.either(Schedule.spaced(1_000)),
  Schedule.jittered,
  Schedule.while((meta) => meta.elapsed < LOCK_RETRY_WINDOW),
)

/**
 * Reports whether a failure is SQLite refusing to write because another
 * connection holds the lock. The reason sits behind a drizzle query error, a
 * `Cause`, or a `SqlError`, so walk whatever chain the caller hands over.
 */
export function isLocked(error: unknown): boolean {
  const seen = new Set<object>()
  const pending: unknown[] = [error]
  while (pending.length > 0) {
    const current = pending.pop()
    if (current === null || typeof current !== "object") continue
    if (seen.has(current)) continue
    seen.add(current)
    if (isSqlErrorReason(current) && current._tag === "LockTimeoutError") return true
    const code = (current as { code?: unknown }).code
    if (code === "SQLITE_BUSY" || code === "SQLITE_LOCKED") return true
    if (Cause.isCause(current)) {
      pending.push(Cause.squash(current))
      continue
    }
    const cause = (current as { cause?: unknown }).cause
    if (cause !== undefined) pending.push(cause)
  }
  return false
}

/**
 * Retries a query for as long as it only fails on a held write lock. Wrap a
 * write with this before `Effect.orDie`, or lock contention turns into a
 * defect that takes the whole process down.
 */
export const retryWhenLocked = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  effect.pipe(Effect.retry({ while: isLocked, schedule: lockRetrySchedule }))
type DatabaseShape = Effect.Success<typeof makeDatabase>

export interface Interface {
  db: DatabaseShape
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/storage/Database") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const db = yield* makeDatabase

    yield* db.run(`PRAGMA busy_timeout = ${BUSY_TIMEOUT}`)
    yield* db.run("PRAGMA journal_mode = WAL")
    yield* db.run("PRAGMA synchronous = NORMAL")
    yield* db.run("PRAGMA cache_size = -64000")
    yield* db.run("PRAGMA foreign_keys = ON")
    yield* db.run("PRAGMA wal_checkpoint(PASSIVE)")
    yield* DatabaseMigration.apply(db)

    return { db }
  }).pipe(Effect.orDie),
)

export function layerFromPath(filename: string) {
  return layer.pipe(Layer.provide(sqliteLayer({ filename, timeout: BUSY_TIMEOUT })))
}

export function path() {
  if (Flag.OPENCODE_DB) {
    if (Flag.OPENCODE_DB === ":memory:" || isAbsolute(Flag.OPENCODE_DB)) return Flag.OPENCODE_DB
    return join(Global.Path.data, Flag.OPENCODE_DB)
  }
  if (
    ["latest", "beta", "prod"].includes(InstallationChannel) ||
    process.env.OPENCODE_DISABLE_CHANNEL_DB === "1" ||
    process.env.OPENCODE_DISABLE_CHANNEL_DB === "true"
  )
    return join(Global.Path.data, "opencode.db")
  return join(Global.Path.data, `opencode-${InstallationChannel.replace(/[^a-zA-Z0-9._-]/g, "-")}.db`)
}

export const node = makeGlobalNode({ service: Service, layer: layerFromPath(path()), deps: [] })

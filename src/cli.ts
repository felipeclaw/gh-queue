#!/usr/bin/env node
import Database from "better-sqlite3";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";

const execFileAsync = promisify(execFile);
const DEFAULT_DB = "./gh-queue.db";
const DEFAULT_INTERVAL_MS = 60_000;
const SAFETY_WINDOW_MS = 10 * 60 * 1000;

type Args = { _: string[]; [key: string]: string | boolean | string[] };
type GhNotification = {
  id: string;
  unread?: boolean;
  reason?: string;
  updated_at: string;
  repository?: { full_name?: string };
  subject?: { title?: string; type?: string; url?: string; latest_comment_url?: string };
  url?: string;
};
type QueuedJob = {
  id: number;
  notification_id: string;
  notification_updated_at: string;
  repo: string;
  created_at: string;
  notification_json: string;
};

function parseArgs(argv: string[]): Args {
  const args: Args = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg.startsWith("--")) {
      args._.push(arg);
      continue;
    }
    const eq = arg.indexOf("=");
    const key = arg.slice(2, eq === -1 ? undefined : eq);
    const next = argv[i + 1];
    const value = eq !== -1 ? arg.slice(eq + 1) : next && !next.startsWith("--") ? argv[++i] : true;
    if (args[key] === undefined) args[key] = value;
    else args[key] = Array.isArray(args[key]) ? [...args[key] as string[], String(value)] : [String(args[key]), String(value)];
  }
  return args;
}

function asString(value: unknown, fallback?: string): string | undefined {
  if (typeof value === "string") return value;
  if (typeof value === "boolean" || value == null) return fallback;
  if (Array.isArray(value)) return value[value.length - 1];
  return String(value);
}

function nowIso(): string {
  return new Date().toISOString();
}

function toIso(value: string | Date): string {
  return new Date(value).toISOString();
}

function parseDurationMs(value: string | undefined, fallbackMs: number): number {
  if (!value) return fallbackMs;
  const match = value.trim().match(/^(\d+(?:\.\d+)?)(ms|s|m|h)?$/);
  if (!match) throw new Error(`Invalid duration: ${value}`);
  const amount = Number(match[1]);
  const unit = match[2] ?? "ms";
  if (unit === "ms") return amount;
  if (unit === "s") return amount * 1000;
  if (unit === "m") return amount * 60 * 1000;
  if (unit === "h") return amount * 60 * 60 * 1000;
  return fallbackMs;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function repoAllowlist(args: Args): string[] {
  const raw = args.repo;
  const values = Array.isArray(raw) ? raw : raw ? [String(raw)] : [];
  return values.flatMap((v) => v.split(",").map((s) => s.trim()).filter(Boolean));
}

function openDb(args: Args): Database.Database {
  const db = new Database(resolve(asString(args.db, DEFAULT_DB)!));
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(`
    CREATE TABLE IF NOT EXISTS state (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS notifications (
      notification_id TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      repo TEXT NOT NULL,
      unread INTEGER NOT NULL,
      reason TEXT,
      subject_type TEXT,
      subject_title TEXT,
      raw_json TEXT NOT NULL,
      stored_at TEXT NOT NULL,
      PRIMARY KEY (notification_id, updated_at)
    );
    CREATE TABLE IF NOT EXISTS jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      notification_id TEXT NOT NULL,
      notification_updated_at TEXT NOT NULL,
      repo TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'queued',
      attempts INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      delivered_at TEXT,
      UNIQUE (notification_id, notification_updated_at)
    );
  `);
  ensureColumn(db, "jobs", "attempts", "INTEGER NOT NULL DEFAULT 0");
  ensureColumn(db, "jobs", "updated_at", "TEXT");
  ensureColumn(db, "jobs", "delivered_at", "TEXT");
  db.prepare("UPDATE jobs SET updated_at=created_at WHERE updated_at IS NULL").run();
  return db;
}

function ensureColumn(db: Database.Database, table: string, column: string, definition: string): void {
  const columns = db.prepare(`PRAGMA table_info(${table})`).all() as { name: string }[];
  if (!columns.some((entry) => entry.name === column)) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
  }
}

function getState(db: Database.Database, key: string): string | undefined {
  return (db.prepare("SELECT value FROM state WHERE key = ?").get(key) as { value: string } | undefined)?.value;
}

function setState(db: Database.Database, key: string, value: string): void {
  db.prepare("INSERT INTO state(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at")
    .run(key, value, nowIso());
}

async function ghNotifications(since: string): Promise<GhNotification[]> {
  const ghArgs = ["api", "--method", "GET", "notifications", "-F", "all=true", "-F", "per_page=100", "-F", `since=${since}`, "--paginate", "--jq", ".[]"];
  const { stdout } = await execFileAsync("gh", ghArgs, { maxBuffer: 50 * 1024 * 1024 });
  return stdout
    .split(/\n+/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line) as GhNotification);
}

function insertNotification(db: Database.Database, n: GhNotification): boolean {
  const repo = n.repository?.full_name ?? "";
  const inserted = db.prepare(`INSERT OR IGNORE INTO notifications
    (notification_id, updated_at, repo, unread, reason, subject_type, subject_title, raw_json, stored_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`) 
    .run(n.id, n.updated_at, repo, n.unread ? 1 : 0, n.reason ?? null, n.subject?.type ?? null, n.subject?.title ?? null, JSON.stringify(n), nowIso()).changes > 0;
  if (inserted) {
    db.prepare(`INSERT OR IGNORE INTO jobs(notification_id, notification_updated_at, repo, status, attempts, created_at, updated_at)
      VALUES (?, ?, ?, 'queued', 0, ?, ?)`) 
      .run(n.id, n.updated_at, repo, nowIso(), nowIso());
  }
  return inserted;
}

async function pollOnce(db: Database.Database, repos: string[]): Promise<{ since: string; fetched: number; matched: number; enqueued: number; checkpoint: string }> {
  if (!repos.length) throw new Error("watch requires at least one --repo owner/name allowlist entry");
  const allow = new Set(repos);
  const stateKey = `lastSeenUpdatedAt:${[...allow].sort().join(",")}`;
  const lastSeen = getState(db, stateKey);

  if (!lastSeen) {
    const checkpoint = nowIso();
    setState(db, stateKey, checkpoint);
    return { since: checkpoint, fetched: 0, matched: 0, enqueued: 0, checkpoint };
  }

  const since = toIso(new Date(new Date(lastSeen).getTime() - SAFETY_WINDOW_MS));
  const notifications = await ghNotifications(since);
  const filtered = notifications.filter((n) => n.id && n.updated_at && allow.has(n.repository?.full_name ?? ""));
  let enqueued = 0;
  let checkpoint = lastSeen;

  const tx = db.transaction(() => {
    for (const n of filtered) {
      if (n.updated_at > checkpoint) checkpoint = n.updated_at;
      if (insertNotification(db, n)) enqueued++;
    }
    setState(db, stateKey, checkpoint);
  });
  tx();

  return { since, fetched: notifications.length, matched: filtered.length, enqueued, checkpoint };
}

async function watch(args: Args): Promise<void> {
  const repos = repoAllowlist(args);
  const intervalMs = parseDurationMs(asString(args.interval, "60s"), DEFAULT_INTERVAL_MS);
  const db = openDb(args);
  let stopping = false;
  process.on("SIGINT", () => { stopping = true; });
  process.on("SIGTERM", () => { stopping = true; });

  console.log(JSON.stringify({ watching: true, intervalMs, repos, db: asString(args.db, DEFAULT_DB) }));
  while (!stopping) {
    try {
      console.log(JSON.stringify(await pollOnce(db, repos)));
    } catch (error) {
      console.error(error instanceof Error ? error.message : String(error));
    }
    if (!stopping) await sleep(intervalMs);
  }
  console.log(JSON.stringify({ watching: false }));
}

function next(args: Args): void {
  const db = openDb(args);
  const tx = db.transaction(() => {
    const job = db.prepare(`SELECT j.*, n.raw_json AS notification_json FROM jobs j
      JOIN notifications n ON n.notification_id=j.notification_id AND n.updated_at=j.notification_updated_at
      WHERE j.status='queued'
      ORDER BY j.created_at ASC, j.id ASC
      LIMIT 1`).get() as QueuedJob | undefined;
    if (!job) return undefined;
    db.prepare("UPDATE jobs SET status='delivered', updated_at=?, delivered_at=? WHERE id=? AND status='queued'").run(nowIso(), nowIso(), job.id);
    return {
      job: {
        id: job.id,
        repo: job.repo,
        notificationId: job.notification_id,
        notificationUpdatedAt: job.notification_updated_at,
        createdAt: job.created_at,
      },
      notification: JSON.parse(job.notification_json),
    };
  });
  const payload = tx();
  if (!payload) {
    console.log("No queued jobs.");
    return;
  }
  console.log(JSON.stringify(payload, null, 2));
}

function usage(): void {
  console.log(`Usage: gh-queue <command> [options]\n\nCommands:\n  watch --repo owner/name [--db path] [--interval 60s]\n  next [--db path]`);
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const command = args._[0];
  try {
    if (command === "watch") await watch(args);
    else if (command === "next") next(args);
    else {
      usage();
      process.exitCode = command ? 1 : 0;
    }
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}

await main();

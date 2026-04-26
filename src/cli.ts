#!/usr/bin/env node
import Database from "better-sqlite3";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";

const execFileAsync = promisify(execFile);
const DEFAULT_DB = "./gh-queue.db";
const DEFAULT_INTERVAL_MS = 60_000;
const DEFAULT_LEASE_MS = 90 * 60 * 1000;
const DEFAULT_MAX_ATTEMPTS = 5;
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
type ItemRow = {
  repo: string;
  number: number;
  subject_type: string | null;
  status: string;
  dirty: number;
  attempts: number;
  notification_count: number;
  latest_notification_updated_at: string;
  delivered_at: string | null;
  lease_until: string | null;
  worker_id: string | null;
  last_error: string | null;
  updated_at: string;
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

function hasFlag(args: Args, name: string): boolean {
  return args[name] === true || args[name] === "true";
}

function nowIso(): string {
  return new Date().toISOString();
}

function addMsIso(ms: number): string {
  return new Date(Date.now() + ms).toISOString();
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
      number INTEGER NOT NULL,
      subject_type TEXT,
      reason TEXT,
      subject_title TEXT,
      subject_url TEXT,
      latest_comment_url TEXT,
      unread INTEGER NOT NULL,
      raw_json TEXT NOT NULL,
      stored_at TEXT NOT NULL,
      PRIMARY KEY (notification_id, updated_at)
    );
    CREATE TABLE IF NOT EXISTS items (
      repo TEXT NOT NULL,
      number INTEGER NOT NULL,
      subject_type TEXT,
      status TEXT NOT NULL DEFAULT 'queued',
      dirty INTEGER NOT NULL DEFAULT 0,
      attempts INTEGER NOT NULL DEFAULT 0,
      notification_count INTEGER NOT NULL DEFAULT 0,
      latest_notification_updated_at TEXT NOT NULL,
      delivered_at TEXT,
      lease_until TEXT,
      worker_id TEXT,
      last_error TEXT,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (repo, number)
    );
  `);
  return db;
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

function extractNumberFromUrl(url: string | undefined): number | null {
  if (!url) return null;
  const match = url.match(/\/(?:issues|pulls)\/(\d+)(?:$|[/?#])/);
  return match ? Number(match[1]) : null;
}

function notificationNumber(n: GhNotification): number | null {
  return extractNumberFromUrl(n.subject?.url) ?? extractNumberFromUrl(n.subject?.latest_comment_url);
}

function apiUrlToHtmlUrl(apiUrl: string | undefined, type: string | null | undefined): string | null {
  if (!apiUrl) return null;
  let match = apiUrl.match(/^https:\/\/api\.github\.com\/repos\/([^/]+)\/([^/]+)\/pulls\/(\d+)$/);
  if (match) return `https://github.com/${match[1]}/${match[2]}/pull/${match[3]}`;
  match = apiUrl.match(/^https:\/\/api\.github\.com\/repos\/([^/]+)\/([^/]+)\/issues\/(\d+)$/);
  if (match) return `https://github.com/${match[1]}/${match[2]}/${type === "PullRequest" ? "pull" : "issues"}/${match[3]}`;
  return null;
}

function compactNotification(n: GhNotification): unknown {
  return {
    id: n.id,
    reason: n.reason ?? null,
    updatedAt: n.updated_at,
    unread: Boolean(n.unread),
    subject: {
      type: n.subject?.type ?? null,
      title: n.subject?.title ?? null,
      number: notificationNumber(n),
      apiUrl: n.subject?.url ?? null,
      htmlUrl: apiUrlToHtmlUrl(n.subject?.url, n.subject?.type),
      latestCommentApiUrl: n.subject?.latest_comment_url ?? null,
    },
  };
}

function insertNotification(db: Database.Database, n: GhNotification): boolean {
  const repo = n.repository?.full_name ?? "";
  const number = notificationNumber(n);
  if (!repo || number == null) return false;
  const ts = nowIso();
  const inserted = db.prepare(`INSERT OR IGNORE INTO notifications
    (notification_id, updated_at, repo, number, subject_type, reason, subject_title, subject_url, latest_comment_url, unread, raw_json, stored_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(
      n.id,
      n.updated_at,
      repo,
      number,
      n.subject?.type ?? null,
      n.reason ?? null,
      n.subject?.title ?? null,
      n.subject?.url ?? null,
      n.subject?.latest_comment_url ?? null,
      n.unread ? 1 : 0,
      JSON.stringify(n),
      ts,
    ).changes > 0;
  if (!inserted) return false;

  const existing = db.prepare("SELECT status FROM items WHERE repo=? AND number=?").get(repo, number) as { status: string } | undefined;
  if (!existing) {
    db.prepare(`INSERT INTO items(repo, number, subject_type, status, dirty, attempts, notification_count, latest_notification_updated_at, updated_at)
      VALUES (?, ?, ?, 'queued', 0, 0, 1, ?, ?)`)
      .run(repo, number, n.subject?.type ?? null, n.updated_at, ts);
  } else {
    const status = existing.status;
    const nextStatus = status === "delivered" ? "delivered" : "queued";
    const dirty = status === "delivered" ? 1 : 0;
    db.prepare(`UPDATE items SET
      subject_type=COALESCE(subject_type, ?),
      status=?,
      dirty=CASE WHEN ? = 1 THEN 1 ELSE dirty END,
      notification_count=notification_count + 1,
      latest_notification_updated_at=MAX(latest_notification_updated_at, ?),
      updated_at=?
      WHERE repo=? AND number=?`)
      .run(n.subject?.type ?? null, nextStatus, dirty, n.updated_at, ts, repo, number);
  }
  return true;
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

function itemNotifications(db: Database.Database, item: Pick<ItemRow, "repo" | "number">, raw: boolean): unknown[] {
  const rows = db.prepare(`SELECT raw_json FROM notifications WHERE repo=? AND number=? ORDER BY updated_at ASC, stored_at ASC`).all(item.repo, item.number) as { raw_json: string }[];
  return rows.map((row) => {
    const parsed = JSON.parse(row.raw_json) as GhNotification;
    return raw ? parsed : compactNotification(parsed);
  });
}

function next(args: Args): void {
  const db = openDb(args);
  const worker = asString(args.worker, `worker-${process.pid}`)!;
  const leaseMs = parseDurationMs(asString(args.lease, "90m"), DEFAULT_LEASE_MS);
  const raw = hasFlag(args, "raw");
  const now = nowIso();
  const leaseUntil = addMsIso(leaseMs);
  const tx = db.transaction(() => {
    const item = db.prepare(`SELECT * FROM items
      WHERE status='queued' OR (status='delivered' AND lease_until IS NOT NULL AND lease_until < ?)
      ORDER BY latest_notification_updated_at ASC, updated_at ASC
      LIMIT 1`).get(now) as ItemRow | undefined;
    if (!item) return undefined;
    const changed = db.prepare(`UPDATE items SET
      status='delivered',
      dirty=0,
      attempts=attempts + 1,
      delivered_at=?,
      lease_until=?,
      worker_id=?,
      updated_at=?
      WHERE repo=? AND number=? AND (status='queued' OR (status='delivered' AND lease_until IS NOT NULL AND lease_until < ?))`)
      .run(now, leaseUntil, worker, now, item.repo, item.number, now).changes;
    if (!changed) return undefined;
    const updated = db.prepare("SELECT * FROM items WHERE repo=? AND number=?").get(item.repo, item.number) as ItemRow;
    return {
      item: {
        repo: updated.repo,
        number: updated.number,
        type: updated.subject_type,
        status: updated.status,
        attempts: updated.attempts,
        notificationCount: updated.notification_count,
        latestNotificationUpdatedAt: updated.latest_notification_updated_at,
        leaseUntil: updated.lease_until,
        workerId: updated.worker_id,
      },
      notifications: itemNotifications(db, updated, raw),
    };
  });
  const payload = tx();
  if (!payload) {
    console.log("No queued items.");
    return;
  }
  console.log(JSON.stringify(payload, null, 2));
}

function ack(args: Args): void {
  const repo = asString(args.repo);
  const number = Number(asString(args.number));
  if (!repo || !Number.isFinite(number)) throw new Error("ack requires --repo owner/name --number n");
  const db = openDb(args);
  const ts = nowIso();
  const item = db.prepare("SELECT dirty FROM items WHERE repo=? AND number=? AND status='delivered'").get(repo, number) as { dirty: number } | undefined;
  if (!item) throw new Error("No delivered item found for ack");
  if (item.dirty) {
    db.prepare(`UPDATE items SET status='queued', dirty=0, delivered_at=NULL, lease_until=NULL, worker_id=NULL, updated_at=? WHERE repo=? AND number=?`).run(ts, repo, number);
    console.log(JSON.stringify({ repo, number, status: "queued", dirtyWasSet: true }));
  } else {
    db.prepare(`UPDATE items SET status='done', delivered_at=NULL, lease_until=NULL, worker_id=NULL, updated_at=? WHERE repo=? AND number=?`).run(ts, repo, number);
    console.log(JSON.stringify({ repo, number, status: "done" }));
  }
}

function fail(args: Args): void {
  const repo = asString(args.repo);
  const number = Number(asString(args.number));
  if (!repo || !Number.isFinite(number)) throw new Error("fail requires --repo owner/name --number n");
  const maxAttempts = Number(asString(args["max-attempts"], String(DEFAULT_MAX_ATTEMPTS)));
  const reason = asString(args.reason, "")!;
  const db = openDb(args);
  const ts = nowIso();
  const item = db.prepare("SELECT attempts, dirty FROM items WHERE repo=? AND number=? AND status='delivered'").get(repo, number) as { attempts: number; dirty: number } | undefined;
  if (!item) throw new Error("No delivered item found for fail");
  const dirtyWasSet = item.dirty === 1;
  const status = dirtyWasSet || item.attempts < maxAttempts ? "queued" : "failed";
  db.prepare(`UPDATE items SET status=?, dirty=0, delivered_at=NULL, lease_until=NULL, worker_id=NULL, last_error=?, updated_at=? WHERE repo=? AND number=?`).run(status, reason, ts, repo, number);
  console.log(JSON.stringify({ repo, number, status, attempts: item.attempts, dirtyWasSet }));
}

function stats(args: Args): void {
  const db = openDb(args);
  const rows = db.prepare("SELECT status, COUNT(*) AS count FROM items GROUP BY status ORDER BY status").all();
  console.log(JSON.stringify(rows, null, 2));
}

function usage(): void {
  console.log(`Usage: gh-queue <command> [options]\n\nCommands:\n  watch --repo owner/name [--db path] [--interval 60s]\n  next [--db path] [--worker id] [--lease 90m] [--raw]\n  ack --repo owner/name --number n [--db path]\n  fail --repo owner/name --number n [--db path] [--reason text] [--max-attempts 5]\n  stats [--db path]`);
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const command = args._[0];
  try {
    if (command === "watch") await watch(args);
    else if (command === "next") next(args);
    else if (command === "ack") ack(args);
    else if (command === "fail") fail(args);
    else if (command === "stats") stats(args);
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

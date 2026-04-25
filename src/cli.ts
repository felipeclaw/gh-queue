#!/usr/bin/env node
import Database from "better-sqlite3";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";

const execFileAsync = promisify(execFile);
const DEFAULT_DB = "./gh-queue.db";
const SAFETY_WINDOW_MS = 10 * 60 * 1000;

type Args = { _: string[]; [key: string]: string | boolean | string[] };
type GhNotification = {
  id: string;
  unread?: boolean;
  reason?: string;
  updated_at: string;
  last_read_at?: string | null;
  repository?: { full_name?: string };
  subject?: { title?: string; type?: string; url?: string; latest_comment_url?: string };
  url?: string;
};
type Job = {
  id: number;
  notification_id: string;
  notification_updated_at: string;
  repo: string;
  status: string;
  attempts: number;
  created_at: string;
  updated_at: string;
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

function hasFlag(args: Args, name: string): boolean {
  return args[name] === true || args[name] === "true";
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
  const dbPath = resolve(asString(args.db, DEFAULT_DB)!);
  const db = new Database(dbPath);
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
      claimed_at TEXT,
      finished_at TEXT,
      last_error TEXT,
      UNIQUE (notification_id, notification_updated_at)
    );
    CREATE TABLE IF NOT EXISTS runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id INTEGER NOT NULL,
      status TEXT NOT NULL,
      command TEXT,
      payload TEXT,
      stdout TEXT,
      stderr TEXT,
      exit_code INTEGER,
      started_at TEXT NOT NULL,
      finished_at TEXT NOT NULL,
      FOREIGN KEY (job_id) REFERENCES jobs(id)
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

async function ghNotifications(since: string | undefined, limit: number | undefined): Promise<GhNotification[]> {
  const ghArgs = ["api", "--method", "GET", "notifications", "-F", "all=true", "-F", "per_page=100", "--paginate", "--jq", ".[]"];
  if (since) ghArgs.push("-F", `since=${since}`);
  const { stdout } = await execFileAsync("gh", ghArgs, { maxBuffer: 50 * 1024 * 1024 });
  const notifications = stdout
    .split(/\n+/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line) as GhNotification);
  return typeof limit === "number" ? notifications.slice(0, limit) : notifications;
}

function insertNotificationAndMaybeJob(db: Database.Database, n: GhNotification, dryRun: boolean): boolean {
  const repo = n.repository?.full_name ?? "";
  const inserted = db.prepare(`INSERT OR IGNORE INTO notifications
    (notification_id, updated_at, repo, unread, reason, subject_type, subject_title, raw_json, stored_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(n.id, n.updated_at, repo, n.unread ? 1 : 0, n.reason ?? null, n.subject?.type ?? null, n.subject?.title ?? null, JSON.stringify(n), nowIso()).changes > 0;
  if (inserted && !dryRun) {
    db.prepare(`INSERT OR IGNORE INTO jobs(notification_id, notification_updated_at, repo, status, created_at, updated_at)
      VALUES (?, ?, ?, 'queued', ?, ?)`)
      .run(n.id, n.updated_at, repo, nowIso(), nowIso());
  }
  return inserted;
}

async function poll(args: Args): Promise<void> {
  const db = openDb(args);
  const dryRun = hasFlag(args, "dry-run");
  const repos = repoAllowlist(args);
  if (!repos.length) throw new Error("poll requires at least one --repo owner/name allowlist entry");
  const allow = new Set(repos);
  const sinceArg = asString(args.since);
  const limit = args.limit ? Number(asString(args.limit)) : undefined;
  const backfill = hasFlag(args, "backfill");
  const stateKey = `lastSeenUpdatedAt:${[...allow].sort().join(",")}`;
  const lastSeen = getState(db, stateKey);

  if (!lastSeen && !backfill && !sinceArg) {
    const checkpoint = nowIso();
    if (!dryRun) setState(db, stateKey, checkpoint);
    console.log(`Initialized checkpoint ${stateKey}=${checkpoint}; no notifications enqueued. Use --backfill or --since to import older notifications.`);
    return;
  }

  const since = sinceArg ?? (backfill ? undefined : toIso(new Date(new Date(lastSeen!).getTime() - SAFETY_WINDOW_MS)));
  const notifications = await ghNotifications(since, limit);
  const filtered = notifications.filter((n) => n.id && n.updated_at && allow.has(n.repository?.full_name ?? ""));
  let newJobs = 0;
  let maxUpdatedAt = lastSeen ?? sinceArg ?? nowIso();

  const tx = db.transaction(() => {
    for (const n of filtered) {
      if (n.updated_at > maxUpdatedAt) maxUpdatedAt = n.updated_at;
      if (dryRun) {
        const exists = db.prepare("SELECT 1 FROM notifications WHERE notification_id=? AND updated_at=?").get(n.id, n.updated_at);
        if (!exists) newJobs++;
      } else if (insertNotificationAndMaybeJob(db, n, false)) newJobs++;
    }
    if (!dryRun) setState(db, stateKey, maxUpdatedAt);
  });
  tx();
  console.log(JSON.stringify({ dryRun, since: since ?? null, repos: [...allow], fetched: notifications.length, matched: filtered.length, enqueued: newJobs, checkpoint: maxUpdatedAt }, null, 2));
}


async function watch(args: Args): Promise<void> {
  if (hasFlag(args, "dry-run")) throw new Error("watch does not support --dry-run; use poll --dry-run instead");
  const intervalMs = parseDurationMs(asString(args.interval, "60s"), 60_000);
  let stopping = false;
  process.on("SIGINT", () => { stopping = true; });
  process.on("SIGTERM", () => { stopping = true; });
  console.log(JSON.stringify({ watching: true, intervalMs, repos: repoAllowlist(args), db: asString(args.db, DEFAULT_DB) }));
  while (!stopping) {
    try {
      await poll(args);
    } catch (error) {
      console.error(error instanceof Error ? error.message : String(error));
    }
    if (!stopping) await sleep(intervalMs);
  }
  console.log(JSON.stringify({ watching: false }));
}

function jobPayload(job: Job): unknown {
  return {
    job: {
      id: job.id,
      repo: job.repo,
      notificationId: job.notification_id,
      notificationUpdatedAt: job.notification_updated_at,
      attempts: job.attempts,
      status: job.status,
      createdAt: job.created_at,
      updatedAt: job.updated_at,
    },
    notification: JSON.parse(job.notification_json),
  };
}

function getJobWithNotification(db: Database.Database, id?: number): Job | undefined {
  const where = id ? "j.id = ?" : "j.status = 'queued'";
  const order = id ? "" : "ORDER BY j.created_at ASC, j.id ASC LIMIT 1";
  return db.prepare(`SELECT j.*, n.raw_json AS notification_json FROM jobs j
    JOIN notifications n ON n.notification_id=j.notification_id AND n.updated_at=j.notification_updated_at
    WHERE ${where} ${order}`).get(...(id ? [id] : [])) as Job | undefined;
}

async function runNext(args: Args): Promise<void> {
  const db = openDb(args);
  const dryRun = hasFlag(args, "dry-run");
  const recordDryRun = hasFlag(args, "record-dry-run");
  const job = getJobWithNotification(db);
  if (!job) {
    console.log("No queued jobs.");
    return;
  }

  const payload = jobPayload(job);

  if (dryRun) {
    console.log(JSON.stringify({ dryRun: true, action: recordDryRun ? "record dry_run_complete" : "leave queue unchanged", ...payload as object }, null, 2));
    if (recordDryRun) {
      const ts = nowIso();
      db.prepare("UPDATE jobs SET status='dry_run_complete', updated_at=?, finished_at=? WHERE id=? AND status='queued'").run(ts, ts, job.id);
      db.prepare("INSERT INTO runs(job_id,status,command,payload,stdout,stderr,exit_code,started_at,finished_at) VALUES(?,?,?,?,?,?,?,?,?)")
        .run(job.id, "dry_run_complete", "run-next --dry-run --record-dry-run", JSON.stringify(payload), "", "", 0, ts, ts);
    }
    return;
  }

  const ts = nowIso();
  const claimed = db.prepare("UPDATE jobs SET status='running', attempts=attempts+1, claimed_at=?, updated_at=? WHERE id=? AND status='queued'")
    .run(ts, ts, job.id).changes;
  if (!claimed) {
    console.error(`Job ${job.id} was already claimed.`);
    process.exitCode = 1;
    return;
  }

  db.prepare("INSERT INTO runs(job_id,status,command,payload,stdout,stderr,exit_code,started_at,finished_at) VALUES(?,?,?,?,?,?,?,?,?)")
    .run(job.id, "claimed", "run-next", JSON.stringify(payload), "", "", 0, ts, ts);
  console.log(JSON.stringify({ claimed: true, ...payload as object }, null, 2));
}

function complete(args: Args, jobId: number): void {
  const db = openDb(args);
  const ts = nowIso();
  const changes = db.prepare("UPDATE jobs SET status='complete', updated_at=?, finished_at=?, last_error=NULL WHERE id=? AND status IN ('queued','running','failed')").run(ts, ts, jobId).changes;
  if (!changes) throw new Error(`Job ${jobId} not found or not completable`);
  console.log(`Completed job ${jobId}.`);
}

function status(args: Args): void {
  const db = openDb(args);
  const rows = db.prepare("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status ORDER BY status").all();
  const state = db.prepare("SELECT key, value FROM state ORDER BY key").all();
  console.log(JSON.stringify({ jobs: rows, state }, null, 2));
}

function jobs(args: Args): void {
  const db = openDb(args);
  const statusFilter = asString(args.status);
  const rows = statusFilter
    ? db.prepare("SELECT id,status,repo,notification_id,notification_updated_at,attempts,created_at,updated_at,last_error FROM jobs WHERE status=? ORDER BY id DESC LIMIT ?").all(statusFilter, Number(asString(args.limit, "50")))
    : db.prepare("SELECT id,status,repo,notification_id,notification_updated_at,attempts,created_at,updated_at,last_error FROM jobs ORDER BY id DESC LIMIT ?").all(Number(asString(args.limit, "50")));
  console.log(JSON.stringify(rows, null, 2));
}

function show(args: Args, jobId: number): void {
  const db = openDb(args);
  const job = getJobWithNotification(db, jobId);
  if (!job) throw new Error(`Job ${jobId} not found`);
  const runs = db.prepare("SELECT id,status,command,stdout,stderr,exit_code,started_at,finished_at FROM runs WHERE job_id=? ORDER BY id DESC").all(jobId);
  console.log(JSON.stringify({ ...job, notification_json: JSON.parse(job.notification_json), runs }, null, 2));
}

function skip(args: Args, jobId: number): void {
  const db = openDb(args);
  const ts = nowIso();
  const changes = db.prepare("UPDATE jobs SET status='skipped', updated_at=?, finished_at=?, last_error=NULL WHERE id=? AND status IN ('queued','failed','running')").run(ts, ts, jobId).changes;
  if (!changes) throw new Error(`Job ${jobId} not found or not skippable`);
  console.log(`Skipped job ${jobId}.`);
}

function retry(args: Args, jobId: number): void {
  const db = openDb(args);
  const changes = db.prepare("UPDATE jobs SET status='queued', updated_at=?, claimed_at=NULL, finished_at=NULL, last_error=NULL WHERE id=? AND status IN ('failed','skipped','dry_run_complete','running','complete')").run(nowIso(), jobId).changes;
  if (!changes) throw new Error(`Job ${jobId} not found or not retryable`);
  console.log(`Queued job ${jobId} for retry.`);
}

function usage(): void {
  console.log(`Usage: gh-queue <command> [options]\n\nCommands:\n  poll [--repo owner/name] [--db path] [--dry-run] [--backfill|--since ISO] [--limit n]\n  watch [--repo owner/name] [--db path] [--interval 60s] [--limit n]\n  run-next [--db path] [--dry-run] [--record-dry-run]\n  status [--db path]\n  jobs [--db path] [--status queued] [--limit n]\n  show <jobId> [--db path]\n  skip <jobId> [--db path]\n  complete <jobId> [--db path]\n  retry <jobId> [--db path]`);
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const command = args._[0];
  try {
    if (command === "poll") await poll(args);
    else if (command === "watch") await watch(args);
    else if (command === "run-next") await runNext(args);
    else if (command === "status") status(args);
    else if (command === "jobs") jobs(args);
    else if (command === "show") show(args, Number(args._[1]));
    else if (command === "skip") skip(args, Number(args._[1]));
    else if (command === "complete") complete(args, Number(args._[1]));
    else if (command === "retry") retry(args, Number(args._[1]));
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

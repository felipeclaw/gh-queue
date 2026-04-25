#!/usr/bin/env node
import { createClient } from "redis";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const DEFAULT_INTERVAL_MS = 60_000;
const SAFETY_WINDOW_MS = 10 * 60 * 1000;
const DEFAULT_REDIS_URL = "redis://127.0.0.1:6379";
const DEFAULT_STREAM = "ghq:notifications";
const DEFAULT_GROUP = "ghq";
const DEFAULT_CONSUMER = `consumer-${process.pid}`;

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
type RedisClient = ReturnType<typeof createClient>;

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

function redisUrl(args: Args): string {
  return asString(args.redis, process.env.REDIS_URL || DEFAULT_REDIS_URL)!;
}

function streamName(args: Args): string {
  return asString(args.stream, DEFAULT_STREAM)!;
}

function groupName(args: Args): string {
  return asString(args.group, DEFAULT_GROUP)!;
}

function consumerName(args: Args): string {
  return asString(args.consumer, DEFAULT_CONSUMER)!;
}

async function redis(args: Args): Promise<RedisClient> {
  const client = createClient({ url: redisUrl(args) });
  client.on("error", (error) => console.error(error instanceof Error ? error.message : String(error)));
  await client.connect();
  return client;
}

async function ensureGroup(client: RedisClient, stream: string, group: string): Promise<void> {
  try {
    await client.xGroupCreate(stream, group, "0", { MKSTREAM: true });
  } catch (error) {
    if (!(error instanceof Error) || !error.message.includes("BUSYGROUP")) throw error;
  }
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

function notificationPayload(n: GhNotification): Record<string, string> {
  return {
    notificationId: n.id,
    updatedAt: n.updated_at,
    repo: n.repository?.full_name ?? "",
    reason: n.reason ?? "",
    subjectType: n.subject?.type ?? "",
    subjectTitle: n.subject?.title ?? "",
    subjectUrl: n.subject?.url ?? "",
    latestCommentUrl: n.subject?.latest_comment_url ?? "",
    unread: n.unread ? "true" : "false",
    raw: JSON.stringify(n),
  };
}

async function pollOnce(client: RedisClient, args: Args, repos: string[]): Promise<{ since: string; fetched: number; matched: number; enqueued: number; checkpoint: string }> {
  if (!repos.length) throw new Error("watch requires at least one --repo owner/name allowlist entry");
  const stream = streamName(args);
  const allow = new Set(repos);
  const stateKey = `ghq:lastSeen:${[...allow].sort().join(",")}`;
  const lastSeen = await client.get(stateKey);

  if (!lastSeen) {
    const checkpoint = nowIso();
    await client.set(stateKey, checkpoint);
    return { since: checkpoint, fetched: 0, matched: 0, enqueued: 0, checkpoint };
  }

  const since = toIso(new Date(new Date(lastSeen).getTime() - SAFETY_WINDOW_MS));
  const notifications = await ghNotifications(since);
  const filtered = notifications.filter((n) => n.id && n.updated_at && allow.has(n.repository?.full_name ?? ""));
  let checkpoint = lastSeen;
  let enqueued = 0;

  for (const n of filtered) {
    if (n.updated_at > checkpoint) checkpoint = n.updated_at;
    const seenKey = `ghq:seen:${n.id}:${n.updated_at}`;
    const inserted = await client.set(seenKey, "1", { NX: true });
    if (inserted) {
      await client.xAdd(stream, "*", notificationPayload(n));
      enqueued++;
    }
  }
  await client.set(stateKey, checkpoint);
  return { since, fetched: notifications.length, matched: filtered.length, enqueued, checkpoint };
}

async function watch(args: Args): Promise<void> {
  const repos = repoAllowlist(args);
  const intervalMs = parseDurationMs(asString(args.interval, "60s"), DEFAULT_INTERVAL_MS);
  const client = await redis(args);
  let stopping = false;
  process.on("SIGINT", () => { stopping = true; });
  process.on("SIGTERM", () => { stopping = true; });

  console.log(JSON.stringify({ watching: true, intervalMs, repos, redis: redisUrl(args), stream: streamName(args) }));
  while (!stopping) {
    try {
      console.log(JSON.stringify(await pollOnce(client, args, repos)));
    } catch (error) {
      console.error(error instanceof Error ? error.message : String(error));
    }
    if (!stopping) await sleep(intervalMs);
  }
  await client.quit();
  console.log(JSON.stringify({ watching: false }));
}

async function next(args: Args): Promise<void> {
  const client = await redis(args);
  const stream = streamName(args);
  const group = groupName(args);
  const consumer = consumerName(args);
  await ensureGroup(client, stream, group);
  const response = await client.xReadGroup(group, consumer, [{ key: stream, id: ">" }], { COUNT: 1, BLOCK: 1 });
  if (!response?.length || !response[0]?.messages.length) {
    console.log("No queued jobs.");
    await client.quit();
    return;
  }
  const message = response[0].messages[0];
  const payload = {
    stream,
    group,
    consumer,
    id: message.id,
    notification: {
      notificationId: message.message.notificationId,
      updatedAt: message.message.updatedAt,
      repo: message.message.repo,
      reason: message.message.reason,
      subjectType: message.message.subjectType,
      subjectTitle: message.message.subjectTitle,
      subjectUrl: message.message.subjectUrl,
      latestCommentUrl: message.message.latestCommentUrl,
      unread: message.message.unread === "true",
      raw: JSON.parse(message.message.raw),
    },
  };
  if (hasFlag(args, "ack")) await client.xAck(stream, group, message.id);
  console.log(JSON.stringify(payload, null, 2));
  await client.quit();
}

async function ack(args: Args): Promise<void> {
  const id = args._[1];
  if (!id) throw new Error("ack requires a stream message id");
  const client = await redis(args);
  const count = await client.xAck(streamName(args), groupName(args), id);
  console.log(JSON.stringify({ acked: count, id }));
  await client.quit();
}

async function pending(args: Args): Promise<void> {
  const client = await redis(args);
  await ensureGroup(client, streamName(args), groupName(args));
  const result = await client.xPending(streamName(args), groupName(args));
  console.log(JSON.stringify(result, null, 2));
  await client.quit();
}

function usage(): void {
  console.log(`Usage: gh-queue <command> [options]\n\nCommands:\n  watch --repo owner/name [--redis url] [--stream name] [--interval 60s]\n  next [--redis url] [--stream name] [--group name] [--consumer name] [--ack]\n  ack <streamId> [--redis url] [--stream name] [--group name]\n  pending [--redis url] [--stream name] [--group name]`);
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const command = args._[0];
  try {
    if (command === "watch") await watch(args);
    else if (command === "next") await next(args);
    else if (command === "ack") await ack(args);
    else if (command === "pending") await pending(args);
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

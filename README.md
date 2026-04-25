# gh-queue

`gh-queue` is a tiny CLI that watches GitHub notifications for selected repositories and publishes them to a Redis Stream.

It does only a few queue-oriented things:

1. `watch` — long-running poller that fills a Redis Stream.
2. `next` — consumes one queued notification from a Redis consumer group and prints a compact JSON payload.
3. `ack` — acknowledges a consumed stream message after successful processing.
4. `pending` — inspects pending messages for the consumer group.

It does **not** decide whether a notification is actionable or mark GitHub notifications read.

## Install

```bash
npm install
npm run build
npm link   # optional, exposes gh-queue locally
```

Requirements:

- Node 20+
- Redis 6+
- `gh` CLI authenticated for notifications

## Usage

Run the watcher:

```bash
gh-queue watch --repo owner/repo --redis redis://127.0.0.1:6379 --interval 60s
```

Pull the next queued notification:

```bash
gh-queue next --group workers --consumer worker-1
```

Acknowledge after successful processing:

```bash
gh-queue ack 1714080000000-0 --group workers
```

If printing or forwarding the payload is the complete processing step, consume and acknowledge in one command:

```bash
gh-queue next --group workers --consumer worker-1 --ack
```

If the queue is empty, `next` prints:

```text
No queued jobs.
```

## Behavior

- The first `watch` run initializes a Redis checkpoint to the current time and does not backfill old notifications.
- Later watch loops query GitHub notifications since the last checkpoint minus a 10-minute safety window.
- `read`/`unread` is stored as metadata only and is never used as an actionability filter.
- Repositories must be explicitly allowlisted with `--repo`.
- Duplicate `(notificationId, updatedAt)` pairs are ignored via Redis dedupe keys.
- Redis consumer groups provide delivery tracking: a message remains pending until `ack` is called.
- `next` returns a compact payload by default; use `--raw` to include the original GitHub notification payload.

## Commands

```text
gh-queue watch --repo owner/name [--redis url] [--stream name] [--interval 60s]
gh-queue next [--redis url] [--stream name] [--group name] [--consumer name] [--ack] [--raw]
gh-queue ack <streamId> [--redis url] [--stream name] [--group name]
gh-queue pending [--redis url] [--stream name] [--group name]
```

Defaults:

- Redis URL: `redis://127.0.0.1:6379`
- Stream: `ghq:notifications`
- Group: `ghq`

`--repo` can be repeated or comma-separated.

## Development

```bash
npm install
npm run check
npm run build
```

The executable is `dist/cli.js` and the package bin is named `gh-queue`.

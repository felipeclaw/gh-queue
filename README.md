# gh-queue

`gh-queue` is a tiny CLI that watches GitHub notifications for selected repositories and stores them in a local SQLite queue.

It does only two things:

1. `watch` — long-running poller that fills the queue.
2. `next` — pops one queued notification and prints it as JSON.

It does **not** decide whether a notification is actionable or mark GitHub notifications read.

## Install

```bash
npm install
npm run build
npm link   # optional, exposes gh-queue locally
```

Requirements:

- Node 20+
- `gh` CLI authenticated for notifications

## Usage

Run the watcher:

```bash
gh-queue watch --repo owner/repo --db ~/.local/state/gh-queue.db --interval 60s
```

Pull the next queued notification:

```bash
gh-queue next --db ~/.local/state/gh-queue.db
```

If the queue is empty, `next` prints:

```text
No queued jobs.
```

## Behavior

- The first `watch` run initializes a checkpoint to the current time and does not backfill old notifications.
- Later watch loops query GitHub notifications since the last checkpoint minus a 10-minute safety window.
- `read`/`unread` is stored as metadata only and is never used as an actionability filter.
- Repositories must be explicitly allowlisted with `--repo`.
- Duplicate `(notificationId, updatedAt)` pairs are ignored.
- `next` atomically marks one queued item as delivered before printing it.

## Commands

```text
gh-queue watch --repo owner/name [--db path] [--interval 60s]
gh-queue next [--db path]
```

`--repo` can be repeated or comma-separated.

## Development

```bash
npm install
npm run check
npm run build
```

The executable is `dist/cli.js` and the package bin is named `gh-queue`.

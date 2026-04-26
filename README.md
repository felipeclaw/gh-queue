# gh-queue

`gh-queue` is a tiny CLI that watches GitHub notifications for selected repositories and stores a local SQLite queue keyed by GitHub item number.

GitHub issues and pull requests share a number namespace within a repository, so multiple notifications for the same issue/PR coalesce into one queue item.

## Commands

```text
gh-queue watch --repo owner/name [--db path] [--interval 60s]
gh-queue next [--db path] [--worker id] [--lease 90m] [--raw]
gh-queue ack --repo owner/name --number n [--db path]
gh-queue fail --repo owner/name --number n [--db path] [--reason text] [--max-attempts 5]
gh-queue stats [--db path]
```

## How it works

- `watch` polls GitHub notifications for explicitly allowlisted repositories.
- The first `watch` run initializes a checkpoint to the current time and does not backfill old notifications.
- Later watch loops query GitHub notifications since the last checkpoint minus a 10-minute safety window.
- `read`/`unread` is stored as metadata only and is never used as an actionability filter.
- Every raw notification is stored in SQLite.
- Queue items are keyed by `(repo, number)`, where `number` is the GitHub issue/PR number extracted from the notification URLs.
- `next` atomically leases one queued item by marking it `delivered`.
- If another notification arrives for an item while it is `delivered`, the item is marked `dirty`.
- `ack` marks a delivered item `done`, unless it is dirty; dirty delivered items return to `queued` for another pass.
- `fail` returns a delivered item to `queued` until `--max-attempts`, then marks it `failed`.
- Expired leases can be picked up again by `next`.

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

Lease the next item:

```bash
gh-queue next --db ~/.local/state/gh-queue.db --worker worker-1
```

Acknowledge success:

```bash
gh-queue ack --db ~/.local/state/gh-queue.db --repo owner/repo --number 123
```

Report failure and requeue/fail depending on attempts:

```bash
gh-queue fail --db ~/.local/state/gh-queue.db --repo owner/repo --number 123 --reason "processing failed"
```

If the queue is empty, `next` prints:

```text
No queued items.
```

## Development

```bash
npm install
npm run check
npm run build
```

The executable is `dist/cli.js` and the package bin is named `gh-queue`.

# gh-queue

`gh-queue` is a small Node/TypeScript CLI that turns GitHub notifications into a local SQLite job queue.

It is intentionally conservative:

- first poll does **not** backfill notifications
- polling never uses read/unread as an actionability filter
- repository filtering is explicit with `--repo`
- `run-next` claims at most one queued job and prints its payload
- it does not mark GitHub notifications read
- it does not include workflow-specific prompts or automation policy

## Install

```bash
npm install
npm run build
npm link   # optional, exposes gh-queue locally
```

Requirements:

- Node 20+
- `gh` CLI authenticated for notifications

## Setup

The default database is `./gh-queue.db`. Override it with `--db /path/to/gh-queue.db`.

On the first poll without `--backfill` or `--since`, `gh-queue` initializes a checkpoint to now and enqueues nothing:

```bash
gh-queue poll --repo owner/repo --db ~/.local/state/gh-queue.db
```

To intentionally import existing notifications:

```bash
gh-queue poll --repo owner/repo --backfill --db ~/.local/state/gh-queue.db
# or
gh-queue poll --repo owner/repo --since 2026-04-25T00:00:00Z --db ~/.local/state/gh-queue.db
```

## Commands

### `poll`

Fetches GitHub notifications with:

```bash
gh api --method GET notifications -F all=true -F per_page=100 -F since=... --paginate
```

Normal polling uses the saved `lastSeenUpdatedAt` minus a 10 minute safety window. Matching notifications are stored raw in SQLite and unseen `(notificationId, updatedAt)` pairs are enqueued as jobs.

Options:

- `--repo owner/name` — allowlist repo; repeat or comma-separate
- `--dry-run` — show what would be enqueued without writing
- `--db path` — SQLite path
- `--limit n` — cap fetched notifications after pagination output is parsed
- `--backfill` — fetch without a `since` checkpoint
- `--since ISO` — explicit checkpoint for this poll

### `run-next`

Claims one queued job, marks it `running`, records the claim, and prints the job plus raw notification payload as JSON.

Options:

- `--dry-run` — print the next job payload and leave the queue unchanged
- `--record-dry-run` — with `--dry-run`, mark the job `dry_run_complete` and record a run
- `--db path`

### Queue inspection

```bash
gh-queue status --db ~/.local/state/gh-queue.db
gh-queue jobs --status queued --db ~/.local/state/gh-queue.db
gh-queue show 12 --db ~/.local/state/gh-queue.db
```

### Queue control

```bash
gh-queue complete 12 --db ~/.local/state/gh-queue.db
gh-queue skip 12 --db ~/.local/state/gh-queue.db
gh-queue retry 12 --db ~/.local/state/gh-queue.db
```

`retry` requeues jobs in `failed`, `skipped`, `dry_run_complete`, `running`, or `complete` state.

## Cron examples

Poll every 10 minutes:

```cron
*/10 * * * * cd /path/to/gh-queue && /usr/bin/env gh-queue poll --repo owner/repo --db ~/.local/state/gh-queue.db >> ~/.local/state/gh-queue.log 2>&1
```

Claim at most one queued job every 15 minutes:

```cron
*/15 * * * * cd /path/to/gh-queue && /usr/bin/env gh-queue run-next --db ~/.local/state/gh-queue.db >> ~/.local/state/gh-queue.log 2>&1
```

Dry-run the next queued job without mutating the queue:

```bash
gh-queue run-next --dry-run --db ~/.local/state/gh-queue.db
```

## Development

```bash
npm install
npm run check
npm run build
```

The executable is `dist/cli.js` and the package bin is named `gh-queue`.

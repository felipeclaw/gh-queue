# ghq-notifications

`ghq-notifications` is a small production-oriented CLI that watches GitHub notifications for selected repositories and stores a local SQLite queue keyed by GitHub item number.

“ghq” means “GitHub queue”; this package is specifically the notifications-backed queue.

GitHub issues and pull requests share a number namespace within a repository, so multiple notifications for the same issue/PR coalesce into one queue item.

## Command summary

```text
ghq-notifications watch --repo owner/name [--db path] [--interval 60s]
ghq-notifications next [--db path] [--worker id] [--lease 90m] [--raw]
ghq-notifications ack --repo owner/name --number n [--db path]
ghq-notifications fail --repo owner/name --number n [--db path] [--reason text] [--max-attempts 5]
ghq-notifications stats [--db path]
```

## Processing contract

`ghq-notifications` is only the queue. The consumer is responsible for completing each leased item.

1. Run one long-lived watcher:
   ```bash
   ghq-notifications watch --repo owner/repo --db /var/lib/ghq-notifications/queue.db --interval 60s
   ```
2. Each worker leases work with `next`:
   ```bash
   ghq-notifications next --db /var/lib/ghq-notifications/queue.db --worker worker-1 --lease 90m
   ```
3. If `next` prints `No queued items.`, there is no work.
4. If processing succeeds, the worker **must** call `ack`:
   ```bash
   ghq-notifications ack --db /var/lib/ghq-notifications/queue.db --repo owner/repo --number 123
   ```
5. If processing fails, the worker **must** call `fail`:
   ```bash
   ghq-notifications fail --db /var/lib/ghq-notifications/queue.db --repo owner/repo --number 123 --reason "processing failed"
   ```

A leased item remains `delivered` until `ack`, `fail`, or lease expiry.

## How it works

- `watch` polls GitHub notifications for explicitly allowlisted repositories.
- The first `watch` run initializes a checkpoint to the current time and does not backfill old notifications.
- Later watch loops query GitHub notifications since the last checkpoint minus a 10-minute safety window.
- `read`/`unread` is stored as metadata only and is never used as an actionability filter.
- Every raw notification is stored in SQLite.
- Queue items are keyed by `(repo, number)`, where `number` is the GitHub issue/PR number extracted from notification URLs.
- `next` atomically leases one queued item by marking it `delivered`.
- Multiple workers can call `next` concurrently; SQLite transactions ensure only one worker leases a given item.
- If another notification arrives for an item while it is `delivered`, the item is marked `dirty`.
- `ack` marks a delivered item `done`, unless it is dirty; dirty delivered items return to `queued` for another pass.
- `fail` returns a delivered item to `queued` until `--max-attempts`, then marks it `failed`.
- If `fail` sees that the item is dirty, it returns the item to `queued` even when max attempts has been reached, so newer notifications are not buried by an older failed attempt.
- Expired leases can be picked up again by `next`.

## Payloads

`next` returns compact JSON by default:

```json
{
  "item": {
    "repo": "owner/repo",
    "number": 123,
    "type": "PullRequest",
    "status": "delivered",
    "attempts": 1,
    "notificationCount": 3,
    "latestNotificationUpdatedAt": "2026-04-26T00:00:00Z",
    "leaseUntil": "2026-04-26T01:30:00Z",
    "workerId": "worker-1"
  },
  "notifications": [
    {
      "id": "23662658022",
      "reason": "mention",
      "updatedAt": "2026-04-26T00:00:00Z",
      "unread": false,
      "subject": {
        "type": "PullRequest",
        "title": "Example PR",
        "number": 123,
        "apiUrl": "https://api.github.com/repos/owner/repo/pulls/123",
        "htmlUrl": "https://github.com/owner/repo/pull/123",
        "latestCommentApiUrl": "https://api.github.com/repos/owner/repo/issues/comments/456"
      }
    }
  ]
}
```

Use `--raw` to include the original GitHub notification payloads.

## Install

```bash
npm install
npm run build
npm link   # optional, exposes ghq-notifications locally
```

Requirements:

- Node 20+
- `gh` CLI authenticated for notifications

## Production notes

- Run exactly one watcher per GitHub account/repo allowlist/database.
- Run as many workers as you want; each worker should use a distinct `--worker` id.
- Choose `--lease` longer than the expected processing time.
- A worker should not call `ack` until all side effects for the leased item are complete.
- A worker should call `fail` when processing fails so the item can be retried or eventually marked `failed`.
- A dirty item always gets another pass, even when the current attempt fails.
- Keep the SQLite database on a local disk, not a network filesystem.
- `ghq-notifications` does not mark GitHub notifications as read.

## Development

```bash
npm install
npm run check
npm run build
```

The executable is `dist/cli.js` and the package bin is named `ghq-notifications`.

# S3 to Bunny Migration

Lightweight Cloudflare Worker app for migrating content from Amazon S3 buckets to Bunny Storage zones without downloading files locally.

## What it does

- Lists S3 buckets from a single AWS access key.
- Lists Bunny storage zones from a Bunny account API key.
- Browses folders on both sides in a two-column file-manager layout.
- Queues background transfer jobs for large migrations and tracks progress in the UI.
- Copies selected files and folders directly from S3 to Bunny using server-side streaming.
- Preserves full source paths by default so folder trees move as folders, not flattened files.

## Bunny auth note

Bunny has two different credential types:

- The Bunny account API key is the admin-style key used to list storage zones.
- The Bunny storage zone password is what the Storage HTTP API uses for file reads and writes.

This app uses the Bunny account API key to discover zones and resolve the correct storage password server-side.

## Large transfers

For migrations with 10,000+ files, the copy runs as a durable background job rather than a single request. The job runner checkpoints progress in a Durable Object and resumes automatically until the transfer finishes.

## Local setup

1. Install dependencies:

```bash
npm install
```

2. Start the Worker locally:

```bash
npm run dev
```

3. Open the local URL Wrangler prints and enter:

- AWS access key ID
- AWS secret access key
- Bunny account API key

## Deployment

```bash
npm run deploy
```

## Notes

- The first version is intentionally lightweight and keeps secrets in the browser session only while you are using the page.
- If you want, the next step can add job persistence, resumable transfers, or a queue-backed background copier for very large migrations.

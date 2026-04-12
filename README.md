# S3 to Bunny Migration

Cloudflare Worker app for copying files and folders between Amazon S3 buckets and Bunny Storage zones without downloading the data locally first.

## What It Does

- Browses a source and destination side by side.
- Supports `aws -> aws`, `bunny -> bunny`, `aws -> bunny`, and `bunny -> aws`.
- Copies files in the background through a Durable Object job runner.
- Uses a small safe-mode concurrency window so large jobs move faster without going fully wide open.
- Streams object data directly from source to destination.
- Expands folders recursively, so selecting a folder copies everything underneath it.
- Prompts when the destination already contains matching files.
- Lets you cancel queued or running jobs from the background jobs list.
- Remembers your last provider, resource, and path selection for convenience.

## Installation

1. Install dependencies.

```bash
npm install
```

2. Start the Worker locally.

```bash
npm run dev
```

3. Open the local URL Wrangler prints in your browser.

4. Enter the credentials for the providers you want to use.

5. Choose a source provider, source resource, destination provider, and destination resource, then browse to the folders or files you want to copy.

## Deployment

Deploy directly with Wrangler:

```bash
npm run deploy
```

If your repository is wired to deploy from `main`, pushing to `main` will also publish the latest version to Cloudflare.

## CLI

You can also drive the same Worker API from the terminal:

```bash
npm run cli -- preview --config transfer.json
npm run cli -- move --config transfer.json
```

Example `transfer.json`:

```json
{
  "workerUrl": "http://127.0.0.1:8787",
  "aws": {
    "accessKeyId": "AKIA...",
    "secretAccessKey": "secret",
    "region": "us-east-1"
  },
  "bunnyApiKey": "your-bunny-api-key",
  "source": {
    "provider": "aws",
    "name": "source-bucket",
    "region": "us-east-1"
  },
  "sourcePrefix": "",
  "selections": [
    { "kind": "folder", "key": "downloads/" }
  ],
  "destination": {
    "provider": "bunny",
    "name": "destination-zone",
    "region": "ny"
  },
  "destinationPrefix": "",
  "conflictMode": "replace"
}
```

The CLI also includes helpers for `jobs`, `job`, `cancel`, `retry`, `aws-buckets`, `bunny-zones`, `aws-list`, and `bunny-list`.

## How It Works

The app has two layers:

- The browser UI handles credentials, resource browsing, selection, and transfer requests.
- The Cloudflare Worker lists buckets and zones, resolves destination storage access, creates background jobs, and streams the files.

When you start a transfer:

1. The UI sends the selected source and destination details to the Worker.
2. The Worker creates a Durable Object job so the copy can continue in the background.
3. The job runner processes work in batches, which keeps large transfers from depending on a single request.
4. Folder selections are expanded recursively, so copying a folder includes every file and subfolder underneath it.
5. If the destination already contains matching items, the app asks whether to replace them, copy only new items, or cancel.
6. When the job completes, the destination view refreshes automatically so you can see the new files.
7. You can cancel a queued or running job from the background jobs list if you need to stop early.

## Credentials

Credentials are stored only in the browser, using `localStorage`.

Saved values include:

- AWS access key ID
- AWS secret access key
- Bunny account API key
- Selected source provider
- Selected destination provider
- Selected source resource
- Selected destination resource
- Current source path
- Current destination path

The app does not persist file selections across page refreshes.

Storage details:

- Saved browser data has a 24-hour TTL.
- The Worker does not store your browser credentials server-side.
- Bunny storage zone passwords are resolved on the server from the Bunny account API key when the app needs to list or copy files.

Use the `Clear credentials` button in the UI if you want to remove the saved AWS and Bunny credential fields from the browser. That keeps your browsing context available while wiping the stored secrets.

## Bunny Auth Notes

Bunny uses two credential layers:

- The Bunny account API key is used to list storage zones and resolve the storage password.
- The Bunny storage zone password is what the Bunny Storage API uses for file reads and writes.

This app uses the Bunny account API key to discover zones and resolve the correct storage password automatically.

## Notes

- File transfers are streamed directly by the Worker.
- Background jobs run in small batches, so large selections can keep making progress without blocking the browser.
- Changing a resource resets that side to the root of the selected bucket or zone.

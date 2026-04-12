# S3 to Bunny Migration

Single-process Node app for copying files and folders between Amazon S3 buckets and Bunny Storage zones.

The UI, API, and background job runner all live in one service so it can be packaged as a single Bunny container.

## Public Package

This repository publishes a Bunny-ready container image from GitHub Container Registry:

```text
ghcr.io/Go-Vennture/s3-bunny-migration
```

The `main` branch publishes the latest image tag, and every build also gets an immutable SHA tag.

You can pull the published image directly with Docker:

```bash
docker pull ghcr.io/Go-Vennture/s3-bunny-migration:main
```

To use the image publicly:

- Make the GHCR package public in GitHub, or
- Add your GitHub registry credentials in Bunny Magic Containers if you want to keep the package private.

## What It Does

- Browses a source and destination side by side.
- Supports `aws -> aws`, `bunny -> bunny`, `aws -> bunny`, and `bunny -> aws`.
- Runs copy jobs in the background with SQLite-backed job tracking.
- Streams object data directly from source to destination.
- Expands folders recursively, so selecting a folder copies everything underneath it.
- Prompts when the destination already contains matching files.
- Lets you cancel queued or running jobs from the background jobs list.
- Remembers your last provider, resource, and path selection for convenience.

## Local Development

Install dependencies:

```bash
npm install
```

Build and start the server:

```bash
npm run build
npm start
```

Or run the dev loop:

```bash
npm run dev
```

Open the URL the server prints, then enter the AWS and Bunny credentials you want to use.

## Docker

Build the container:

```bash
docker build -t s3-bunny-migration .
```

Run it locally:

```bash
docker run --rm -p 8787:80 -e PORT=80 -e HOST=0.0.0.0 s3-bunny-migration
```

The app stores its SQLite database in `./data` by default. If your Bunny container provides a persistent mount, point `DATA_DIR` or `DB_PATH` at that location.

## GitHub Container Registry

GitHub Actions publishes Bunny-ready images on every push to `main`.

Use either of these image tags in Bunny Magic Containers:

```text
ghcr.io/Go-Vennture/s3-bunny-migration:main
ghcr.io/Go-Vennture/s3-bunny-migration:<sha>
```

To auto-apply updates in Bunny on every push, set these repository variables/secrets:

- `BUNNY_MC_APP_ID`
- `BUNNY_MC_CONTAINER`
- `BUNNYNET_API_KEY`

If you keep the GHCR package private, add your GitHub registry credentials in Bunny Magic Containers under Image Registries. If you make it public, Bunny can pull it without extra registry credentials.

## CLI

You can also drive the same API from the terminal:

```bash
npm run cli -- preview --config transfer.json
npm run cli -- move --config transfer.json
```

Example `transfer.json`:

```json
{
  "apiUrl": "http://127.0.0.1:8787",
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

## Bunny Auth Notes

Bunny uses two credential layers:

- The Bunny account API key is used to list storage zones and resolve the storage password.
- The Bunny storage zone password is what the Bunny Storage HTTP API uses for file reads and writes.

This app uses the Bunny account API key to discover zones and resolve the correct storage password automatically.

## Notes

- File transfers are streamed directly from source to destination over HTTP.
- Background jobs run in small batches, so large selections can keep making progress without blocking the browser.
- Changing a resource resets that side to the root of the selected bucket or zone.

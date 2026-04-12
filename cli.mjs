#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

const DEFAULT_WORKER_URL = process.env.S3_BUNNY_WORKER_URL || "http://127.0.0.1:8787";

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(message);
  process.exitCode = 1;
});

async function main() {
  const parsed = parseArgs(process.argv.slice(2));
  const command = parsed.positionals[0];
  if (!command || parsed.flags.help || parsed.flags.h) {
    printUsage();
    return;
  }

  const configPath = parsed.flags.config || parsed.flags.c || process.env.S3_BUNNY_CONFIG;
  const config = configPath ? await loadConfig(configPath) : {};
  const workerUrl = String(parsed.flags["worker-url"] || config.workerUrl || DEFAULT_WORKER_URL).replace(/\/+$/, "");

  switch (command) {
    case "preview":
      await runPreview(workerUrl, buildTransferInput(config, parsed.flags, true));
      return;
    case "move":
      await runMove(workerUrl, buildTransferInput(config, parsed.flags, false), parsed.flags);
      return;
    case "jobs":
      await runJobs(workerUrl, parsed.flags);
      return;
    case "job":
      await runJob(workerUrl, parsed.flags);
      return;
    case "cancel":
      await runCancel(workerUrl, parsed.flags);
      return;
    case "retry":
      await runRetry(workerUrl, parsed.flags);
      return;
    case "aws-buckets":
      await runAwsBuckets(workerUrl, config, parsed.flags);
      return;
    case "bunny-zones":
      await runBunnyZones(workerUrl, config, parsed.flags);
      return;
    case "aws-list":
      await runAwsList(workerUrl, config, parsed.flags);
      return;
    case "bunny-list":
      await runBunnyList(workerUrl, config, parsed.flags);
      return;
    default:
      throw new Error(`Unknown command "${command}".`);
  }
}

function printUsage() {
  console.log([
    "Usage:",
    "  npm run cli -- preview --config transfer.json",
    "  npm run cli -- move --config transfer.json [--no-wait]",
    "  npm run cli -- jobs [--page 1]",
    "  npm run cli -- job --job-id <id>",
    "  npm run cli -- cancel --job-id <id>",
    "  npm run cli -- retry --job-id <id> --subject-key <key>",
    "  npm run cli -- aws-buckets --config transfer.json",
    "  npm run cli -- bunny-zones --config transfer.json",
    "  npm run cli -- aws-list --config transfer.json --bucket <name> [--prefix <path>]",
    "  npm run cli -- bunny-list --config transfer.json --zone <name> [--region <region>] [--path <path>]",
    "",
    "Config file shape:",
    JSON.stringify({
      workerUrl: "http://127.0.0.1:8787",
      aws: {
        accessKeyId: "AKIA...",
        secretAccessKey: "secret",
        sessionToken: "optional",
        region: "us-east-1",
      },
      bunnyApiKey: "bunny-api-key",
      source: { provider: "aws", name: "source-bucket", region: "us-east-1" },
      sourcePrefix: "",
      selections: [{ kind: "folder", key: "downloads/" }],
      destination: { provider: "bunny", name: "zone-name", region: "ny" },
      destinationPrefix: "",
      conflictMode: "replace",
    }, null, 2),
  ].join("\n"));
}

function parseArgs(argv) {
  const flags = {};
  const positionals = [];
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith("-")) {
      positionals.push(token);
      continue;
    }
    if (token.startsWith("--")) {
      const [rawKey, inlineValue] = token.slice(2).split("=", 2);
      if (inlineValue !== undefined) {
        flags[rawKey] = inlineValue;
        continue;
      }
      const next = argv[index + 1];
      if (next && !next.startsWith("-")) {
        flags[rawKey] = next;
        index += 1;
        continue;
      }
      flags[rawKey] = true;
      continue;
    }
    const short = token.slice(1);
    const next = argv[index + 1];
    if (next && !next.startsWith("-")) {
      flags[short] = next;
      index += 1;
    } else {
      flags[short] = true;
    }
  }
  return { flags, positionals };
}

async function loadConfig(configPath) {
  const file = resolve(process.cwd(), String(configPath));
  const raw = await readFile(file, "utf8");
  return JSON.parse(raw);
}

function buildTransferInput(config, flags, previewOnly) {
  const aws = config.aws || {};
  const source = config.source || {};
  const destination = config.destination || {};
  const selections = Array.isArray(config.selections) ? config.selections : [];
  const conflictMode = String(flags["conflict-mode"] || config.conflictMode || "replace");
  return {
    aws: {
      accessKeyId: String(flags["aws-access-key-id"] || aws.accessKeyId || process.env.AWS_ACCESS_KEY_ID || ""),
      secretAccessKey: String(flags["aws-secret-access-key"] || aws.secretAccessKey || process.env.AWS_SECRET_ACCESS_KEY || ""),
      sessionToken: String(flags["aws-session-token"] || aws.sessionToken || process.env.AWS_SESSION_TOKEN || "").trim() || undefined,
      region: String(flags["aws-region"] || aws.region || process.env.AWS_REGION || "us-east-1"),
    },
    bunnyApiKey: String(flags["bunny-api-key"] || config.bunnyApiKey || process.env.BUNNY_API_KEY || ""),
    source: {
      provider: String(flags["source-provider"] || source.provider || "aws"),
      name: String(flags["source-name"] || source.name || ""),
      region: String(flags["source-region"] || source.region || "us-east-1"),
    },
    sourcePrefix: String(flags["source-prefix"] || config.sourcePrefix || ""),
    selections,
    destination: {
      provider: String(flags["destination-provider"] || destination.provider || "bunny"),
      name: String(flags["destination-name"] || destination.name || ""),
      region: String(flags["destination-region"] || destination.region || ""),
    },
    destinationPrefix: String(flags["destination-prefix"] || config.destinationPrefix || ""),
    conflictMode,
    totalFiles: Number(config.totalFiles || 0) || undefined,
    previewOnly,
  };
}

async function runPreview(workerUrl, transfer) {
  const result = await postJson(`${workerUrl}/api/transfer`, { ...transfer, previewOnly: true });
  const conflicts = Array.isArray(result.payload?.conflicts) ? result.payload.conflicts.map(String) : [];
  const conflictCount = Number(result.payload?.conflictCount || conflicts.length || 0);
  const plannedFiles = Number(result.payload?.plannedFiles || 0);
  console.log(`Planned files: ${plannedFiles}`);
  console.log(`Conflicts: ${conflictCount}`);
  if (conflicts.length) {
    conflicts.forEach((item) => console.log(`  ${item}`));
  }
  if (!result.response.ok && result.response.status !== 409) {
    throw new Error(`Preview failed: ${result.response.status} ${result.response.statusText}`);
  }
}

async function runMove(workerUrl, transfer, flags) {
  const wait = flags["no-wait"] ? false : true;
  const preview = await postJson(`${workerUrl}/api/transfer`, { ...transfer, previewOnly: true });
  if (!preview.response.ok && preview.response.status !== 409) {
    throw new Error(`Preview failed: ${preview.response.status} ${preview.response.statusText}`);
  }
  const plannedFiles = Number(preview.payload?.plannedFiles || 0);
  const queued = await postJson(`${workerUrl}/api/transfer`, {
    ...transfer,
    totalFiles: plannedFiles || transfer.totalFiles,
  });
  const job = queued.payload?.job;
  if (!job || !job.id) {
    throw new Error("Worker did not return a job id.");
  }
  console.log(`Queued job ${job.id}`);
  if (!wait) {
    return;
  }
  await waitForJob(workerUrl, job.id);
}

async function runJobs(workerUrl, flags) {
  const page = String(flags.page || "1");
  const result = await getJson(`${workerUrl}/api/jobs?page=${encodeURIComponent(page)}`);
  printJson(result.payload);
}

async function runJob(workerUrl, flags) {
  const jobId = String(flags["job-id"] || flags.jobId || "");
  if (!jobId) throw new Error("Missing --job-id.");
  const result = await getJson(`${workerUrl}/api/jobs?jobId=${encodeURIComponent(jobId)}`);
  printJson(result.payload);
}

async function runCancel(workerUrl, flags) {
  const jobId = String(flags["job-id"] || flags.jobId || "");
  if (!jobId) throw new Error("Missing --job-id.");
  const result = await postJson(`${workerUrl}/api/jobs/cancel`, { jobId });
  printJson(result.payload);
}

async function runRetry(workerUrl, flags) {
  const jobId = String(flags["job-id"] || flags.jobId || "");
  const subjectKey = String(flags["subject-key"] || flags.subjectKey || "");
  if (!jobId) throw new Error("Missing --job-id.");
  if (!subjectKey) throw new Error("Missing --subject-key.");
  const result = await postJson(`${workerUrl}/api/jobs/retry`, { jobId, subjectKey });
  printJson(result.payload);
}

async function runAwsBuckets(workerUrl, config, flags) {
  const aws = config.aws || {};
  const result = await postJson(`${workerUrl}/api/aws/buckets`, {
    accessKeyId: String(flags["aws-access-key-id"] || aws.accessKeyId || process.env.AWS_ACCESS_KEY_ID || ""),
    secretAccessKey: String(flags["aws-secret-access-key"] || aws.secretAccessKey || process.env.AWS_SECRET_ACCESS_KEY || ""),
    sessionToken: String(flags["aws-session-token"] || aws.sessionToken || process.env.AWS_SESSION_TOKEN || "").trim() || undefined,
    region: String(flags["aws-region"] || aws.region || process.env.AWS_REGION || "us-east-1"),
  });
  printJson(result.payload);
}

async function runBunnyZones(workerUrl, config, flags) {
  const apiKey = String(flags["bunny-api-key"] || config.bunnyApiKey || process.env.BUNNY_API_KEY || "");
  if (!apiKey) throw new Error("Missing Bunny API key.");
  const result = await postJson(`${workerUrl}/api/bunny/zones`, { apiKey });
  printJson(result.payload);
}

async function runAwsList(workerUrl, config, flags) {
  const aws = config.aws || {};
  const bucket = String(flags.bucket || flags.b || config.source?.name || "");
  const prefix = String(flags.prefix || config.sourcePrefix || "");
  if (!bucket) throw new Error("Missing --bucket.");
  const result = await postJson(`${workerUrl}/api/aws/list`, {
    accessKeyId: String(flags["aws-access-key-id"] || aws.accessKeyId || process.env.AWS_ACCESS_KEY_ID || ""),
    secretAccessKey: String(flags["aws-secret-access-key"] || aws.secretAccessKey || process.env.AWS_SECRET_ACCESS_KEY || ""),
    sessionToken: String(flags["aws-session-token"] || aws.sessionToken || process.env.AWS_SESSION_TOKEN || "").trim() || undefined,
    region: String(flags["aws-region"] || aws.region || process.env.AWS_REGION || "us-east-1"),
    bucket,
    prefix,
  });
  printJson(result.payload);
}

async function runBunnyList(workerUrl, config, flags) {
  const apiKey = String(flags["bunny-api-key"] || config.bunnyApiKey || process.env.BUNNY_API_KEY || "");
  const zoneName = String(flags.zone || flags["zone-name"] || config.destination?.name || "");
  const region = String(flags.region || config.destination?.region || "");
  const path = String(flags.path || config.destinationPrefix || "");
  if (!apiKey) throw new Error("Missing Bunny API key.");
  if (!zoneName) throw new Error("Missing --zone.");
  const result = await postJson(`${workerUrl}/api/bunny/list`, { apiKey, zoneName, region, path });
  printJson(result.payload);
}

async function waitForJob(workerUrl, jobId) {
  let lastSnapshot = "";
  for (;;) {
    const result = await getJson(`${workerUrl}/api/jobs?jobId=${encodeURIComponent(jobId)}`);
    const job = result.payload?.job;
    if (!job) {
      throw new Error("Job not found.");
    }
    const snapshot = `${job.status}|${job.copied}|${job.skipped}|${job.failed}|${job.processed}|${job.lastKey || ""}|${job.lastError || ""}`;
    if (snapshot !== lastSnapshot) {
      lastSnapshot = snapshot;
      console.log(formatJob(job));
    }
    if (job.status === "completed" || job.status === "failed" || job.status === "cancelled") {
      if (job.status === "failed") process.exitCode = 1;
      return;
    }
    await sleep(5000);
  }
}

function formatJob(job) {
  const parts = [
    `Job ${job.id}`,
    `status=${job.status}`,
    `copied=${job.copied || 0}`,
    `skipped=${job.skipped || 0}`,
    `failed=${job.failed || 0}`,
    `processed=${job.processed || 0}`,
  ];
  if (job.lastKey) parts.push(`lastKey=${job.lastKey}`);
  if (job.lastError) parts.push(`lastError=${job.lastError}`);
  return parts.join(" ");
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const raw = await response.text();
  let payload = {};
  try {
    payload = raw ? JSON.parse(raw) : {};
  } catch {
    payload = { raw };
  }
  return { response, payload, raw };
}

async function getJson(url) {
  const response = await fetch(url);
  const raw = await response.text();
  let payload = {};
  try {
    payload = raw ? JSON.parse(raw) : {};
  } catch {
    payload = { raw };
  }
  return { response, payload, raw };
}

function printJson(value) {
  console.log(JSON.stringify(value, null, 2));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

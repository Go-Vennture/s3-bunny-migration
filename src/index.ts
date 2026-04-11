import { DurableObject } from "cloudflare:workers";
import { FAVICON_BASE64 } from "./favicon-data";

type AwsCredentials = {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
  region: string;
};

type S3Bucket = {
  name: string;
  creationDate?: string;
};

type AwsBucket = S3Bucket & {
  region: string;
};

type StorageProvider = "aws" | "bunny";

type ConflictMode = "replace" | "new";

type StorageResourceRef = {
  provider: StorageProvider;
  name: string;
  region: string;
};

type S3Item = {
  key: string;
  size: number;
  lastModified?: string;
  type: "file" | "folder";
};

type StorageItem = {
  key: string;
  name: string;
  type: "file" | "folder";
  size: number;
  lastModified?: string;
  lastChanged?: string;
};

type BunnyZone = {
  id: number;
  name: string;
  region: string;
  password?: string;
  deleted?: boolean;
};

type BunnyItem = {
  name: string;
  path: string;
  type: "file" | "folder";
  size: number;
  lastChanged?: string;
};

type TransferSelection = {
  kind: "file" | "folder";
  key: string;
};

type TransferRequest = {
  aws: AwsCredentials;
  bunnyApiKey: string;
  source: StorageResourceRef;
  sourcePrefix: string;
  selections: TransferSelection[];
  destination: StorageResourceRef;
  destinationPrefix: string;
  conflictMode?: ConflictMode;
  previewOnly?: boolean;
};

type TransferPlanEntry = {
  sourceKey: string;
  destinationKey: string;
};

type TransferConflictPreview = {
  conflictCount: number;
  conflicts: string[];
};

type ResolvedBunnyZone = BunnyZone & {
  password: string;
};

type ResolvedStorageResource = StorageResourceRef & {
  password?: string;
};

type TransferJobSelection = TransferSelection;

type TransferJobCreateRequest = {
  aws: AwsCredentials;
  source: StorageResourceRef & { password?: string };
  sourcePrefix: string;
  selections: TransferJobSelection[];
  destinationPrefix: string;
  destination: StorageResourceRef & { password?: string };
  conflictMode?: ConflictMode;
};

type TransferJobSummary = {
  id: string;
  status: string;
  sourceProvider: StorageProvider;
  sourceResource: string;
  destinationProvider: StorageProvider;
  destinationResource: string;
  sourcePrefix: string;
  destinationPrefix: string;
  selections: number;
  copied: number;
  skipped: number;
  failed: number;
  processed: number;
  lastKey?: string;
  lastError?: string;
  message?: string;
  createdAt: string;
  updatedAt: string;
};

type TransferJobDetail = TransferJobSummary & {
  currentSelectionIndex: number;
  currentFolderPrefix?: string;
  currentFolderContinuationToken?: string | null;
};

type AppEnv = Omit<Env, "TRANSFER_MANAGER"> & {
  TRANSFER_MANAGER: DurableObjectNamespace<TransferManager>;
};

type TransferJobPageState = {
  keys: string[];
  index: number;
  continuationToken?: string | null;
};

type TransferJobRow = {
  id: string;
  created_at: number;
  updated_at: number;
  status: string;
  source_provider: StorageProvider;
  source_bucket: string;
  source_region: string;
  source_prefix: string;
  source_resource_password: string | null;
  aws_access_key_id: string;
  aws_secret_access_key: string;
  aws_session_token: string | null;
  destination_provider: StorageProvider;
  destination_zone: string;
  destination_zone_region: string;
  destination_zone_password: string;
  destination_prefix: string;
  selections_json: string;
  current_selection_index: number;
  current_folder_prefix: string | null;
  current_folder_page_json: string | null;
  current_folder_continuation_token: string | null;
  copied: number;
  skipped: number;
  failed: number;
  processed: number;
  last_key: string | null;
  last_error: string | null;
  message: string | null;
  conflict_mode: ConflictMode;
};

const REGION_ENDPOINTS: Record<string, string> = {
  falkenstein: "storage.bunnycdn.com",
  "frankfurt, de": "storage.bunnycdn.com",
  de: "storage.bunnycdn.com",
  uk: "uk.storage.bunnycdn.com",
  "london, uk": "uk.storage.bunnycdn.com",
  ny: "ny.storage.bunnycdn.com",
  "new york, us": "ny.storage.bunnycdn.com",
  la: "la.storage.bunnycdn.com",
  "los angeles, us": "la.storage.bunnycdn.com",
  sg: "sg.storage.bunnycdn.com",
  "singapore, sg": "sg.storage.bunnycdn.com",
  se: "se.storage.bunnycdn.com",
  "stockholm, se": "se.storage.bunnycdn.com",
  br: "br.storage.bunnycdn.com",
  "sao paulo, br": "br.storage.bunnycdn.com",
  "são paulo, br": "br.storage.bunnycdn.com",
  sa: "jh.storage.bunnycdn.com",
  "johannesburg, sa": "jh.storage.bunnycdn.com",
  syd: "syd.storage.bunnycdn.com",
  "sydney, syd": "syd.storage.bunnycdn.com",
};

export default {
  async fetch(request: Request, env: AppEnv): Promise<Response> {
    const url = new URL(request.url);
    try {
      if (request.method === "GET" && url.pathname === "/") return new Response(renderHtml(), { headers: { "content-type": "text/html; charset=utf-8" } });
      if (request.method === "GET" && url.pathname === "/favicon.ico") return faviconResponse();
      if (request.method === "GET" && url.pathname === "/health") return json({ ok: true });
      if (request.method === "POST" && url.pathname === "/api/aws/buckets") return json({ buckets: await listAwsBuckets(await parseAwsCredentials(request)) });
      if (request.method === "POST" && url.pathname === "/api/aws/list") return await handleAwsList(request);
      if (request.method === "POST" && url.pathname === "/api/bunny/zones") return await handleBunnyZones(request);
      if (request.method === "POST" && url.pathname === "/api/bunny/list") return await handleBunnyList(request);
      if (request.method === "POST" && url.pathname === "/api/transfer") return await handleTransfer(request, env);
      if (request.method === "GET" && url.pathname === "/api/jobs") return await handleJobs(request, env);
      return new Response("Not found", { status: 404 });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return json({ error: message }, { status: 500 });
    }
  },
} satisfies ExportedHandler<AppEnv>;

function renderHtml(): string {
  return String.raw`<!doctype html>
<html lang="en">
  <head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>S3 to Bunny Migration</title>
  <link rel="icon" href="/favicon.ico" />
  <style>
    :root{
      color-scheme: light;
      --bg:#f4f1ea; --bg2:#ece6dc; --panel:rgba(255,255,255,.86); --border:rgba(47,45,41,.12);
      --text:#1f1b16; --muted:#6f665b; --accent:#0f766e; --accent2:#b45309; --shadow:0 18px 45px rgba(34,29,24,.12);
      --radius:20px;
      font-family:ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
    }
    *{box-sizing:border-box}
    body{margin:0;min-height:100vh;color:var(--text);background:radial-gradient(circle at top left, rgba(15,118,110,.14), transparent 32%),radial-gradient(circle at top right, rgba(180,83,9,.14), transparent 26%),linear-gradient(180deg,var(--bg),var(--bg2))}
    .shell{max-width:1440px;margin:0 auto;padding:24px}
    header{display:grid;gap:12px;margin-bottom:20px}
    .eyebrow{letter-spacing:.14em;text-transform:uppercase;font-size:12px;color:var(--accent2);font-weight:800}
    h1{margin:0;font-size:clamp(28px,4vw,50px);line-height:1.02}
    .lede{max-width:980px;margin:0;font-size:16px;line-height:1.55;color:var(--muted)}
    .statusbar{display:flex;flex-wrap:wrap;gap:10px;align-items:center}
    .pill{display:inline-flex;align-items:center;gap:8px;border:1px solid var(--border);background:rgba(255,255,255,.72);border-radius:999px;padding:9px 14px;font-size:13px;color:var(--muted)}
    .pill strong{color:var(--text)}
    .clear-credentials{display:inline-flex;align-items:center;justify-content:center;min-height:unset;padding:9px 14px;border-radius:999px;background:rgba(255,255,255,.84);color:var(--text);border:1px solid rgba(47,45,41,.16);font-size:13px;font-weight:700}
    .layout{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:18px}
    .contents-layout{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:18px;align-items:stretch;margin-top:18px}
    .panel{background:var(--panel);border:1px solid var(--border);border-radius:var(--radius);box-shadow:var(--shadow);backdrop-filter:blur(16px);overflow:hidden}
    .panel-head{padding:18px 18px 14px;border-bottom:1px solid rgba(47,45,41,.08)}
    .panel-head h2{margin:0 0 6px;font-size:20px}
    .panel-head p{margin:0;font-size:13px;line-height:1.45;color:var(--muted)}
    .panel-body{padding:16px 18px 18px;display:grid;gap:12px}
    .grid-2{display:grid;grid-template-columns:1fr 1fr;gap:10px}
    .credential-actions{display:flex;justify-content:flex-end;margin-top:4px}
    .row{display:flex;gap:10px;align-items:end;flex-wrap:wrap}
    .field{display:grid;gap:6px;flex:1 1 240px}
    .wide-field{flex:1 1 100%;min-width:0}
    .field label{font-size:12px;letter-spacing:.03em;color:var(--muted);font-weight:700}
    .field input,.field select{width:100%;min-height:42px;border-radius:12px;border:1px solid rgba(47,45,41,.18);background:rgba(255,255,255,.92);color:var(--text);padding:10px 12px;font:inherit}
    .field input:focus,.field select:focus,button:focus{outline:2px solid rgba(15,118,110,.26);outline-offset:2px}
    .pathbar{display:flex;gap:8px;align-items:end}
    .pathbar .mini{width:44px;flex:0 0 44px;padding-inline:0}
    button{border:0;border-radius:12px;min-height:42px;padding:10px 14px;font:inherit;font-weight:700;cursor:pointer;transition:transform .15s ease,box-shadow .15s ease,opacity .15s ease}
    button:hover{transform:translateY(-1px)}
    .primary{background:linear-gradient(135deg,var(--accent),#115e59);color:#fff;box-shadow:0 10px 24px rgba(15,118,110,.24)}
    .secondary{background:rgba(255,255,255,.84);color:var(--text);border:1px solid rgba(47,45,41,.16)}
    .ghost{background:transparent;color:var(--muted);border:1px solid rgba(47,45,41,.12)}
    button:disabled{opacity:.55;cursor:not-allowed;transform:none}
    .contents-card{background:var(--panel);border:1px solid var(--border);border-radius:var(--radius);box-shadow:var(--shadow);backdrop-filter:blur(16px);padding:18px;display:flex;flex-direction:column;min-height:450px}
    .contents-card h3{margin:0 0 10px;font-size:18px}
    .contents-card .inline-note{margin:0 0 12px}
    .contents-controls{display:grid;gap:12px;margin-bottom:12px}
    .contents-select-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:10px;align-items:end}
    .contents-path-row{display:grid;grid-template-columns:auto minmax(0,1fr) auto;gap:10px;align-items:end}
    .contents-path-row .field{margin:0}
    .list{border:1px solid rgba(47,45,41,.12);border-radius:16px;overflow:hidden;background:rgba(255,255,255,.72);overflow-y:auto}
    .contents-list{flex:none;height:540px;min-height:0;overflow-y:auto}
    .list-head,.list-row{display:grid;grid-template-columns:34px minmax(0,1fr) 120px 140px;align-items:center;gap:10px;padding:10px 12px}
    .list-head{position:sticky;top:0;background:rgba(244,241,234,.96);border-bottom:1px solid rgba(47,45,41,.08);font-size:12px;color:var(--muted);font-weight:700;letter-spacing:.03em;z-index:1}
    .list-row{border-bottom:1px solid rgba(47,45,41,.06)}
    .list-row:last-child{border-bottom:0}
    .list-row:hover{background:rgba(15,118,110,.06)}
    .name-cell{display:flex;gap:10px;align-items:center;min-width:0}
    .icon{width:30px;height:30px;border-radius:10px;display:grid;place-items:center;background:rgba(15,118,110,.12);color:var(--accent);flex:0 0 auto}
    .name-cell button.link{background:none;border:0;padding:0;min-height:0;text-align:left;color:var(--text);font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .meta{color:var(--muted);font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .inline-note{font-size:12px;color:var(--muted);line-height:1.45}
    .inline-note.error{color:#b42318;font-weight:600}
    .check{width:16px;height:16px}
    .footer{margin-top:18px;display:grid;gap:12px}
    .transfer-card{background:rgba(255,255,255,.84);border:1px solid var(--border);border-radius:var(--radius);box-shadow:var(--shadow);padding:18px;display:grid;gap:14px}
    .transfer-card h3{margin:0;font-size:18px}
    .summary{display:flex;flex-wrap:wrap;gap:10px;align-items:center}
    .selection-pill{display:inline-flex;align-items:center;gap:6px;border-radius:999px;padding:7px 10px;font-size:12px;background:rgba(15,118,110,.08);color:var(--accent);border:1px solid rgba(15,118,110,.14)}
    .log{display:grid;gap:8px;background:rgba(255,255,255,.7);border:1px solid rgba(47,45,41,.12);border-radius:16px;padding:12px;min-height:96px;max-height:240px;overflow-y:auto;font-size:13px;color:var(--muted)}
    .jobs-list{min-height:220px}
    .jobs-list .list-head,.jobs-list .list-row{grid-template-columns:minmax(140px,1.2fr) 100px 120px minmax(0,1.6fr)}
    .error{color:#b42318}
    .dialog-backdrop[hidden]{display:none !important}
    .dialog-backdrop{position:fixed;inset:0;background:rgba(28,24,20,.48);backdrop-filter:blur(10px);display:grid;place-items:center;padding:20px;z-index:9999;pointer-events:auto}
    .dialog-card{position:relative;width:min(560px,100%);background:#fff;border:1px solid rgba(47,45,41,.12);border-radius:24px;box-shadow:0 24px 80px rgba(28,24,20,.24);padding:20px;display:grid;gap:14px;pointer-events:auto}
    .dialog-card h3{margin:0;font-size:20px}
    .dialog-card p{margin:0;color:var(--muted);line-height:1.5}
    .dialog-list{display:grid;gap:6px;max-height:220px;overflow-y:auto;border:1px solid rgba(47,45,41,.08);border-radius:16px;padding:12px;background:rgba(244,241,234,.42);font-size:13px;color:var(--text)}
    .dialog-actions{display:flex;flex-wrap:wrap;gap:10px;justify-content:flex-end}
    .dialog-actions .ghost,.dialog-actions .secondary{min-height:42px}
    @media (max-width:1100px){.layout,.contents-layout{grid-template-columns:1fr}}
    @media (max-width:720px){.grid-2{grid-template-columns:1fr}.list-head,.list-row{grid-template-columns:34px minmax(0,1fr) 86px}.size-col{display:none}}
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div class="eyebrow">Cloud to cloud migration</div>
      <h1>S3 to Bunny without the local detour</h1>
      <p class="lede">Browse source and destination side by side, pick folders or files, and stream objects directly from AWS S3 into Bunny Storage on the server.</p>
      <div class="statusbar">
        <span class="pill"><strong>Source</strong> AWS S3</span>
        <span class="pill"><strong>Destination</strong> Bunny Storage</span>
        <button class="clear-credentials" id="clearCredentials" type="button">Clear credentials</button>
      </div>
    </header>

    <section class="layout">
      <article class="panel">
        <div class="panel-head">
          <h2>Shared AWS credentials</h2>
          <p>Used whenever a source or destination side is set to AWS.</p>
        </div>
        <div class="panel-body">
          <div class="grid-2">
            <div class="field"><label for="awsAccessKeyId">AWS access key ID</label><input id="awsAccessKeyId" autocomplete="off" spellcheck="false" /></div>
            <div class="field"><label for="awsSecretAccessKey">AWS secret access key</label><input id="awsSecretAccessKey" type="password" autocomplete="off" spellcheck="false" /></div>
          </div>
        </div>
      </article>

      <article class="panel">
        <div class="panel-head">
          <h2>Shared Bunny credentials</h2>
          <p>Used whenever a source or destination side is set to Bunny.</p>
        </div>
        <div class="panel-body">
          <div class="field"><label for="bunnyApiKey">Bunny account API key</label><input id="bunnyApiKey" type="password" autocomplete="off" spellcheck="false" /></div>
        </div>
      </article>
    </section>

    <section class="contents-layout">
      <article class="contents-card">
        <h3>Source</h3>
        <div class="inline-note" id="sourceStatus">Load a resource to see folders and files.</div>
        <div class="contents-controls">
          <div class="contents-select-row">
            <div class="field" style="margin:0">
              <label for="sourceProviderSelect">Source provider</label>
              <select id="sourceProviderSelect">
                <option value="aws">AWS</option>
                <option value="bunny">Bunny</option>
              </select>
            </div>
          </div>
          <div class="contents-select-row">
            <div class="field" style="margin:0">
              <label for="sourceResourceSelect">Resource</label>
              <select id="sourceResourceSelect"><option value="">Load resources first</option></select>
            </div>
            <button class="secondary" id="loadSourceResources">Refresh resources</button>
          </div>
          <div class="contents-path-row">
            <button class="ghost mini" id="sourceUp" title="Parent folder">..</button>
            <div class="field" style="margin:0"><label for="sourcePrefix">Path</label><input id="sourcePrefix" autocomplete="off" spellcheck="false" placeholder="optional/prefix/" value="" /></div>
            <button class="secondary" id="loadSourcePath">Refresh</button>
          </div>
        </div>
        <div class="list contents-list" id="sourceList" aria-live="polite"></div>
      </article>
      <article class="contents-card">
        <h3>Destination</h3>
        <div class="inline-note" id="destinationStatus">Load a resource to browse its contents.</div>
        <div class="contents-controls">
          <div class="contents-select-row">
            <div class="field" style="margin:0">
              <label for="destinationProviderSelect">Destination provider</label>
              <select id="destinationProviderSelect">
                <option value="aws">AWS</option>
                <option value="bunny">Bunny</option>
              </select>
            </div>
          </div>
          <div class="contents-select-row">
            <div class="field" style="margin:0">
              <label for="destinationResourceSelect">Resource</label>
              <select id="destinationResourceSelect"><option value="">Load resources first</option></select>
            </div>
            <button class="secondary" id="loadDestinationResources">Refresh resources</button>
          </div>
          <div class="contents-path-row">
            <button class="ghost mini" id="destinationUp" title="Parent folder">..</button>
            <div class="field" style="margin:0"><label for="destinationPrefix">Path</label><input id="destinationPrefix" autocomplete="off" spellcheck="false" placeholder="destination/prefix/" value="" /></div>
            <button class="secondary" id="loadDestinationPath">Refresh</button>
          </div>
        </div>
        <div class="list contents-list" id="destinationList" aria-live="polite"></div>
      </article>
    </section>

    <section class="footer">
      <div class="transfer-card">
        <h3>Transfer selected items</h3>
        <div class="summary">
          <span class="selection-pill" id="selectionCount">0 selected</span>
          <span class="pill" id="destinationSummary">Destination root</span>
        </div>
        <div class="row">
          <button class="primary" id="transferButton">Start background transfer</button>
          <button class="secondary" id="clearSelection">Clear selection</button>
        </div>
        <div class="log" id="log"></div>
      </div>
      <div class="transfer-card">
        <h3>Background jobs</h3>
        <div class="inline-note">Queued jobs keep running in the Durable Object and you can watch progress here.</div>
        <div class="list jobs-list" id="jobsList" aria-live="polite"></div>
        <div class="inline-note" id="jobsStatus"></div>
      </div>
    </section>
  </div>
  <div class="dialog-backdrop" id="conflictDialog" hidden>
    <div class="dialog-card" role="dialog" aria-modal="true" aria-labelledby="conflictDialogTitle">
      <h3 id="conflictDialogTitle">Existing items found</h3>
      <p id="conflictDialogMessage"></p>
      <div class="dialog-list" id="conflictDialogList"></div>
      <div class="dialog-actions">
        <button type="button" class="secondary" id="conflictReplace" data-conflict-choice="replace">Replace existing</button>
        <button type="button" class="secondary" id="conflictNew" data-conflict-choice="new">Copy only new</button>
        <button type="button" class="ghost" id="conflictCancel" data-conflict-choice="cancel">Cancel</button>
      </div>
    </div>
  </div>

  <script>
    (() => {
      const $ = (id) => document.getElementById(id);
      const bootError = (message) => {
        const existing = document.getElementById("log");
        const text = "Application error: " + message;
        if (existing) {
          const row = document.createElement("div");
          row.innerHTML = '<span class="error"><strong>Error:</strong> ' + text.replace(/[&<>"']/g, (character) => {
            if (character === "&") return "&amp;";
            if (character === "<") return "&lt;";
            if (character === ">") return "&gt;";
            if (character === '"') return "&quot;";
            return "&#39;";
          }) + '</span>';
          existing.appendChild(row);
        } else {
          console.error(text);
        }
      };

      try {
            const els = {
        awsAccessKeyId: $("awsAccessKeyId"),
        awsSecretAccessKey: $("awsSecretAccessKey"),
        bunnyApiKey: $("bunnyApiKey"),
        clearCredentials: $("clearCredentials"),
        sourceProviderSelect: $("sourceProviderSelect"),
        sourceResourceSelect: $("sourceResourceSelect"),
        sourcePrefix: $("sourcePrefix"),
        sourceList: $("sourceList"),
        sourceStatus: $("sourceStatus"),
        sourceUp: $("sourceUp"),
        loadSourceResources: $("loadSourceResources"),
        loadSourcePath: $("loadSourcePath"),
        destinationProviderSelect: $("destinationProviderSelect"),
        destinationResourceSelect: $("destinationResourceSelect"),
        destinationPrefix: $("destinationPrefix"),
        destinationList: $("destinationList"),
        destinationStatus: $("destinationStatus"),
        destinationUp: $("destinationUp"),
        loadDestinationResources: $("loadDestinationResources"),
        loadDestinationPath: $("loadDestinationPath"),
        selectionCount: $("selectionCount"),
        destinationSummary: $("destinationSummary"),
        transferButton: $("transferButton"),
        clearSelection: $("clearSelection"),
        log: $("log"),
        jobsList: $("jobsList"),
        jobsStatus: $("jobsStatus"),
        conflictDialog: $("conflictDialog"),
        conflictDialogMessage: $("conflictDialogMessage"),
        conflictDialogList: $("conflictDialogList"),
        conflictReplace: $("conflictReplace"),
        conflictNew: $("conflictNew"),
        conflictCancel: $("conflictCancel"),
      };

      const STORAGE_KEY = "s3-bunny-migration:ui-state:v3";
      const STORAGE_TTL_MS = 24 * 60 * 60 * 1000;

      const state = {
        awsBuckets: [],
        bunnyZones: [],
        sourceItems: [],
        sourceContinuationToken: null,
        sourceSelections: new Map(),
        destinationItems: [],
        destinationContinuationToken: null,
        jobs: [],
        transferQueueNotice: "",
      };
      let conflictDialogResolve = null;
      let uiPersistenceEnabled = false;
      let pendingDestinationRefresh = null;

      function escapeHtml(value) {
        return String(value)
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/\"/g, "&quot;");
      }

      function formatBytes(bytes) {
        if (!Number.isFinite(bytes)) return "";
        const units = ["B", "KB", "MB", "GB", "TB"];
        let value = bytes;
        let unit = 0;
        while (value >= 1024 && unit < units.length - 1) {
          value /= 1024;
          unit += 1;
        }
        return String(value.toFixed(value >= 10 || unit === 0 ? 0 : 1)) + " " + units[unit];
      }

      function ensureTrailingSlash(value) {
        const trimmed = value.trim();
        if (!trimmed) return "";
        return trimmed.endsWith("/") ? trimmed : trimmed + "/";
      }

      function parentPrefix(value) {
        const trimmed = value.replace(/^\/+/, "").replace(/\/+$/, "");
        if (!trimmed) return "";
        const parts = trimmed.split("/");
        parts.pop();
        return parts.length ? parts.join("/") + "/" : "";
      }

      function joinPrefix(prefix, child) {
        const left = prefix ? prefix.replace(/\/+$/, "") : "";
        const right = child ? child.replace(/^\/+/, "") : "";
        if (!left) return right;
        if (!right) return left + "/";
        return left + "/" + right;
      }

      function sideElements(side) {
        return side === "source"
          ? {
              providerSelect: els.sourceProviderSelect,
              resourceSelect: els.sourceResourceSelect,
              prefix: els.sourcePrefix,
              list: els.sourceList,
              status: els.sourceStatus,
              up: els.sourceUp,
              loadResources: els.loadSourceResources,
              loadPath: els.loadSourcePath,
            }
          : {
              providerSelect: els.destinationProviderSelect,
              resourceSelect: els.destinationResourceSelect,
              prefix: els.destinationPrefix,
              list: els.destinationList,
              status: els.destinationStatus,
              up: els.destinationUp,
              loadResources: els.loadDestinationResources,
              loadPath: els.loadDestinationPath,
            };
      }

      function providerLabel(provider) {
        return provider === "aws" ? "AWS" : "Bunny";
      }

      function selectedProvider(side) {
        return sideElements(side).providerSelect.value === "bunny" ? "bunny" : "aws";
      }

      function selectedResource(side) {
        const provider = selectedProvider(side);
        const value = sideElements(side).resourceSelect.value.trim();
        if (!value) return null;
        return provider === "aws"
          ? state.awsBuckets.find((bucket) => bucket.name === value) || null
          : state.bunnyZones.find((zone) => zone.name === value) || null;
      }

      function sideItems(side) {
        return side === "source" ? state.sourceItems : state.destinationItems;
      }

      function setSideItems(side, items) {
        if (side === "source") state.sourceItems = items;
        else state.destinationItems = items;
      }

      function sideContinuationToken(side) {
        return side === "source" ? state.sourceContinuationToken : state.destinationContinuationToken;
      }

      function setSideContinuationToken(side, token) {
        if (side === "source") state.sourceContinuationToken = token;
        else state.destinationContinuationToken = token;
      }

      function log(message, kind = "info") {
        const row = document.createElement("div");
        row.innerHTML = kind === "error" ? '<span class="error"><strong>Error:</strong> ' + escapeHtml(message) + '</span>' : escapeHtml(message);
        els.log.appendChild(row);
        els.log.scrollTop = els.log.scrollHeight;
      }

      function errorMessage(error) {
        return error instanceof Error ? error.message : String(error ?? "Unknown error");
      }

      function readUiState() {
        try {
          const raw = window.localStorage.getItem(STORAGE_KEY);
          if (!raw) return {};
          const parsed = JSON.parse(raw);
          if (!parsed || typeof parsed !== "object") return {};
          const state = parsed;
          const expiresAt = typeof state.expiresAt === "number" ? state.expiresAt : 0;
          if (expiresAt && Date.now() > expiresAt) {
            window.localStorage.removeItem(STORAGE_KEY);
            return {};
          }
          return state;
        } catch {
          return {};
        }
      }

      function writeUiState() {
        if (!uiPersistenceEnabled) {
          return;
        }
        try {
          window.localStorage.setItem(STORAGE_KEY, JSON.stringify({
            expiresAt: Date.now() + STORAGE_TTL_MS,
            awsAccessKeyId: els.awsAccessKeyId.value,
            awsSecretAccessKey: els.awsSecretAccessKey.value,
            bunnyApiKey: els.bunnyApiKey.value,
            sourceProvider: els.sourceProviderSelect.value,
            sourceResource: els.sourceResourceSelect.value,
            sourcePrefix: els.sourcePrefix.value,
            destinationProvider: els.destinationProviderSelect.value,
            destinationResource: els.destinationResourceSelect.value,
            destinationPrefix: els.destinationPrefix.value,
          }));
        } catch {
          // Local storage can be unavailable in some browser privacy modes.
        }
      }

      function clearCredentials() {
        els.awsAccessKeyId.value = "";
        els.awsSecretAccessKey.value = "";
        els.bunnyApiKey.value = "";
        try {
          window.localStorage.removeItem(STORAGE_KEY);
        } catch {
          // Ignore storage failures when the browser blocks persistence.
        }
        writeUiState();
      }

      function restoreUiState() {
        const saved = readUiState();
        els.awsAccessKeyId.value = saved.awsAccessKeyId || "";
        els.awsSecretAccessKey.value = saved.awsSecretAccessKey || "";
        els.bunnyApiKey.value = saved.bunnyApiKey || "";
        els.sourceProviderSelect.value = saved.sourceProvider || (saved.awsBucketSelect ? "aws" : "aws");
        els.sourceResourceSelect.value = saved.sourceResource || saved.awsBucketSelect || "";
        els.sourcePrefix.value = saved.sourcePrefix || saved.awsPrefix || "";
        els.destinationProviderSelect.value = saved.destinationProvider || (saved.bunnyZoneSelect ? "bunny" : "bunny");
        els.destinationResourceSelect.value = saved.destinationResource || saved.bunnyZoneSelect || "";
        els.destinationPrefix.value = saved.destinationPrefix || saved.bunnyPrefix || "";
        state.sourceSelections = new Map();
      }

      function setStatus(target, message, kind = "info") {
        target.textContent = message;
        target.classList.toggle("error", kind === "error");
      }

      function summarizeSide(side) {
        const provider = selectedProvider(side);
        const resource = selectedResource(side);
        const prefix = ensureTrailingSlash(sideElements(side).prefix.value).replace(/\/+$/, "");
        if (!resource) {
          return providerLabel(provider) + " root";
        }
        return prefix ? providerLabel(provider) + " " + resource.name + "/" + prefix : providerLabel(provider) + " " + resource.name;
      }

      function syncSummary() {
        els.selectionCount.textContent = String(state.sourceSelections.size) + " selected";
        els.destinationSummary.textContent = summarizeSide("source") + " -> " + summarizeSide("destination");
        writeUiState();
      }

      function populateResourceSelect(side, resources) {
        const elements = sideElements(side);
        const provider = selectedProvider(side);
        const saved = readUiState();
        const persisted = side === "source"
          ? String(saved.sourceResource || saved.awsBucketSelect || "").trim()
          : String(saved.destinationResource || saved.bunnyZoneSelect || "").trim();
        const previous = elements.resourceSelect.value.trim() || persisted;
        if (!resources.length) {
          elements.resourceSelect.innerHTML = '<option value="">' + (provider === "aws" ? "No buckets returned" : "No storage zones returned") + '</option>';
          elements.resourceSelect.value = "";
          return;
        }
        elements.resourceSelect.innerHTML = ['<option value="">' + (provider === "aws" ? "Choose a bucket" : "Choose a zone") + '</option>'].concat(
          resources.map((resource) => '<option value="' + escapeHtml(resource.name) + '">' + escapeHtml(resource.name) + '</option>'),
        ).join("");
        if (previous && resources.some((resource) => resource.name === previous)) {
          elements.resourceSelect.value = previous;
        } else {
          elements.resourceSelect.value = resources[0].name;
        }
        writeUiState();
      }

      function sortResources(resources) {
        return Array.isArray(resources)
          ? resources.slice().sort((left, right) => String(left.name || "").localeCompare(String(right.name || "")))
          : [];
      }

      function normalizeAwsItems(items) {
        return Array.isArray(items)
          ? items.map((item) => ({
              key: String(item.key || ""),
              type: item.type === "folder" ? "folder" : "file",
              size: Number(item.size || 0),
              lastModified: String(item.lastModified || ""),
            })).filter((item) => item.key)
          : [];
      }

      function normalizeBunnyItems(items) {
        return Array.isArray(items)
          ? items.map((item) => ({
              key: String(item.path || ""),
              name: String(item.name || ""),
              type: item.type === "folder" ? "folder" : "file",
              size: Number(item.size || 0),
              lastChanged: String(item.lastChanged || ""),
            })).filter((item) => item.key)
          : [];
      }

      function renderSideList(side) {
        const elements = sideElements(side);
        const items = sideItems(side);
        const header = side === "source"
          ? '<div class="list-head"><div><input class="check" type="checkbox" id="sourceSelectAll" title="Select all visible" /></div><div>Name</div><div class="size-col">Size</div><div>Modified</div></div>'
          : '<div class="list-head"><div></div><div>Name</div><div class="size-col">Size</div><div>Modified</div></div>';
        if (!items.length) {
          elements.list.innerHTML = header + '<div class="list-row"><div></div><div class="meta">No objects loaded yet.</div><div></div><div></div></div>';
          return;
        }
        const rows = items.map((item) => {
          const label = item.type === "folder"
            ? item.key.replace(/\/+$/, "").split("/").pop() + "/"
            : item.key.split("/").pop();
          const checked = side === "source" && state.sourceSelections.has(item.key) ? "checked" : "";
          return '<div class="list-row">' +
            (side === "source" ? '<div><input class="check" type="checkbox" data-key="' + escapeHtml(item.key) + '" ' + checked + ' /></div>' : '<div></div>') +
            '<div class="name-cell"><div class="icon">' + (item.type === "folder" ? "+" : "-") + '</div><button class="link" data-open="' + escapeHtml(item.key) + '">' + escapeHtml(label) + '</button></div>' +
            '<div class="size-col meta">' + (item.type === "folder" ? "Folder" : formatBytes(item.size)) + '</div>' +
            '<div class="meta">' + escapeHtml(item.lastModified || item.lastChanged || "") + '</div>' +
            '</div>';
        }).join("");
        const more = sideContinuationToken(side) ? '<div class="list-row"><div></div><div><button class="secondary" id="loadMore' + (side === "source" ? "Source" : "Destination") + '">Load more</button></div><div></div><div></div></div>' : "";
        elements.list.innerHTML = header + rows + more;
        if (side === "source") {
          const selectAll = $("sourceSelectAll");
          if (selectAll) {
            const visibleKeys = items.map((item) => item.key);
            const allSelected = visibleKeys.length > 0 && visibleKeys.every((key) => state.sourceSelections.has(key));
            const someSelected = visibleKeys.some((key) => state.sourceSelections.has(key));
            selectAll.checked = allSelected;
            selectAll.indeterminate = !allSelected && someSelected;
            selectAll.addEventListener("change", () => {
              const checked = selectAll.checked;
              visibleKeys.forEach((key) => {
                if (checked) state.sourceSelections.set(key, true);
                else state.sourceSelections.delete(key);
              });
              renderSideList("source");
              syncSummary();
            });
          }
          elements.list.querySelectorAll('input[type="checkbox"][data-key]').forEach((checkbox) => {
            checkbox.addEventListener("change", () => {
              if (checkbox.checked) state.sourceSelections.set(checkbox.dataset.key, true);
              else state.sourceSelections.delete(checkbox.dataset.key);
              renderSideList("source");
              syncSummary();
            });
          });
        }
        elements.list.querySelectorAll("[data-open]").forEach((button) => {
          button.addEventListener("click", () => {
            const key = button.dataset.open || "";
            if (key.endsWith("/")) {
              elements.prefix.value = key;
              writeUiState();
              loadPath(side, false);
            }
          });
        });
        const moreButton = $("loadMore" + (side === "source" ? "Source" : "Destination"));
        if (moreButton) {
          moreButton.addEventListener("click", () => loadPath(side, true));
        }
      }

      function renderJobs() {
        const header = '<div class="list-head"><div>Job</div><div>Status</div><div>Progress</div><div>Details</div></div>';
        const queueNotice = state.transferQueueNotice;
        if (!state.jobs.length) {
          els.jobsList.innerHTML = header + '<div class="list-row"><div class="meta">No jobs yet.</div><div></div><div></div><div class="meta">Start a transfer to queue one.</div></div>';
          els.jobsStatus.textContent = queueNotice || "No background jobs running.";
          return;
        }
        els.jobsList.innerHTML = header + state.jobs.map((job) => {
          const statusLabel = job.status === "completed" && job.failed ? "completed with warnings" : job.status;
          const progressParts = [];
          if (job.copied) progressParts.push(String(job.copied) + " copied");
          if (job.skipped) progressParts.push(String(job.skipped) + " skipped");
          if (job.failed) progressParts.push(String(job.failed) + " failed");
          const progress = job.status === "completed"
            ? (progressParts.length ? progressParts.join(" / ") : "Completed")
            : (progressParts.length ? progressParts.join(" / ") : "Running");
          const details = job.lastKey ? escapeHtml(job.lastKey) : escapeHtml(job.message || "");
          const route = providerLabel(job.sourceProvider) + " " + escapeHtml(job.sourceResource) + " -> " + providerLabel(job.destinationProvider) + " " + escapeHtml(job.destinationResource);
          return '<div class="list-row">' +
            '<div>' +
              '<div><strong>' + escapeHtml(job.id.slice(0, 8)) + '</strong></div>' +
              '<div class="meta">' + route + '</div>' +
            '</div>' +
            '<div class="meta">' + escapeHtml(statusLabel) + '</div>' +
            '<div class="meta">' + escapeHtml(progress) + '</div>' +
            '<div class="meta">' + details + (job.lastError ? '<div class="error">' + escapeHtml(job.lastError) + '</div>' : '') + '</div>' +
          '</div>';
        }).join("");
        const running = state.jobs.find((job) => job.status === "running" || job.status === "queued");
        els.jobsStatus.textContent = queueNotice || (running ? "Latest active job: " + running.id : "No active jobs.");
      }

      async function postJsonResponse(path, body) {
        const response = await fetch(path, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        });
        const raw = await response.text();
        let payload = {};
        if (raw) {
          try {
            payload = JSON.parse(raw);
          } catch {
            payload = { raw };
          }
        }
        return { response, payload, raw };
      }

      async function postJson(path, body) {
        const { response, payload, raw } = await postJsonResponse(path, body);
        if (!response.ok) {
          const detail = payload && typeof payload === "object" && payload.error ? payload.error : raw;
          throw new Error(detail
            ? "Request failed (" + String(response.status) + " " + (response.statusText || "unknown") + "): " + String(detail)
            : "Request failed (" + String(response.status) + " " + (response.statusText || "unknown") + ")");
        }
        return payload;
      }

      function renderConflictList(conflicts, total) {
        const visible = conflicts.slice(0, 8);
        const overflow = total > visible.length ? '<div class="meta">And ' + String(total - visible.length) + ' more.</div>' : "";
        return visible.map((item) => "<div>" + escapeHtml(item) + "</div>").join("") + overflow;
      }

      function closeConflictDialog(value) {
        if (!els.conflictDialog || !els.conflictDialogMessage || !els.conflictDialogList) return;
        els.conflictDialog.hidden = true;
        document.body.style.overflow = "";
        const resolve = conflictDialogResolve;
        conflictDialogResolve = null;
        if (resolve) resolve(value);
      }

      function askTransferConflict(conflicts, total) {
        return new Promise((resolve) => {
          if (!els.conflictDialog || !els.conflictDialogMessage || !els.conflictDialogList) {
            resolve("replace");
            return;
          }
          if (conflictDialogResolve) {
            conflictDialogResolve(null);
          }
          conflictDialogResolve = resolve;
          els.conflictDialogMessage.textContent = String(total) + " matching file" + (total === 1 ? "" : "s") + " already exist in the destination. Choose how to continue.";
          els.conflictDialogList.innerHTML = renderConflictList(conflicts, total);
          els.conflictDialog.hidden = false;
          document.body.style.overflow = "hidden";
        });
      }

      async function loadResources(side) {
        const provider = selectedProvider(side);
        const elements = sideElements(side);
        if (provider === "aws") {
          if (!els.awsAccessKeyId.value.trim() || !els.awsSecretAccessKey.value.trim()) {
            setStatus(elements.status, "Enter AWS credentials first.", "error");
            return;
          }
          setStatus(elements.status, "Loading buckets...");
          try {
            const payload = await postJson("/api/aws/buckets", {
              accessKeyId: els.awsAccessKeyId.value.trim(),
              secretAccessKey: els.awsSecretAccessKey.value.trim(),
            });
            state.awsBuckets = sortResources(Array.isArray(payload.buckets) ? payload.buckets : []);
            ["source", "destination"].forEach((targetSide) => {
              if (selectedProvider(targetSide) === "aws") {
                populateResourceSelect(targetSide, state.awsBuckets);
              }
            });
            setStatus(elements.status, String(state.awsBuckets.length) + " bucket(s) loaded.");
            log("Loaded " + String(state.awsBuckets.length) + " AWS bucket(s).");
            if (selectedResource(side)) {
              await loadPath(side, false);
            }
          } catch (error) {
            setStatus(elements.status, "Could not load buckets.", "error");
            log(errorMessage(error), "error");
          }
          return;
        }
        if (!els.bunnyApiKey.value.trim()) {
          setStatus(elements.status, "Enter a Bunny API key first.", "error");
          return;
        }
        setStatus(elements.status, "Loading zones...");
        try {
          const payload = await postJson("/api/bunny/zones", { apiKey: els.bunnyApiKey.value.trim() });
          state.bunnyZones = sortResources(Array.isArray(payload.zones) ? payload.zones : []);
          ["source", "destination"].forEach((targetSide) => {
            if (selectedProvider(targetSide) === "bunny") {
              populateResourceSelect(targetSide, state.bunnyZones);
            }
          });
          if (!state.bunnyZones.length) {
            setStatus(elements.status, "No storage zones were returned for that Bunny API key.", "error");
            log("Bunny returned 0 storage zones. Double-check that you used the Bunny account API key from the dashboard, not a storage zone password, and that the account actually has storage zones.", "error");
            return;
          }
          setStatus(elements.status, String(state.bunnyZones.length) + " zone(s) loaded.");
          log("Loaded " + String(state.bunnyZones.length) + " Bunny storage zone(s).");
          if (selectedResource(side)) {
            await loadPath(side, false);
          }
        } catch (error) {
          setStatus(elements.status, "Could not load zones.", "error");
          log(errorMessage(error), "error");
        }
      }

      async function loadPath(side, append = false) {
        const provider = selectedProvider(side);
        const elements = sideElements(side);
        const resource = selectedResource(side);
        if (!resource) {
          log("Choose a " + (provider === "aws" ? "bucket" : "storage zone") + " first.", "error");
          return;
        }
        const prefix = ensureTrailingSlash(elements.prefix.value);
        if (!append) {
          setSideItems(side, []);
          setSideContinuationToken(side, null);
        }
        setStatus(elements.status, "Loading " + providerLabel(provider) + " " + resource.name + "/" + prefix + "...");
        try {
          let items = [];
          let nextContinuationToken = null;
          if (provider === "aws") {
            const payload = await postJson("/api/aws/list", {
              accessKeyId: els.awsAccessKeyId.value.trim(),
              secretAccessKey: els.awsSecretAccessKey.value.trim(),
              region: resource.region,
              bucket: resource.name,
              prefix,
              continuationToken: append ? sideContinuationToken(side) : undefined,
            });
            items = normalizeAwsItems(payload.items || []);
            nextContinuationToken = payload.nextContinuationToken || null;
          } else {
            const payload = await postJson("/api/bunny/list", {
              apiKey: els.bunnyApiKey.value.trim(),
              zoneName: resource.name,
              region: resource.region,
              path: prefix,
            });
            items = normalizeBunnyItems(payload.items || []);
          }
          setSideItems(side, append ? sideItems(side).concat(items) : items);
          setSideContinuationToken(side, nextContinuationToken);
          renderSideList(side);
          setStatus(elements.status, String(sideItems(side).length) + " item(s) visible." + (sideContinuationToken(side) ? " More available." : ""));
          syncSummary();
        } catch (error) {
          setStatus(elements.status, "Could not load " + providerLabel(provider) + " contents.", "error");
          log(errorMessage(error), "error");
        }
      }

      async function refreshJobs(cleanupFailed = false) {
        try {
          const response = await fetch("/api/jobs" + (cleanupFailed ? "?cleanupFailed=1" : ""));
          const payload = await response.json().catch(() => ({}));
          if (!response.ok) {
            throw new Error(payload && payload.error ? payload.error : response.statusText || "Request failed");
          }
          state.jobs = payload.jobs || [];
          renderJobs();
          if (pendingDestinationRefresh) {
            const job = state.jobs.find((item) => item.id === pendingDestinationRefresh.jobId);
            const completed = job && job.status === "completed";
            const failed = job && job.status === "failed";
            if (completed || failed) {
              const matchesDestination = selectedProvider("destination") === pendingDestinationRefresh.provider &&
                selectedResource("destination") &&
                selectedResource("destination").name === pendingDestinationRefresh.resource;
              pendingDestinationRefresh = null;
              if (completed && matchesDestination) {
                await loadPath("destination", false);
              }
            }
          }
        } catch (error) {
          setStatus(els.jobsStatus, "Could not refresh jobs.", "error");
          log(errorMessage(error), "error");
        }
      }

      async function startTransfer() {
        const selections = Array.from(state.sourceSelections.keys()).map((key) => {
          const item = state.sourceItems.find((entry) => entry.key === key);
          return { kind: item && item.type === "folder" ? "folder" : "file", key };
        });
        if (!selections.length) {
          log("Select at least one file or folder to copy.", "error");
          return;
        }
        const sourceResource = selectedResource("source");
        const destinationResource = selectedResource("destination");
        if (!sourceResource || !destinationResource) {
          log("Choose both a source and destination resource first.", "error");
          return;
        }
        els.transferButton.disabled = true;
        state.transferQueueNotice = "";
        setStatus(els.sourceStatus, "Checking destination for conflicts...");
        setStatus(els.destinationStatus, "Checking destination for conflicts...");
        log("Checking transfer of " + String(selections.length) + " selected item(s).");
        try {
          const payload = {
            aws: {
              accessKeyId: els.awsAccessKeyId.value.trim(),
              secretAccessKey: els.awsSecretAccessKey.value.trim(),
              region: sourceResource.region,
            },
            bunnyApiKey: els.bunnyApiKey.value.trim(),
            source: {
              provider: selectedProvider("source"),
              name: sourceResource.name,
              region: sourceResource.region,
            },
            sourcePrefix: ensureTrailingSlash(els.sourcePrefix.value),
            selections,
            destination: {
              provider: selectedProvider("destination"),
              name: destinationResource.name,
              region: destinationResource.region,
            },
            destinationPrefix: ensureTrailingSlash(els.destinationPrefix.value),
          };
          const preview = await postJsonResponse("/api/transfer", {
            ...payload,
            previewOnly: true,
          });
          if (!preview.response.ok && preview.response.status !== 409) {
            const previewError = preview.payload && typeof preview.payload === "object" && preview.payload.error ? preview.payload.error : preview.raw;
            throw new Error(previewError
              ? "Preview failed (" + String(preview.response.status) + " " + (preview.response.statusText || "unknown") + "): " + String(previewError)
              : "Preview failed (" + String(preview.response.status) + " " + (preview.response.statusText || "unknown") + ")");
          }
          const conflicts = Array.isArray(preview.payload.conflicts) ? preview.payload.conflicts.map((item) => String(item)) : [];
          const conflictCount = Number(preview.payload.conflictCount || conflicts.length || 0);
          const conflictMode = conflictCount > 0 ? await askTransferConflict(conflicts, conflictCount) : "replace";
          if (!conflictMode) {
            setStatus(els.sourceStatus, "Transfer cancelled.");
            setStatus(els.destinationStatus, "Transfer cancelled.");
            log("Transfer cancelled.");
            return;
          }
          setStatus(els.sourceStatus, "Queueing background job...");
          setStatus(els.destinationStatus, "Queueing background job...");
          state.transferQueueNotice = "Queuing transfer of " + String(selections.length) + " selected item(s)...";
          renderJobs();
          log("Queueing transfer of " + String(selections.length) + " selected item(s).");
          const queued = await postJson("/api/transfer", {
            ...payload,
            conflictMode,
          });
          state.transferQueueNotice = "";
          log("Job " + queued.job.id + " queued.");
          pendingDestinationRefresh = {
            jobId: queued.job.id,
            provider: selectedProvider("destination"),
            resource: destinationResource.name,
          };
          state.sourceSelections.clear();
          renderSideList("source");
          setStatus(els.sourceStatus, "Job queued: " + queued.job.id);
          setStatus(els.destinationStatus, "Job queued: " + queued.job.id);
          syncSummary();
          await refreshJobs();
        } catch (error) {
          state.transferQueueNotice = "";
          renderJobs();
          log(errorMessage(error), "error");
          setStatus(els.sourceStatus, "Queue failed.", "error");
          setStatus(els.destinationStatus, "Queue failed.", "error");
        } finally {
          state.transferQueueNotice = "";
          els.transferButton.disabled = false;
          renderJobs();
        }
      }

      restoreUiState();
      const startupLoads = [];
      if (els.awsAccessKeyId.value.trim() && els.awsSecretAccessKey.value.trim()) {
        if (selectedProvider("source") === "aws") startupLoads.push(loadResources("source"));
        if (selectedProvider("destination") === "aws") startupLoads.push(loadResources("destination"));
      }
      if (els.bunnyApiKey.value.trim()) {
        if (selectedProvider("source") === "bunny") startupLoads.push(loadResources("source"));
        if (selectedProvider("destination") === "bunny") startupLoads.push(loadResources("destination"));
      }

      [
        els.awsAccessKeyId,
        els.awsSecretAccessKey,
        els.bunnyApiKey,
        els.sourceProviderSelect,
        els.sourceResourceSelect,
        els.sourcePrefix,
        els.destinationProviderSelect,
        els.destinationResourceSelect,
        els.destinationPrefix,
      ].forEach((element) => {
        element.addEventListener("input", writeUiState);
        element.addEventListener("change", writeUiState);
      });

      els.loadSourceResources.addEventListener("click", () => loadResources("source"));
      els.loadSourcePath.addEventListener("click", () => loadPath("source", false));
      els.loadDestinationResources.addEventListener("click", () => loadResources("destination"));
      els.loadDestinationPath.addEventListener("click", () => loadPath("destination", false));
      els.transferButton.addEventListener("click", startTransfer);
      els.clearCredentials.addEventListener("click", () => {
        clearCredentials();
        log("Credentials cleared.");
      });
      els.conflictReplace.addEventListener("click", () => closeConflictDialog("replace"));
      els.conflictNew.addEventListener("click", () => closeConflictDialog("new"));
      els.conflictCancel.addEventListener("click", () => closeConflictDialog(null));
      document.addEventListener("click", (event) => {
        if (!els.conflictDialog || els.conflictDialog.hidden) return;
        const target = event.target;
        if (!(target instanceof Element)) return;
        const choice = target.closest("[data-conflict-choice]");
        if (choice instanceof HTMLElement) {
          event.preventDefault();
          const action = choice.getAttribute("data-conflict-choice");
          if (action === "replace") {
            closeConflictDialog("replace");
          } else if (action === "new") {
            closeConflictDialog("new");
          } else {
            closeConflictDialog(null);
          }
          return;
        }
        if (target === els.conflictDialog) {
          closeConflictDialog(null);
        }
      });
      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && els.conflictDialog && !els.conflictDialog.hidden) {
          event.preventDefault();
          closeConflictDialog(null);
        }
      });
      els.clearSelection.addEventListener("click", () => {
        state.sourceSelections.clear();
        renderSideList("source");
        syncSummary();
        log("Selection cleared.");
        writeUiState();
      });
      els.sourceUp.addEventListener("click", () => {
        els.sourcePrefix.value = parentPrefix(els.sourcePrefix.value);
        syncSummary();
        writeUiState();
        loadPath("source", false);
      });
      els.destinationUp.addEventListener("click", () => {
        els.destinationPrefix.value = parentPrefix(els.destinationPrefix.value);
        syncSummary();
        writeUiState();
        loadPath("destination", false);
      });
      els.sourceProviderSelect.addEventListener("change", () => {
        state.sourceSelections.clear();
        state.sourceItems = [];
        state.sourceContinuationToken = null;
        els.sourcePrefix.value = "";
        populateResourceSelect("source", selectedProvider("source") === "aws" ? state.awsBuckets : state.bunnyZones);
        renderSideList("source");
        syncSummary();
        writeUiState();
        void loadResources("source");
      });
      els.destinationProviderSelect.addEventListener("change", () => {
        state.destinationItems = [];
        state.destinationContinuationToken = null;
        els.destinationPrefix.value = "";
        populateResourceSelect("destination", selectedProvider("destination") === "aws" ? state.awsBuckets : state.bunnyZones);
        renderSideList("destination");
        syncSummary();
        writeUiState();
        void loadResources("destination");
      });
      els.sourceResourceSelect.addEventListener("change", () => {
        state.sourceSelections.clear();
        els.sourcePrefix.value = "";
        syncSummary();
        writeUiState();
        void loadPath("source", false);
      });
      els.destinationResourceSelect.addEventListener("change", () => {
        els.destinationPrefix.value = "";
        syncSummary();
        writeUiState();
        void loadPath("destination", false);
      });
      els.sourcePrefix.addEventListener("change", () => {
        syncSummary();
        writeUiState();
        void loadPath("source", false);
      });
      els.destinationPrefix.addEventListener("change", () => {
        syncSummary();
        writeUiState();
        void loadPath("destination", false);
      });
      els.sourcePrefix.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          void loadPath("source", false);
        }
      });
      els.destinationPrefix.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          void loadPath("destination", false);
        }
      });
      els.sourceList.addEventListener("change", (event) => {
        const target = event.target;
        if (target instanceof HTMLInputElement && target.dataset.key) {
          if (target.checked) state.sourceSelections.set(target.dataset.key, true);
          else state.sourceSelections.delete(target.dataset.key);
          syncSummary();
        }
      });
      window.addEventListener("pagehide", writeUiState);
      window.addEventListener("beforeunload", writeUiState);

      renderSideList("source");
      renderSideList("destination");
      renderJobs();
      syncSummary();
      void Promise.allSettled(startupLoads).then(() => {
        uiPersistenceEnabled = true;
        writeUiState();
      });
      void refreshJobs(true);
      setInterval(() => {
        void refreshJobs();
      }, 5000);
      log("Choose a source and destination provider, then load resources and browse the folders you want to migrate.");
      } catch (error) {
        bootError(errorMessage(error));
      }
    })();
  </script>
</body>
</html>`;
}

function json(data: unknown, init?: ResponseInit): Response {
  return new Response(JSON.stringify(data, null, 2), {
    ...init,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...(init?.headers || {}),
    },
  });
}

async function handleAwsList(request: Request): Promise<Response> {
  const body = await parseJson(request);
  const obj = body as Record<string, unknown>;
  const creds = requireAwsCredentials(body);
  const bucket = typeof obj.bucket === "string" ? obj.bucket : "";
  const prefix = typeof obj.prefix === "string" ? obj.prefix : "";
  const continuationToken = typeof obj.continuationToken === "string" ? obj.continuationToken : undefined;
  if (!bucket) throw new Error("Missing bucket.");
  return json(await listAwsObjects(creds, bucket, prefix, continuationToken));
}

async function handleBunnyZones(request: Request): Promise<Response> {
  const body = await parseJson(request);
  const obj = body as Record<string, unknown>;
  const apiKey = typeof obj.apiKey === "string" ? obj.apiKey : "";
  if (!apiKey) throw new Error("Missing apiKey.");
  console.log("[bunny] /api/bunny/zones requested");
  return json({ zones: await listBunnyZones(apiKey) });
}

async function handleBunnyList(request: Request): Promise<Response> {
  const body = await parseJson(request);
  const obj = body as Record<string, unknown>;
  const apiKey = typeof obj.apiKey === "string" ? obj.apiKey : "";
  const zoneName = typeof obj.zoneName === "string" ? obj.zoneName : "";
  const region = typeof obj.region === "string" ? obj.region : "";
  const path = typeof obj.path === "string" ? obj.path : "";
  if (!apiKey) throw new Error("Missing apiKey.");
  if (!zoneName) throw new Error("Missing zoneName.");
  const zone = await resolveBunnyZone(apiKey, zoneName);
  return json({ items: await listBunnyObjects(zone, region, path) });
}

async function handleTransfer(request: Request, env: AppEnv): Promise<Response> {
  const transfer = parseTransferRequest(await parseJson(request));
  const [source, destination] = await Promise.all([
    resolveStorageResource(transfer.bunnyApiKey, transfer.source),
    resolveStorageResource(transfer.bunnyApiKey, transfer.destination),
  ]);
  if (transfer.previewOnly) {
    const plan = await buildTransferPlan(source, transfer.aws, transfer.selections, transfer.sourcePrefix);
    const preview = await detectTransferConflicts(transfer.aws, destination, plan, transfer.destinationPrefix);
    const body: TransferConflictPreview = {
      conflictCount: preview.conflicts.length,
      conflicts: preview.conflicts.slice(0, 20),
    };
    return json(body, { status: preview.conflicts.length ? 409 : 200 });
  }
  const manager = getTransferManagerStub(env);
  const job = await manager.createJob({
    aws: transfer.aws,
    source,
    sourcePrefix: transfer.sourcePrefix,
    selections: transfer.selections,
    destinationPrefix: transfer.destinationPrefix || "",
    destination,
    conflictMode: transfer.conflictMode || "replace",
  });
  return json({ job }, { status: 202 });
}

async function handleJobs(request: Request, env: AppEnv): Promise<Response> {
  const url = new URL(request.url);
  const manager = getTransferManagerStub(env);
  if (url.searchParams.get("cleanupFailed") === "1") {
    await manager.clearFailedJobs();
  }
  const jobId = url.searchParams.get("jobId");
  if (jobId) {
    const job = await manager.getJob(jobId);
    if (!job) {
      return json({ error: "Job not found" }, { status: 404 });
    }
    return json({ job });
  }
  return json({ jobs: await manager.listJobs() });
}

function getTransferManagerStub(env: AppEnv): DurableObjectStub<TransferManager> {
  return env.TRANSFER_MANAGER.getByName("transfer-manager");
}

function faviconResponse(): Response {
  return new Response(base64ToBytes(FAVICON_BASE64), {
    headers: {
      "content-type": "image/x-icon",
      "cache-control": "public, max-age=86400",
    },
  });
}

function base64ToBytes(base64: string): ArrayBuffer {
  const binary = atob(base64.replace(/\s+/g, ""));
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
}

async function parseAwsCredentials(request: Request): Promise<AwsCredentials> {
  return requireAwsCredentials(await parseJson(request));
}

function parseTransferRequest(body: unknown): TransferRequest {
  if (!body || typeof body !== "object") throw new Error("Request body must be an object.");
  const obj = body as Record<string, unknown>;
  const aws = obj.aws as Record<string, unknown> | undefined;
  if (!aws) throw new Error("Missing aws credentials.");
  const source = parseStorageResource(obj.source, "source");
  const destination = parseStorageResource(obj.destination, "destination");
  const bunnyApiKey = typeof obj.bunnyApiKey === "string" ? obj.bunnyApiKey.trim() : "";
  if ((source.provider === "bunny" || destination.provider === "bunny") && !bunnyApiKey) {
    throw new Error("Missing bunnyApiKey.");
  }
  const selections = Array.isArray(obj.selections)
    ? obj.selections.map((selection) => {
        const item = selection as Record<string, unknown>;
        return {
          kind: item.kind === "folder" ? "folder" : "file",
          key: requireString(item.key, "selection.key"),
        } satisfies TransferSelection;
      })
    : [];
  return {
    aws: {
      accessKeyId: requireString(aws.accessKeyId, "aws.accessKeyId"),
      secretAccessKey: requireString(aws.secretAccessKey, "aws.secretAccessKey"),
      sessionToken: typeof aws.sessionToken === "string" && aws.sessionToken.trim() ? aws.sessionToken.trim() : undefined,
      region: typeof aws.region === "string" && aws.region.trim() ? aws.region.trim() : "us-east-1",
    },
    bunnyApiKey,
    source,
    sourcePrefix: typeof obj.sourcePrefix === "string" ? obj.sourcePrefix : "",
    selections,
    destination,
    destinationPrefix: typeof obj.destinationPrefix === "string" ? obj.destinationPrefix : "",
    conflictMode: obj.conflictMode === "new" ? "new" : "replace",
    previewOnly: obj.previewOnly === true,
  };
}

function parseStorageResource(value: unknown, label: string): StorageResourceRef {
  if (!value || typeof value !== "object") throw new Error(`Missing ${label}.`);
  const obj = value as Record<string, unknown>;
  const provider = obj.provider === "aws" || obj.provider === "bunny" ? (obj.provider as StorageProvider) : "";
  if (!provider) throw new Error(`Missing ${label}.provider.`);
  return {
    provider,
    name: requireString(obj.name, `${label}.name`),
    region: requireString(obj.region, `${label}.region`),
  };
}

function requireAwsCredentials(body: unknown): AwsCredentials {
  if (!body || typeof body !== "object") throw new Error("Request body must be an object.");
  const obj = body as Record<string, unknown>;
  return {
    accessKeyId: requireString(obj.accessKeyId, "accessKeyId"),
    secretAccessKey: requireString(obj.secretAccessKey, "secretAccessKey"),
    sessionToken: typeof obj.sessionToken === "string" && obj.sessionToken.trim() ? obj.sessionToken.trim() : undefined,
    region: typeof obj.region === "string" && obj.region.trim() ? obj.region.trim() : "us-east-1",
  };
}

function requireString(value: unknown, label: string): string {
  if (typeof value !== "string" || !value.trim()) throw new Error(`Missing ${label}.`);
  return value.trim();
}

async function listAwsBuckets(creds: AwsCredentials): Promise<AwsBucket[]> {
  const response = await signedS3RequestToHost(creds, "GET", "s3.amazonaws.com", "us-east-1", "/", new URLSearchParams(), undefined);
  if (!response.ok) throw new Error(await response.text());
  const buckets = parseBucketsXml(await response.text());
  const regions = await Promise.all(
    buckets.map(async (bucket) => {
      try {
        return await resolveAwsBucketRegion(creds, bucket.name);
      } catch {
        return creds.region || "us-east-1";
      }
    }),
  );
  return buckets.map((bucket, index) => ({
    ...bucket,
    region: regions[index] || "us-east-1",
  }));
}

async function listAwsObjects(
  creds: AwsCredentials,
  bucket: string,
  prefix: string,
  continuationToken?: string,
  options?: { delimiter?: string; maxKeys?: number },
): Promise<{ items: S3Item[]; nextContinuationToken?: string }> {
  const query = new URLSearchParams();
  query.set("list-type", "2");
  if (!options) {
    query.set("delimiter", "/");
  } else if (options.delimiter !== undefined) {
    query.set("delimiter", options.delimiter);
  }
  if (typeof options?.maxKeys === "number") {
    query.set("max-keys", String(options.maxKeys));
  }
  if (prefix) query.set("prefix", prefix);
  if (continuationToken) query.set("continuation-token", continuationToken);
  const response = await signedS3Request(creds, "GET", `/${bucket}`, query, undefined);
  if (!response.ok) throw new Error(await response.text());
  return parseObjectsXml(await response.text());
}

async function listAllS3Objects(creds: AwsCredentials, bucket: string, prefix: string): Promise<S3Item[]> {
  const items: S3Item[] = [];
  let token: string | undefined;
  do {
    const page = await listAwsObjects(creds, bucket, prefix, token, { maxKeys: 1000 });
    items.push(...page.items);
    token = page.nextContinuationToken;
  } while (token);
  return items;
}

async function signedS3Request(
  creds: AwsCredentials,
  method: string,
  path: string,
  query: URLSearchParams,
  body?: BodyInit | null,
): Promise<Response> {
  return signedS3RequestToHost(creds, method, `s3.${creds.region}.amazonaws.com`, creds.region, path, query, body);
}

async function signedS3RequestToHost(
  creds: AwsCredentials,
  method: string,
  host: string,
  signingRegion: string,
  path: string,
  query: URLSearchParams,
  body?: BodyInit | null,
  payloadHashOverride?: string,
): Promise<Response> {
  const url = `https://${host}${encodeUrlPath(path)}${query.toString() ? `?${query.toString()}` : ""}`;
  const amzDate = toAmzDate(new Date());
  const dateStamp = amzDate.slice(0, 8);
  const payloadHash = payloadHashOverride || await sha256Hex(body ?? "");
  const headers = new Headers({
    host,
    "x-amz-date": amzDate,
    "x-amz-content-sha256": payloadHash,
  });
  if (creds.sessionToken) headers.set("x-amz-security-token", creds.sessionToken);
  headers.set(
    "authorization",
    await signAwsRequest({
      method,
      path,
      query,
      headers,
      payloadHash,
      dateStamp,
      region: signingRegion,
      accessKeyId: creds.accessKeyId,
      secretAccessKey: creds.secretAccessKey,
    }),
  );
  return fetch(url, { method, headers, body });
}

async function signAwsRequest(params: {
  method: string;
  path: string;
  query: URLSearchParams;
  headers: Headers;
  payloadHash: string;
  dateStamp: string;
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
}): Promise<string> {
  const headerPairs: Array<[string, string]> = [];
  params.headers.forEach((value, key) => {
    headerPairs.push([key.toLowerCase(), value.trim()]);
  });
  headerPairs.sort(([a], [b]) => a.localeCompare(b));
  const signedHeaders = headerPairs.map(([key]) => key).join(";");
  const canonicalHeaders = headerPairs.map(([key, value]) => `${key}:${value}\n`).join("");
  const canonicalRequest = [
    params.method.toUpperCase(),
    encodeAwsPath(params.path),
    canonicalQuery(params.query),
    canonicalHeaders,
    signedHeaders,
    params.payloadHash,
  ].join("\n");
  const scope = `${params.dateStamp}/${params.region}/s3/aws4_request`;
  const stringToSign = ["AWS4-HMAC-SHA256", params.headers.get("x-amz-date") || "", scope, await sha256Hex(canonicalRequest)].join("\n");
  const signingKey = await deriveSigningKey(params.secretAccessKey, params.dateStamp, params.region, "s3");
  const signature = await hmacHex(signingKey, stringToSign);
  return `AWS4-HMAC-SHA256 Credential=${params.accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
}

async function deriveSigningKey(secretAccessKey: string, dateStamp: string, region: string, service: string): Promise<ArrayBuffer> {
  const kDate = await hmac(`AWS4${secretAccessKey}`, dateStamp);
  const kRegion = await hmac(kDate, region);
  const kService = await hmac(kRegion, service);
  return hmac(kService, "aws4_request");
}

async function hmac(key: string | ArrayBuffer, data: string): Promise<ArrayBuffer> {
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    typeof key === "string" ? new TextEncoder().encode(key) : key,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  return crypto.subtle.sign("HMAC", cryptoKey, new TextEncoder().encode(data));
}

async function hmacHex(key: ArrayBuffer, data: string): Promise<string> {
  return hex(await hmac(key, data));
}

async function sha256Hex(data: BodyInit | ArrayBuffer | string): Promise<string> {
  return hex(await crypto.subtle.digest("SHA-256", await toBytes(data)));
}

async function toBytes(data: BodyInit | ArrayBuffer | string): Promise<ArrayBuffer> {
  if (typeof data === "string") return new TextEncoder().encode(data).buffer;
  if (data instanceof ArrayBuffer) return data;
  if (ArrayBuffer.isView(data)) return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
  if (data instanceof Blob) return data.arrayBuffer();
  return new TextEncoder().encode(String(data)).buffer;
}

function hex(buffer: ArrayBuffer): string {
  return [...new Uint8Array(buffer)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function canonicalQuery(query: URLSearchParams): string {
  const pairs: Array<[string, string]> = [];
  query.forEach((value, key) => {
    pairs.push([awsEncode(key), awsEncode(value)]);
  });
  pairs.sort(([aKey, aValue], [bKey, bValue]) => (aKey === bKey ? aValue.localeCompare(bValue) : aKey.localeCompare(bKey)));
  return pairs.map(([key, value]) => `${key}=${value}`).join("&");
}

function awsEncode(value: string): string {
  return encodeURIComponent(value)
    .replace(/[!'()*]/g, (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`)
    .replace(/%7E/g, "~");
}

function encodeAwsPath(path: string): string {
  if (!path || path === "/") return "/";
  return path
    .split("/")
    .map((segment) => awsEncode(segment))
    .join("/");
}

function toAmzDate(date: Date): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hour = String(date.getUTCHours()).padStart(2, "0");
  const minute = String(date.getUTCMinutes()).padStart(2, "0");
  const second = String(date.getUTCSeconds()).padStart(2, "0");
  return `${year}${month}${day}T${hour}${minute}${second}Z`;
}

function parseBucketsXml(xml: string): S3Bucket[] {
  const bucketsBlock = matchBlock(xml, "Buckets");
  if (!bucketsBlock) return [];
  return matchBlocks(bucketsBlock, "Bucket").map((block) => ({
    name: extractTag(block, "Name"),
    creationDate: extractTag(block, "CreationDate"),
  }));
}

async function resolveAwsBucketRegion(creds: AwsCredentials, bucket: string): Promise<string> {
  const query = new URLSearchParams();
  query.set("location", "");
  const response = await signedS3RequestToHost(creds, "GET", "s3.amazonaws.com", "us-east-1", `/${bucket}`, query, undefined);
  if (!response.ok) throw new Error(await response.text());
  const xml = await response.text();
  return normalizeAwsRegionLocation(extractTagFlexible(xml, "LocationConstraint"));
}

function normalizeAwsRegionLocation(value: string): string {
  const trimmed = value.trim();
  if (!trimmed || trimmed.toLowerCase() === "null") return "us-east-1";
  if (trimmed === "EU") return "eu-west-1";
  return trimmed;
}

function parseObjectsXml(xml: string): { items: S3Item[]; nextContinuationToken?: string } {
  const items: S3Item[] = [];
  for (const block of matchBlocks(xml, "CommonPrefixes")) {
    const key = extractTag(block, "Prefix");
    if (key) items.push({ key, size: 0, type: "folder", lastModified: "" });
  }
  for (const block of matchBlocks(xml, "Contents")) {
    const key = extractTag(block, "Key");
    if (!key || key.endsWith("/")) continue;
    items.push({
      key,
      size: Number(extractTag(block, "Size") || 0),
      lastModified: extractTag(block, "LastModified"),
      type: "file",
    });
  }
  items.sort((a, b) => {
    if (a.type !== b.type) return a.type === "folder" ? -1 : 1;
    return a.key.localeCompare(b.key);
  });
  return { items, nextContinuationToken: extractTag(xml, "NextContinuationToken") || undefined };
}

function matchBlock(xml: string, tag: string): string | null {
  const pattern = new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`);
  const match = xml.match(pattern);
  return match ? match[1] : null;
}

function matchBlocks(xml: string, tag: string): string[] {
  const pattern = new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, "g");
  return Array.from(xml.matchAll(pattern), (match) => match[1]);
}

function extractTag(xml: string, tag: string): string {
  const pattern = new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`);
  const match = xml.match(pattern);
  return match ? decodeXml(match[1]) : "";
}

function extractTagFlexible(xml: string, tag: string): string {
  const pattern = new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)</${tag}>`);
  const match = xml.match(pattern);
  return match ? decodeXml(match[1]) : "";
}

function decodeXml(value: string): string {
  return value
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'");
}

async function listBunnyZones(apiKey: string): Promise<BunnyZone[]> {
  const url = new URL("https://api.bunny.net/storagezone");
  url.searchParams.set("perPage", "1000");
  const response = await fetch(url, {
    headers: { AccessKey: apiKey },
  });
  const rawText = await response.text();
  console.log("[bunny] storagezone response", {
    ok: response.ok,
    status: response.status,
    contentType: response.headers.get("content-type"),
    bodyPreview: rawText.slice(0, 500),
  });
  if (!response.ok) throw new Error(rawText);
  const payload = JSON.parse(rawText) as unknown;
  const zones = normalizeBunnyZonePayload(payload);
  console.log("[bunny] parsed storage zones", { count: zones.length });
  return zones;
}

function normalizeBunnyZonePayload(payload: unknown): BunnyZone[] {
  const items = Array.isArray(payload)
    ? payload
    : payload && typeof payload === "object"
      ? ((payload as Record<string, unknown>).Items ?? (payload as Record<string, unknown>).items)
      : [];
  if (!Array.isArray(items)) return [];
  return items
    .map((item) => {
      const zone = item as Record<string, unknown>;
      const id = Number(zone.id ?? zone.Id ?? 0);
      const name = String(zone.name ?? zone.Name ?? "");
      const region = String(zone.region ?? zone.Region ?? "");
      const passwordValue = zone.password ?? zone.Password;
      const deletedValue = zone.deleted ?? zone.Deleted;
      return {
        id,
        name,
        region,
        password: typeof passwordValue === "string" ? passwordValue : undefined,
        deleted: Boolean(deletedValue),
      };
    })
    .filter((zone) => !zone.deleted && zone.id && zone.name && zone.region);
}

async function resolveBunnyZone(apiKey: string, zoneName: string): Promise<ResolvedBunnyZone> {
  const zones = await listBunnyZones(apiKey);
  const zone = zones.find((item) => item.name === zoneName);
  if (!zone) throw new Error(`Could not find Bunny storage zone "${zoneName}".`);
  if (!zone.password) throw new Error(`Bunny zone "${zoneName}" did not return a storage password.`);
  return zone as ResolvedBunnyZone;
}

async function listBunnyObjects(zone: BunnyZone, region: string, path: string): Promise<BunnyItem[]> {
  const endpoint = bunnyEndpointForRegion(region || zone.region);
  const requestPath = bunnyPath(zone.name, path);
  const response = await fetch(`https://${endpoint}${requestPath}`, {
    headers: { AccessKey: zone.password || "" },
  });
  if (!response.ok) throw new Error(await response.text());
  const payload = (await response.json()) as BunnyItem[] | { Items?: BunnyItem[]; items?: BunnyItem[] };
  const rawItems = Array.isArray(payload) ? payload : payload.Items || payload.items || [];
  return rawItems
    .map((item) => ({
      name: String((item as Record<string, unknown>).ObjectName || item.name || ""),
      path: bunnyObjectKey(
        zone.name,
        String((item as Record<string, unknown>).Path || item.path || ""),
        String((item as Record<string, unknown>).ObjectName || item.name || ""),
        (((item as Record<string, unknown>).IsDirectory || item.type) ? "folder" : "file") as BunnyItem["type"],
      ),
      type: (((item as Record<string, unknown>).IsDirectory || item.type) ? "folder" : "file") as BunnyItem["type"],
      size: Number((item as Record<string, unknown>).Length || item.size || 0),
      lastChanged: String((item as Record<string, unknown>).LastChanged || item.lastChanged || ""),
    }))
    .filter((item) => item.name);
}

function bunnyEndpointForRegion(region: string): string {
  const key = String(region || "").trim().toLowerCase();
  return REGION_ENDPOINTS[key] || "storage.bunnycdn.com";
}

function bunnyPath(zoneName: string, path: string): string {
  const encodedZone = encodeURIComponent(zoneName);
  const cleanPath = path
    .split("/")
    .map((segment) => segment.trim())
    .filter(Boolean)
    .map((segment) => encodeURIComponent(segment))
    .join("/");
  return cleanPath ? `/${encodedZone}/${cleanPath}/` : `/${encodedZone}/`;
}

function bunnyObjectKey(zoneName: string, path: string, name: string, type: BunnyItem["type"]): string {
  const zoneKey = normalizeBunnyKey(zoneName);
  const cleanPath = normalizeBunnyKey(path);
  const cleanName = normalizeBunnyKey(name);
  let key = cleanPath && cleanName ? `${cleanPath}/${cleanName}` : cleanPath || cleanName;
  if (zoneKey && key) {
    const zonePrefix = `${zoneKey}/`;
    while (key === zoneKey || key.startsWith(zonePrefix)) {
      key = key === zoneKey ? "" : key.slice(zonePrefix.length);
    }
  }
  if (!key) return "";
  return type === "folder" && !key.endsWith("/") ? `${key}/` : key;
}

function normalizeBunnyKey(value: string): string {
  return String(value || "")
    .split("/")
    .map((segment) => segment.trim())
    .filter(Boolean)
    .join("/");
}

async function buildTransferPlan(
  source: ResolvedStorageResource,
  aws: AwsCredentials,
  selections: TransferSelection[],
  sourcePrefix: string,
): Promise<TransferPlanEntry[]> {
  const plan: TransferPlanEntry[] = [];
  for (const selection of selections) {
    if (selection.kind === "file") {
      if (!selection.key.endsWith("/")) {
        plan.push({ sourceKey: selection.key, destinationKey: relativePathFromPrefix(sourcePrefix, selection.key) });
      }
      continue;
    }
    const prefix = selection.key.endsWith("/") ? selection.key : `${selection.key}/`;
    const objects = source.provider === "aws"
      ? await listAllS3Objects(aws, source.name, prefix)
      : await listAllBunnyFiles(source, prefix);
    for (const object of objects) {
      if (!object.key.endsWith("/")) {
        plan.push({ sourceKey: object.key, destinationKey: relativePathFromPrefix(sourcePrefix, object.key) });
      }
    }
  }
  return dedupePlan(plan);
}

async function detectTransferConflicts(
  aws: AwsCredentials,
  destination: ResolvedStorageResource,
  plan: TransferPlanEntry[],
  destinationPrefix: string,
): Promise<TransferConflictPreview> {
  const conflicts: string[] = [];
  for (const entry of plan) {
    const destinationKey = joinPrefix(destinationPrefix, entry.destinationKey);
    if (await storageObjectExists(aws, destination, destinationKey)) {
      conflicts.push(destinationKey);
    }
  }
  return {
    conflictCount: conflicts.length,
    conflicts,
  };
}

function relativePathFromPrefix(prefix: string, key: string): string {
  const trimmedPrefix = prefix.trim();
  const cleanPrefix = trimmedPrefix ? (trimmedPrefix.endsWith("/") ? trimmedPrefix : `${trimmedPrefix}/`) : "";
  const cleanKey = key.replace(/^\/+/, "");
  if (!cleanPrefix) return cleanKey;
  return cleanKey.startsWith(cleanPrefix) ? cleanKey.slice(cleanPrefix.length) : cleanKey;
}

function dedupePlan(plan: TransferPlanEntry[]): TransferPlanEntry[] {
  const seen = new Set<string>();
  const result: TransferPlanEntry[] = [];
  for (const entry of plan) {
    if (seen.has(entry.sourceKey)) continue;
    seen.add(entry.sourceKey);
    result.push(entry);
  }
  return result;
}

async function executeTransfer(
  aws: AwsCredentials,
  source: ResolvedStorageResource,
  destination: ResolvedStorageResource,
  plan: TransferPlanEntry[],
  destinationPrefix: string,
  conflictMode: ConflictMode,
): Promise<{ copied: number; failed: number; errors: string[] }> {
  let copied = 0;
  let failed = 0;
  const errors: string[] = [];
  for (const entry of plan) {
    try {
      const outcome = await copyStorageObject(aws, source, destination, entry.sourceKey, destinationPrefix, entry.destinationKey, conflictMode);
      if (outcome === "copied") {
        copied += 1;
      }
    } catch (error) {
      failed += 1;
      errors.push(`${entry.sourceKey}: ${(error as Error).message}`);
    }
  }
  return { copied, failed, errors };
}

async function copyStorageObject(
  aws: AwsCredentials,
  source: ResolvedStorageResource,
  destination: ResolvedStorageResource,
  sourceKey: string,
  destinationPrefix: string,
  destinationKey: string,
  conflictMode: ConflictMode,
): Promise<"copied" | "skipped"> {
  const finalKey = joinPrefix(destinationPrefix, destinationKey);
  if (conflictMode === "new" && await storageObjectExists(aws, destination, finalKey)) {
    return "skipped";
  }
  const sourceResponse = await readStorageObject(aws, source, sourceKey);
  if (!sourceResponse.ok || !sourceResponse.body) {
    throw new Error(await sourceResponse.text());
  }
  await writeStorageObject(aws, destination, finalKey, sourceResponse.body, sourceResponse.headers.get("content-type") || "application/octet-stream");
  return "copied";
}

async function readStorageObject(aws: AwsCredentials, resource: ResolvedStorageResource, key: string): Promise<Response> {
  if (resource.provider === "aws") {
    return getAwsObject(aws, resource.name, resource.region, key);
  }
  return getBunnyObject(resource, key);
}

async function storageObjectExists(aws: AwsCredentials, resource: ResolvedStorageResource, key: string): Promise<boolean> {
  if (resource.provider === "aws") {
    return awsObjectExists(aws, resource.name, resource.region, key);
  }
  return bunnyObjectExists(resource, key);
}

async function writeStorageObject(
  aws: AwsCredentials,
  resource: ResolvedStorageResource,
  key: string,
  body: BodyInit | null,
  contentType: string,
): Promise<void> {
  if (resource.provider === "aws") {
    await putAwsObject(aws, resource.name, resource.region, key, body, contentType);
    return;
  }
  await putBunnyObject(resource, key, body, contentType);
}

async function getAwsObject(aws: AwsCredentials, bucket: string, region: string, key: string): Promise<Response> {
  return signedS3RequestToHost(
    aws,
    "GET",
    `s3.${region}.amazonaws.com`,
    region,
    `/${bucket}/${key}`,
    new URLSearchParams(),
    undefined,
  );
}

async function awsObjectExists(aws: AwsCredentials, bucket: string, region: string, key: string): Promise<boolean> {
  const response = await signedS3RequestToHost(
    aws,
    "HEAD",
    `s3.${region}.amazonaws.com`,
    region,
    `/${bucket}/${key}`,
    new URLSearchParams(),
    undefined,
  );
  return response.ok;
}

async function putAwsObject(
  aws: AwsCredentials,
  bucket: string,
  region: string,
  key: string,
  body: BodyInit | null,
  contentType: string,
): Promise<Response> {
  const query = new URLSearchParams();
  const response = await signedS3RequestToHost(
    aws,
    "PUT",
    `s3.${region}.amazonaws.com`,
    region,
    `/${bucket}/${key}`,
    query,
    body,
    "UNSIGNED-PAYLOAD",
  );
  if (!response.ok && response.status !== 200) {
    throw new Error(await response.text());
  }
  return response;
}

async function getBunnyObject(resource: ResolvedStorageResource, key: string): Promise<Response> {
  const endpoint = bunnyEndpointForRegion(resource.region);
  const response = await fetch(`https://${endpoint}${bunnyObjectPath(resource.name, key)}`, {
    headers: { AccessKey: resource.password || "" },
  });
  return response;
}

async function bunnyObjectExists(resource: ResolvedStorageResource, key: string): Promise<boolean> {
  const endpoint = bunnyEndpointForRegion(resource.region);
  const response = await fetch(`https://${endpoint}${bunnyObjectPath(resource.name, key)}`, {
    method: "HEAD",
    headers: { AccessKey: resource.password || "" },
  });
  return response.ok;
}

async function putBunnyObject(
  resource: ResolvedStorageResource,
  key: string,
  body: BodyInit | null,
  contentType: string,
): Promise<void> {
  const endpoint = bunnyEndpointForRegion(resource.region);
  const upload = await fetch(`https://${endpoint}${bunnyObjectPath(resource.name, key)}`, {
    method: "PUT",
    headers: {
      AccessKey: resource.password || "",
      "Content-Type": contentType,
    },
    body,
  });
  if (!upload.ok && upload.status !== 201) {
    throw new Error(await upload.text());
  }
}

async function listAllBunnyFiles(resource: ResolvedStorageResource, prefix: string): Promise<S3Item[]> {
  const items: S3Item[] = [];
  const seen = new Set<string>();
  const walk = async (path: string): Promise<void> => {
    const listed = await listBunnyObjects({ id: 0, name: resource.name, region: resource.region, password: resource.password }, resource.region, path);
    for (const item of listed) {
      const itemPath = item.path || joinPrefix(path, item.name);
      if (item.type === "file") {
        if (!seen.has(itemPath)) {
          seen.add(itemPath);
          items.push({
            key: itemPath,
            size: item.size,
            type: "file",
            lastModified: item.lastChanged,
          });
        }
        continue;
      }
      if (item.type === "folder") {
        await walk(itemPath);
      }
    }
  };
  await walk(prefix);
  return items;
}

function bunnyObjectPath(zoneName: string, key: string): string {
  const encodedZone = encodeURIComponent(zoneName);
  const cleanKey = key
    .split("/")
    .map((segment) => segment.trim())
    .filter(Boolean)
    .map((segment) => encodeURIComponent(segment))
    .join("/");
  return cleanKey ? `/${encodedZone}/${cleanKey}` : `/${encodedZone}`;
}

async function resolveStorageResource(apiKey: string, resource: StorageResourceRef): Promise<ResolvedStorageResource> {
  if (resource.provider !== "bunny") {
    return resource;
  }
  const zone = await resolveBunnyZone(apiKey, resource.name);
  return {
    provider: resource.provider,
    name: zone.name,
    region: zone.region || resource.region,
    password: zone.password,
  };
}

async function parseJson(request: Request): Promise<unknown> {
  const contentType = request.headers.get("content-type") || "";
  if (!contentType.includes("application/json")) throw new Error("Expected application/json body.");
  return request.json();
}

function splitDestinationKey(key: string): { path: string; fileName: string } {
  const clean = key.replace(/^\/+/, "");
  const index = clean.lastIndexOf("/");
  if (index === -1) return { path: "", fileName: clean };
  return { path: clean.slice(0, index), fileName: clean.slice(index + 1) };
}

function joinPrefix(prefix: string, child: string): string {
  const left = prefix ? prefix.replace(/\/+$/, "") : "";
  const right = child ? child.replace(/^\/+/, "") : "";
  if (!left) return right;
  if (!right) return left + "/";
  return left + "/" + right;
}

function encodeUrlPath(path: string): string {
  if (!path || path === "/") return "/";
  return path
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

function encodeBunnyPath(path: string): string {
  return path
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

export class TransferManager extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS jobs (
          id TEXT PRIMARY KEY,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          status TEXT NOT NULL,
          source_bucket TEXT NOT NULL,
          source_region TEXT NOT NULL,
          source_prefix TEXT NOT NULL DEFAULT '',
          aws_access_key_id TEXT NOT NULL,
          aws_secret_access_key TEXT NOT NULL,
          aws_session_token TEXT,
          destination_zone TEXT NOT NULL,
          destination_zone_region TEXT NOT NULL,
          destination_zone_password TEXT NOT NULL,
          destination_prefix TEXT NOT NULL,
          selections_json TEXT NOT NULL,
          current_selection_index INTEGER NOT NULL DEFAULT 0,
          current_folder_prefix TEXT,
          current_folder_page_json TEXT,
          current_folder_continuation_token TEXT,
          copied INTEGER NOT NULL DEFAULT 0,
          failed INTEGER NOT NULL DEFAULT 0,
          processed INTEGER NOT NULL DEFAULT 0,
          last_key TEXT,
          last_error TEXT,
          message TEXT
        )
      `);
      const columns = this.ctx.storage.sql.exec<{ name: string }>(`PRAGMA table_info(jobs)`).toArray();
      const addColumn = (name: string, definition: string) => {
        if (!columns.some((column) => column.name === name)) {
          this.ctx.storage.sql.exec(`ALTER TABLE jobs ADD COLUMN ${definition}`);
        }
      };
      addColumn("source_provider", "source_provider TEXT NOT NULL DEFAULT 'aws'");
      addColumn("source_bucket", "source_bucket TEXT NOT NULL DEFAULT ''");
      addColumn("source_region", "source_region TEXT NOT NULL DEFAULT 'us-east-1'");
      addColumn("source_prefix", "source_prefix TEXT NOT NULL DEFAULT ''");
      addColumn("source_resource_password", "source_resource_password TEXT");
      addColumn("aws_access_key_id", "aws_access_key_id TEXT NOT NULL DEFAULT ''");
      addColumn("aws_secret_access_key", "aws_secret_access_key TEXT NOT NULL DEFAULT ''");
      addColumn("aws_session_token", "aws_session_token TEXT");
      addColumn("destination_provider", "destination_provider TEXT NOT NULL DEFAULT 'bunny'");
      addColumn("destination_zone", "destination_zone TEXT NOT NULL DEFAULT ''");
      addColumn("destination_zone_region", "destination_zone_region TEXT NOT NULL DEFAULT ''");
      addColumn("destination_zone_password", "destination_zone_password TEXT NOT NULL DEFAULT ''");
      addColumn("destination_prefix", "destination_prefix TEXT NOT NULL DEFAULT ''");
      addColumn("selections_json", "selections_json TEXT NOT NULL DEFAULT '[]'");
      addColumn("current_selection_index", "current_selection_index INTEGER NOT NULL DEFAULT 0");
      addColumn("current_folder_prefix", "current_folder_prefix TEXT");
      addColumn("current_folder_page_json", "current_folder_page_json TEXT");
      addColumn("current_folder_continuation_token", "current_folder_continuation_token TEXT");
      addColumn("copied", "copied INTEGER NOT NULL DEFAULT 0");
      addColumn("skipped", "skipped INTEGER NOT NULL DEFAULT 0");
      addColumn("failed", "failed INTEGER NOT NULL DEFAULT 0");
      addColumn("processed", "processed INTEGER NOT NULL DEFAULT 0");
      addColumn("last_key", "last_key TEXT");
      addColumn("last_error", "last_error TEXT");
      addColumn("message", "message TEXT");
      addColumn("conflict_mode", "conflict_mode TEXT NOT NULL DEFAULT 'replace'");
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS job_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_id TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          level TEXT NOT NULL,
          message TEXT NOT NULL
        )
      `);
      this.ctx.storage.sql.exec(`CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at)`);
      this.ctx.storage.sql.exec(`CREATE INDEX IF NOT EXISTS idx_job_events_job_created ON job_events(job_id, created_at DESC)`);
    });
  }

  async createJob(input: TransferJobCreateRequest): Promise<TransferJobDetail> {
    const id = crypto.randomUUID();
    const now = Date.now();
    this.ctx.storage.sql.exec(
      `
        INSERT INTO jobs (
          id, created_at, updated_at, status,
          source_provider, source_bucket, source_region,
          source_prefix, source_resource_password,
          aws_access_key_id, aws_secret_access_key, aws_session_token,
          destination_provider, destination_zone, destination_zone_region, destination_zone_password,
          destination_prefix, selections_json,
          current_selection_index, current_folder_prefix, current_folder_page_json, current_folder_continuation_token,
          copied, skipped, failed, processed, last_key, last_error, message, conflict_mode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      id,
      now,
      now,
      "queued",
      input.source.provider,
      input.source.name,
      input.source.region,
      input.sourcePrefix || "",
      input.source.password || null,
      input.aws.accessKeyId,
      input.aws.secretAccessKey,
      input.aws.sessionToken || null,
      input.destination.provider,
      input.destination.name,
      input.destination.region,
      input.destination.password || "",
      input.destinationPrefix || "",
      JSON.stringify(input.selections),
      0,
      null,
      null,
      null,
      0,
      0,
      0,
      0,
      null,
      null,
      "Queued for transfer",
      input.conflictMode || "replace",
    );
    this.logEvent(id, "info", "Job queued");
    await this.schedule();
    const job = await this.getJob(id);
    if (!job) {
      throw new Error("Could not create job.");
    }
    return job;
  }

  async listJobs(): Promise<TransferJobSummary[]> {
    return this.ctx.storage.sql
      .exec<TransferJobRow>(`SELECT * FROM jobs ORDER BY created_at DESC LIMIT 50`)
      .toArray()
      .map((row) => this.rowToSummary(row));
  }

  async clearFailedJobs(): Promise<void> {
    this.ctx.storage.sql.exec(`DELETE FROM job_events WHERE job_id IN (SELECT id FROM jobs WHERE status = 'failed')`);
    this.ctx.storage.sql.exec(`DELETE FROM jobs WHERE status = 'failed'`);
  }

  async getJob(jobId: string): Promise<TransferJobDetail | null> {
    const cursor = this.ctx.storage.sql.exec<TransferJobRow>(`SELECT * FROM jobs WHERE id = ? LIMIT 1`, jobId);
    const row = cursor.toArray()[0];
    if (!row) {
      return null;
    }
    return this.rowToDetail(row);
  }

  async alarm(): Promise<void> {
    await this.processPendingJobs();
  }

  private async processPendingJobs(): Promise<void> {
    const started = Date.now();
    while (Date.now() - started < 20_000) {
      const job = this.nextRunnableJob();
      if (!job) {
        await this.ctx.storage.deleteAlarm();
        return;
      }
      const completed = await this.processJob(job);
      if (!completed) {
        await this.schedule();
        return;
      }
    }
    await this.schedule();
  }

  private nextRunnableJob(): TransferJobRow | null {
    const cursor = this.ctx.storage.sql.exec<TransferJobRow>(
      `SELECT * FROM jobs WHERE status IN ('queued', 'running') ORDER BY created_at ASC LIMIT 1`,
    );
    return cursor.toArray()[0] ?? null;
  }

  private async processJob(job: TransferJobRow): Promise<boolean> {
    if (job.status === "queued") {
      this.updateJob(job.id, { status: "running", message: "Running" });
      this.logEvent(job.id, "info", "Job started");
      job.status = "running";
    }

    const batchLimit = 25;
    for (let processedInBatch = 0; processedInBatch < batchLimit; processedInBatch += 1) {
      const refreshed = this.getJobRow(job.id);
      if (!refreshed || refreshed.status !== "running") {
        return true;
      }
      const outcome = await this.processStep(refreshed);
      if (outcome === "done") {
        this.logEvent(job.id, "info", "Job completed");
        this.updateJob(job.id, { status: "completed", message: "Completed" });
        return true;
      }
      if (outcome === "failed") {
        this.updateJob(job.id, { status: "failed", message: "Failed" });
        return true;
      }
      if (outcome === "idle") {
        return false;
      }
    }
    return false;
  }

  private async processStep(job: TransferJobRow): Promise<"continue" | "done" | "idle" | "failed"> {
    const selections = JSON.parse(job.selections_json) as TransferJobSelection[];
    if (job.current_selection_index >= selections.length) {
      return "done";
    }

    const selection = selections[job.current_selection_index];
    if (!selection) {
      return "done";
    }

    const source = this.rowToResource(job, "source");
    const destination = this.rowToResource(job, "destination");
    const aws = {
      accessKeyId: job.aws_access_key_id,
      secretAccessKey: job.aws_secret_access_key,
      sessionToken: job.aws_session_token || undefined,
      region: job.source_region,
    };
    const conflictMode = job.conflict_mode === "new" ? "new" : "replace";

    if (selection.kind === "file") {
      try {
        const outcome = await copyStorageObject(
          aws,
          source,
          destination,
          selection.key,
          job.destination_prefix,
          relativePathFromPrefix(job.source_prefix, selection.key),
          conflictMode,
        );
        const patch: Partial<TransferJobRow> = {
          processed: job.processed + 1,
          current_selection_index: job.current_selection_index + 1,
          last_key: selection.key,
          last_error: null,
          current_folder_prefix: null,
          current_folder_page_json: null,
          current_folder_continuation_token: null,
        };
        if (outcome === "skipped") {
          patch.skipped = job.skipped + 1;
          patch.message = `Skipped ${selection.key}`;
        } else {
          patch.copied = job.copied + 1;
          patch.message = `Copied ${job.copied + 1} file(s)`;
        }
        this.updateJob(job.id, patch);
      } catch (error) {
        this.handleStepFailure(job, selection.key, error);
        this.updateJob(job.id, {
          failed: job.failed + 1,
          processed: job.processed + 1,
          current_selection_index: job.current_selection_index + 1,
          last_key: selection.key,
          last_error: (error as Error).message,
          message: `Failed on ${selection.key}`,
          current_folder_prefix: null,
          current_folder_page_json: null,
          current_folder_continuation_token: null,
        });
      }
      return "continue";
    }

    const prefix = selection.key.endsWith("/") ? selection.key : `${selection.key}/`;
    let page = job.current_folder_page_json ? (JSON.parse(job.current_folder_page_json) as TransferJobPageState) : null;
    if (!page) {
      try {
        const listing = source.provider === "aws"
          ? await listAwsObjects(
              aws,
              source.name,
              prefix,
              job.current_folder_continuation_token || undefined,
              { maxKeys: 100 },
            )
          : {
              items: await listAllBunnyFiles(source, prefix),
              nextContinuationToken: null,
            };
        page = {
          keys: listing.items
            .filter((item) => item.type === "file")
            .map((item) => item.key),
          index: 0,
          continuationToken: listing.nextContinuationToken || null,
        };
        this.updateJob(job.id, {
          current_folder_prefix: prefix,
          current_folder_page_json: JSON.stringify(page),
          current_folder_continuation_token: page.continuationToken || null,
          message: `Scanning ${prefix}`,
        });
        if (!page.keys.length) {
          if (page.continuationToken) {
            this.updateJob(job.id, {
              current_folder_page_json: null,
              current_folder_continuation_token: page.continuationToken,
            });
            return "continue";
          }
          this.updateJob(job.id, {
            current_selection_index: job.current_selection_index + 1,
            current_folder_prefix: null,
            current_folder_page_json: null,
            current_folder_continuation_token: null,
            message: `Finished folder ${selection.key}`,
          });
          return "continue";
        }
      } catch (error) {
        this.handleStepFailure(job, prefix, error);
        this.updateJob(job.id, {
          failed: job.failed + 1,
          processed: job.processed + 1,
          current_selection_index: job.current_selection_index + 1,
          last_key: prefix,
          last_error: (error as Error).message,
          message: `Failed while listing ${selection.key}`,
          current_folder_prefix: null,
          current_folder_page_json: null,
          current_folder_continuation_token: null,
        });
        return "continue";
      }
    }

    const key = page.keys[page.index];
    if (!key) {
      if (page.continuationToken) {
        this.updateJob(job.id, {
          current_folder_page_json: null,
          current_folder_continuation_token: page.continuationToken,
          message: `Continuing ${selection.key}`,
        });
        return "continue";
      }
      this.updateJob(job.id, {
        current_selection_index: job.current_selection_index + 1,
        current_folder_prefix: null,
        current_folder_page_json: null,
        current_folder_continuation_token: null,
        message: `Finished folder ${selection.key}`,
      });
      return "continue";
    }

    try {
      const outcome = await copyStorageObject(
        aws,
        source,
        destination,
        key,
        job.destination_prefix,
        relativePathFromPrefix(job.source_prefix, key),
        conflictMode,
      );
      page.index += 1;
      const isPageDone = page.index >= page.keys.length;
      const patch: Partial<TransferJobRow> = {
        processed: job.processed + 1,
        last_key: key,
        last_error: null,
        current_folder_page_json: isPageDone ? null : JSON.stringify(page),
        current_folder_continuation_token: isPageDone ? page.continuationToken || null : job.current_folder_continuation_token,
        current_selection_index: isPageDone && !page.continuationToken ? job.current_selection_index + 1 : job.current_selection_index,
      };
      if (outcome === "skipped") {
        patch.skipped = job.skipped + 1;
        patch.message = `Skipped ${key}`;
      } else {
        patch.copied = job.copied + 1;
        patch.message = `Copied ${key}`;
      }
      this.updateJob(job.id, patch);
      if (isPageDone && !page.continuationToken) {
        this.updateJob(job.id, {
          current_folder_prefix: null,
          current_folder_page_json: null,
          current_folder_continuation_token: null,
          current_selection_index: job.current_selection_index + 1,
        });
      }
    } catch (error) {
      this.handleStepFailure(job, key, error);
      page.index += 1;
      const isPageDone = page.index >= page.keys.length;
      this.updateJob(job.id, {
        failed: job.failed + 1,
        processed: job.processed + 1,
        last_key: key,
        last_error: (error as Error).message,
        message: `Failed on ${key}`,
        current_folder_page_json: isPageDone ? null : JSON.stringify(page),
        current_folder_continuation_token: isPageDone ? page.continuationToken || null : job.current_folder_continuation_token,
        current_selection_index: isPageDone && !page.continuationToken ? job.current_selection_index + 1 : job.current_selection_index,
      });
      if (isPageDone && !page.continuationToken) {
        this.updateJob(job.id, {
          current_folder_prefix: null,
          current_folder_page_json: null,
          current_folder_continuation_token: null,
          current_selection_index: job.current_selection_index + 1,
        });
      }
    }

    return "continue";
  }

  private rowToResource(row: TransferJobRow, side: "source" | "destination"): ResolvedStorageResource {
    if (side === "source") {
      return {
        provider: row.source_provider,
        name: row.source_bucket,
        region: row.source_region,
        password: row.source_resource_password || undefined,
      };
    }
    return {
      provider: row.destination_provider,
      name: row.destination_zone,
      region: row.destination_zone_region,
      password: row.destination_zone_password || undefined,
    };
  }

  private handleStepFailure(job: TransferJobRow, key: string, error: unknown): void {
    this.logEvent(job.id, "error", `${key}: ${(error as Error).message}`);
  }

  private updateJob(jobId: string, patch: Partial<TransferJobRow>): void {
    const fields = Object.entries(patch);
    if (!fields.length) return;
    const assignments = fields.map(([key]) => `${key} = ?`).join(", ");
    const values = fields.map(([, value]) => value);
    this.ctx.storage.sql.exec(
      `UPDATE jobs SET ${assignments}, updated_at = ? WHERE id = ?`,
      ...values,
      Date.now(),
      jobId,
    );
  }

  private getJobRow(jobId: string): TransferJobRow | null {
    const cursor = this.ctx.storage.sql.exec<TransferJobRow>(`SELECT * FROM jobs WHERE id = ? LIMIT 1`, jobId);
    return cursor.toArray()[0] ?? null;
  }

  private rowToSummary(row: TransferJobRow): TransferJobSummary {
    return {
      id: row.id,
      status: row.status,
      sourceProvider: row.source_provider,
      sourceResource: row.source_bucket,
      destinationProvider: row.destination_provider,
      destinationResource: row.destination_zone,
      destinationPrefix: row.destination_prefix,
      sourcePrefix: row.source_prefix,
      selections: JSON.parse(row.selections_json).length,
      copied: row.copied,
      skipped: row.skipped,
      failed: row.failed,
      processed: row.processed,
      lastKey: row.last_key || undefined,
      lastError: row.last_error || undefined,
      message: row.message || undefined,
      createdAt: new Date(row.created_at).toISOString(),
      updatedAt: new Date(row.updated_at).toISOString(),
    };
  }

  private rowToDetail(row: TransferJobRow): TransferJobDetail {
    return {
      ...this.rowToSummary(row),
      currentSelectionIndex: row.current_selection_index,
      currentFolderPrefix: row.current_folder_prefix || undefined,
      currentFolderContinuationToken: row.current_folder_continuation_token || null,
    };
  }

  private logEvent(jobId: string, level: string, message: string): void {
    this.ctx.storage.sql.exec(
      `INSERT INTO job_events (job_id, created_at, level, message) VALUES (?, ?, ?, ?)`,
      jobId,
      Date.now(),
      level,
      message,
    );
  }

  private async schedule(): Promise<void> {
    await this.ctx.storage.setAlarm(Date.now() + 1_000);
  }
}



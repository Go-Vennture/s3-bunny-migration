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

type S3Item = {
  key: string;
  size: number;
  lastModified?: string;
  type: "file" | "folder";
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
  sourceBucket: string;
  sourcePrefix: string;
  selections: TransferSelection[];
  bunnyApiKey: string;
  destinationZone: string;
  destinationPrefix?: string;
};

type TransferPlanEntry = {
  sourceKey: string;
  destinationKey: string;
};

type ResolvedBunnyZone = BunnyZone & {
  password: string;
};

type TransferJobSelection = TransferSelection;

type TransferJobCreateRequest = {
  aws: AwsCredentials;
  sourceBucket: string;
  sourcePrefix: string;
  selections: TransferJobSelection[];
  destinationPrefix?: string;
  bunnyZone: ResolvedBunnyZone;
};

type TransferJobSummary = {
  id: string;
  status: string;
  sourceBucket: string;
  destinationZone: string;
  destinationPrefix: string;
  selections: number;
  copied: number;
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
  source_bucket: string;
  source_region: string;
  source_prefix: string;
  aws_access_key_id: string;
  aws_secret_access_key: string;
  aws_session_token: string | null;
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
  failed: number;
  processed: number;
  last_key: string | null;
  last_error: string | null;
  message: string | null;
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
    if (request.method === "GET" && url.pathname === "/") return new Response(renderHtml(), { headers: { "content-type": "text/html; charset=utf-8" } });
    if (request.method === "GET" && url.pathname === "/favicon.ico") return faviconResponse();
    if (request.method === "GET" && url.pathname === "/health") return json({ ok: true });
    if (request.method === "POST" && url.pathname === "/api/aws/buckets") return json({ buckets: await listAwsBuckets(await parseAwsCredentials(request)) });
    if (request.method === "POST" && url.pathname === "/api/aws/list") return handleAwsList(request);
    if (request.method === "POST" && url.pathname === "/api/bunny/zones") return handleBunnyZones(request);
    if (request.method === "POST" && url.pathname === "/api/bunny/list") return handleBunnyList(request);
    if (request.method === "POST" && url.pathname === "/api/transfer") return handleTransfer(request, env);
    if (request.method === "GET" && url.pathname === "/api/jobs") return handleJobs(request, env);
    return new Response("Not found", { status: 404 });
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
    .layout{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:18px}
    .contents-layout{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:18px;align-items:stretch;margin-top:18px}
    .panel{background:var(--panel);border:1px solid var(--border);border-radius:var(--radius);box-shadow:var(--shadow);backdrop-filter:blur(16px);overflow:hidden}
    .panel-head{padding:18px 18px 14px;border-bottom:1px solid rgba(47,45,41,.08)}
    .panel-head h2{margin:0 0 6px;font-size:20px}
    .panel-head p{margin:0;font-size:13px;line-height:1.45;color:var(--muted)}
    .panel-body{padding:16px 18px 18px;display:grid;gap:12px}
    .grid-2{display:grid;grid-template-columns:1fr 1fr;gap:10px}
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
        <span class="pill">Bunny account API key is used to discover zones and resolve the storage password</span>
      </div>
    </header>

    <section class="layout">
      <article class="panel">
        <div class="panel-head">
          <h2>AWS source</h2>
          <p>Use one AWS key pair to list all buckets, then browse folders or mark entire trees for copy.</p>
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
          <h2>Bunny destination</h2>
          <p>List storage zones with the Bunny account API key, then browse the selected zone like a folder tree.</p>
        </div>
        <div class="panel-body">
          <div class="field"><label for="bunnyApiKey">Bunny account API key</label><input id="bunnyApiKey" type="password" autocomplete="off" spellcheck="false" /></div>
        </div>
      </article>
    </section>

    <section class="contents-layout">
      <article class="contents-card">
        <h3>AWS contents</h3>
        <div class="inline-note" id="awsStatus">Load a bucket to see folders and files.</div>
        <div class="contents-controls">
          <div class="contents-select-row">
            <div class="field" style="margin:0">
              <label for="awsBucketSelect">Bucket</label>
              <select id="awsBucketSelect"><option value="">Load buckets first</option></select>
            </div>
            <button class="secondary" id="loadAwsBuckets">Refresh buckets</button>
          </div>
          <div class="contents-path-row">
            <button class="ghost mini" id="awsUp" title="Parent folder">..</button>
            <div class="field" style="margin:0"><label for="awsPrefix">Path</label><input id="awsPrefix" autocomplete="off" spellcheck="false" placeholder="optional/prefix/" value="" /></div>
            <button class="secondary" id="loadAwsPath">Refresh</button>
          </div>
        </div>
        <div class="list contents-list" id="awsList" aria-live="polite"></div>
      </article>
      <article class="contents-card">
        <h3>Bunny contents</h3>
        <div class="inline-note" id="bunnyStatus">Load a zone to browse its contents.</div>
        <div class="contents-controls">
          <div class="contents-select-row">
            <div class="field" style="margin:0">
              <label for="bunnyZoneSelect">Storage zone</label>
              <select id="bunnyZoneSelect"><option value="">Load zones first</option></select>
            </div>
            <button class="secondary" id="loadBunnyZones">Refresh zones</button>
          </div>
          <div class="contents-path-row">
            <button class="ghost mini" id="bunnyUp" title="Parent folder">..</button>
            <div class="field" style="margin:0"><label for="bunnyPrefix">Path</label><input id="bunnyPrefix" autocomplete="off" spellcheck="false" placeholder="destination/prefix/" value="" /></div>
            <button class="secondary" id="loadBunnyPath">Refresh</button>
          </div>
        </div>
        <div class="list contents-list" id="bunnyList" aria-live="polite"></div>
      </article>
    </section>

    <section class="footer">
      <div class="transfer-card">
        <h3>Transfer selected items</h3>
        <div class="summary">
          <span class="selection-pill" id="selectionCount">0 selected</span>
          <span class="pill" id="destinationSummary">Destination root</span>
        </div>
          <div class="grid-2">
            <div class="field"><label>Path behavior</label><input value="Preserve relative paths from the selected source and destination points" disabled /></div>
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
        awsBucketSelect: $("awsBucketSelect"),
        awsPrefix: $("awsPrefix"),
        awsList: $("awsList"),
        awsStatus: $("awsStatus"),
        awsUp: $("awsUp"),
        loadAwsBuckets: $("loadAwsBuckets"),
        loadAwsPath: $("loadAwsPath"),
        bunnyApiKey: $("bunnyApiKey"),
        bunnyZoneSelect: $("bunnyZoneSelect"),
        bunnyPrefix: $("bunnyPrefix"),
        bunnyList: $("bunnyList"),
        bunnyStatus: $("bunnyStatus"),
        bunnyUp: $("bunnyUp"),
        loadBunnyZones: $("loadBunnyZones"),
        loadBunnyPath: $("loadBunnyPath"),
        selectionCount: $("selectionCount"),
        destinationSummary: $("destinationSummary"),
        transferButton: $("transferButton"),
        clearSelection: $("clearSelection"),
        log: $("log"),
        jobsList: $("jobsList"),
        jobsStatus: $("jobsStatus"),
      };

      const STORAGE_KEY = "s3-bunny-migration:ui-state:v2";
      const STORAGE_TTL_MS = 24 * 60 * 60 * 1000;

      const state = {
        awsItems: [],
        awsBuckets: [],
        awsBucketRegions: new Map(),
        bunnyItems: [],
        awsSelections: new Map(),
        awsContinuationToken: null,
        bunnyZones: [],
        jobs: [],
      };

      function escapeHtml(value) {
        return String(value)
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/"/g, "&quot;");
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

      function selectedBucket() {
        return els.awsBucketSelect.value.trim();
      }

      function selectedAwsRegion() {
        return state.awsBucketRegions.get(selectedBucket()) || "us-east-1";
      }

      function selectedZone() {
        return els.bunnyZoneSelect.value.trim();
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
        try {
          window.localStorage.setItem(STORAGE_KEY, JSON.stringify({
            expiresAt: Date.now() + STORAGE_TTL_MS,
            awsAccessKeyId: els.awsAccessKeyId.value,
            awsSecretAccessKey: els.awsSecretAccessKey.value,
            awsBucketSelect: els.awsBucketSelect.value,
            awsPrefix: els.awsPrefix.value,
            bunnyApiKey: els.bunnyApiKey.value,
            bunnyZoneSelect: els.bunnyZoneSelect.value,
            bunnyPrefix: els.bunnyPrefix.value,
            awsSelections: Array.from(state.awsSelections.keys()),
          }));
        } catch {
          // Local storage can be unavailable in some browser privacy modes.
        }
      }

      function restoreUiState() {
        const saved = readUiState();
        els.awsAccessKeyId.value = saved.awsAccessKeyId || "";
        els.awsSecretAccessKey.value = saved.awsSecretAccessKey || "";
        els.awsBucketSelect.value = saved.awsBucketSelect || "";
        els.awsPrefix.value = saved.awsPrefix || "";
        els.bunnyApiKey.value = saved.bunnyApiKey || "";
        els.bunnyZoneSelect.value = saved.bunnyZoneSelect || "";
        els.bunnyPrefix.value = saved.bunnyPrefix || "";
        const selections = Array.isArray(saved.awsSelections) ? saved.awsSelections : [];
        state.awsSelections = new Map(selections.map((key) => [String(key), true]));
      }

      function setStatus(target, message, kind = "info") {
        target.textContent = message;
        target.classList.toggle("error", kind === "error");
      }

      function syncSummary() {
        els.selectionCount.textContent = String(state.awsSelections.size) + " selected";
        const zone = selectedZone();
        const destinationPrefix = ensureTrailingSlash(els.bunnyPrefix.value);
        els.destinationSummary.textContent = zone
          ? "Destination: " + zone + "/" + (destinationPrefix ? destinationPrefix : "")
          : (destinationPrefix ? "Destination: " + destinationPrefix : "Destination root");
        writeUiState();
      }

      function renderAwsList() {
        const header = '<div class="list-head"><div></div><div>Name</div><div class="size-col">Size</div><div>Modified</div></div>';
        if (!state.awsItems.length) {
          els.awsList.innerHTML = header + '<div class="list-row"><div></div><div class="meta">No objects loaded yet.</div><div></div><div></div></div>';
          return;
        }
        const rows = state.awsItems.map((item) => {
          const checked = state.awsSelections.has(item.key) ? "checked" : "";
          const icon = item.type === "folder" ? "▸" : "⬚";
          const label = item.type === "folder" ? item.key.replace(/\/+$/, "").split("/").pop() + "/" : item.key.split("/").pop();
          return '<div class="list-row">' +
            '<div><input class="check" type="checkbox" data-key="' + escapeHtml(item.key) + '" ' + checked + ' /></div>' +
            '<div class="name-cell"><div class="icon">' + icon + '</div><button class="link" data-open-source="' + escapeHtml(item.key) + '">' + escapeHtml(label) + '</button></div>' +
            '<div class="size-col meta">' + (item.type === "folder" ? "Folder" : formatBytes(item.size)) + '</div>' +
            '<div class="meta">' + (item.lastModified || "") + '</div>' +
            '</div>';
        }).join("");
        const more = state.awsContinuationToken ? '<div class="list-row"><div></div><div><button class="secondary" id="loadMoreAws">Load more</button></div><div></div><div></div></div>' : "";
        els.awsList.innerHTML = header + rows + more;
        els.awsList.querySelectorAll('input[type="checkbox"][data-key]').forEach((checkbox) => {
          checkbox.addEventListener("change", () => {
            if (checkbox.checked) state.awsSelections.set(checkbox.dataset.key, true);
            else state.awsSelections.delete(checkbox.dataset.key);
            syncSummary();
          });
        });
        els.awsList.querySelectorAll("[data-open-source]").forEach((button) => {
          button.addEventListener("click", () => {
            const key = button.dataset.openSource || "";
            if (key.endsWith("/")) {
              els.awsPrefix.value = key;
              loadAwsPath(false);
            }
          });
        });
        const moreButton = $("loadMoreAws");
        if (moreButton) moreButton.addEventListener("click", () => loadAwsPath(true));
      }

      function renderBunnyList() {
        const header = '<div class="list-head"><div></div><div>Name</div><div class="size-col">Size</div><div>Modified</div></div>';
        if (!state.bunnyItems.length) {
          els.bunnyList.innerHTML = header + '<div class="list-row"><div></div><div class="meta">No objects loaded yet.</div><div></div><div></div></div>';
          return;
        }
        els.bunnyList.innerHTML = header + state.bunnyItems.map((item) => {
          const icon = item.type === "folder" ? "▸" : "⬚";
          return '<div class="list-row">' +
            '<div></div>' +
            '<div class="name-cell"><div class="icon">' + icon + '</div><button class="link" data-open-bunny="' + escapeHtml(item.path) + '">' + escapeHtml(item.name) + '</button></div>' +
            '<div class="size-col meta">' + (item.type === "folder" ? "Folder" : formatBytes(item.size)) + '</div>' +
            '<div class="meta">' + (item.lastChanged || "") + '</div>' +
            '</div>';
        }).join("");
        els.bunnyList.querySelectorAll("[data-open-bunny]").forEach((button) => {
          button.addEventListener("click", () => {
            const path = button.dataset.openBunny || "";
            if (path.endsWith("/")) {
              els.bunnyPrefix.value = path;
              loadBunnyPath();
            }
          });
        });
      }

      function renderJobs() {
        const header = '<div class="list-head"><div>Job</div><div>Status</div><div>Progress</div><div>Details</div></div>';
        if (!state.jobs.length) {
          els.jobsList.innerHTML = header + '<div class="list-row"><div class="meta">No jobs yet.</div><div></div><div></div><div class="meta">Start a transfer to queue one.</div></div>';
          els.jobsStatus.textContent = "No background jobs running.";
          return;
        }
        els.jobsList.innerHTML = header + state.jobs.map((job) => {
          const progress = String(job.copied) + " copied / " + String(job.failed) + " failed";
          const details = job.lastKey ? escapeHtml(job.lastKey) : escapeHtml(job.message || "");
          return '<div class="list-row">' +
            '<div>' +
              '<div><strong>' + escapeHtml(job.id.slice(0, 8)) + '</strong></div>' +
              '<div class="meta">' + escapeHtml(job.sourceBucket) + ' → ' + escapeHtml(job.destinationZone) + '</div>' +
            '</div>' +
            '<div class="meta">' + escapeHtml(job.status) + '</div>' +
            '<div class="meta">' + escapeHtml(progress) + '</div>' +
            '<div class="meta">' + details + (job.lastError ? '<div class="error">' + escapeHtml(job.lastError) + '</div>' : '') + '</div>' +
          '</div>';
        }).join("");
        const running = state.jobs.find((job) => job.status === "running" || job.status === "queued");
        els.jobsStatus.textContent = running ? "Latest active job: " + running.id : "No active jobs.";
      }

      async function postJson(path, body) {
        const response = await fetch(path, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
          throw new Error(payload && payload.error ? payload.error : response.statusText || "Request failed");
        }
        return payload;
      }

      async function loadAwsBuckets() {
        setStatus(els.awsStatus, "Loading buckets...");
        try {
          const payload = await postJson("/api/aws/buckets", {
            accessKeyId: els.awsAccessKeyId.value.trim(),
            secretAccessKey: els.awsSecretAccessKey.value.trim(),
          });
          state.awsBuckets = Array.isArray(payload.buckets) ? payload.buckets : [];
          state.awsBucketRegions = new Map(state.awsBuckets.map((bucket) => [bucket.name, bucket.region || "us-east-1"]));
          els.awsBucketSelect.innerHTML = ['<option value="">Choose a bucket</option>'].concat(
            state.awsBuckets.map((bucket) => '<option value="' + escapeHtml(bucket.name) + '">' + escapeHtml(bucket.name) + '</option>'),
          ).join("");
          if (state.awsBuckets[0] && !els.awsBucketSelect.value) {
            els.awsBucketSelect.value = state.awsBuckets[0].name;
          }
          syncSummary();
          setStatus(els.awsStatus, String(state.awsBuckets.length) + " bucket(s) loaded.");
          log("Loaded " + String(state.awsBuckets.length) + " AWS bucket(s).");
          if (selectedBucket()) void loadAwsPath(false);
        } catch (error) {
          setStatus(els.awsStatus, "Could not load buckets.", "error");
          log(errorMessage(error), "error");
        }
      }

      async function loadAwsPath(append = false) {
        const bucket = selectedBucket();
        if (!bucket) {
          log("Choose an AWS bucket first.", "error");
          return;
        }
        if (!append) {
          state.awsItems = [];
          state.awsContinuationToken = null;
        }
        setStatus(els.awsStatus, "Loading " + bucket + "/" + els.awsPrefix.value.trim() + "...");
        try {
          const payload = await postJson("/api/aws/list", {
            accessKeyId: els.awsAccessKeyId.value.trim(),
            secretAccessKey: els.awsSecretAccessKey.value.trim(),
            region: selectedAwsRegion(),
            bucket,
            prefix: ensureTrailingSlash(els.awsPrefix.value),
            continuationToken: append ? state.awsContinuationToken : undefined,
          });
          state.awsItems = append ? state.awsItems.concat(payload.items || []) : (payload.items || []);
          state.awsContinuationToken = payload.nextContinuationToken || null;
          renderAwsList();
          setStatus(els.awsStatus, String(state.awsItems.length) + " item(s) visible." + (state.awsContinuationToken ? " More available." : ""));
        } catch (error) {
          setStatus(els.awsStatus, "Could not load bucket contents.", "error");
          log(errorMessage(error), "error");
        }
      }

      async function loadBunnyZones() {
        setStatus(els.bunnyStatus, "Loading zones...");
        try {
          const payload = await postJson("/api/bunny/zones", { apiKey: els.bunnyApiKey.value.trim() });
          state.bunnyZones = payload.zones || [];
          if (!state.bunnyZones.length) {
            els.bunnyZoneSelect.innerHTML = '<option value="">No storage zones returned</option>';
            setStatus(els.bunnyStatus, "No storage zones were returned for that Bunny API key.", "error");
            log("Bunny returned 0 storage zones. Double-check that you used the Bunny account API key from the dashboard, not a storage zone password, and that the account actually has storage zones.", "error");
            return;
          }
          els.bunnyZoneSelect.innerHTML = ['<option value="">Choose a zone</option>'].concat(state.bunnyZones.map((zone) => '<option value="' + escapeHtml(zone.name) + '">' + escapeHtml(zone.name) + '</option>')).join("");
          if (state.bunnyZones[0] && !els.bunnyZoneSelect.value.trim()) {
            els.bunnyZoneSelect.value = state.bunnyZones[0].name;
          }
          syncSummary();
          setStatus(els.bunnyStatus, String(state.bunnyZones.length) + " zone(s) loaded.");
          log("Loaded " + String(state.bunnyZones.length) + " Bunny storage zone(s).");
          if (selectedZone()) void loadBunnyPath();
        } catch (error) {
          setStatus(els.bunnyStatus, "Could not load zones.", "error");
          log(errorMessage(error), "error");
        }
      }

      async function loadBunnyPath() {
        const zoneName = selectedZone();
        if (!zoneName) {
          log("Choose a Bunny storage zone first.", "error");
          return;
        }
        const zone = state.bunnyZones.find((item) => item.name === zoneName);
        setStatus(els.bunnyStatus, "Loading " + zoneName + "/" + els.bunnyPrefix.value.trim() + "...");
        try {
          const payload = await postJson("/api/bunny/list", {
            apiKey: els.bunnyApiKey.value.trim(),
            zoneName,
            region: zone ? zone.region : "",
            path: ensureTrailingSlash(els.bunnyPrefix.value),
          });
          state.bunnyItems = payload.items || [];
          renderBunnyList();
          setStatus(els.bunnyStatus, String(state.bunnyItems.length) + " item(s) visible.");
        } catch (error) {
          setStatus(els.bunnyStatus, "Could not load zone contents.", "error");
          log(errorMessage(error), "error");
        }
      }

      async function refreshJobs() {
        try {
          const response = await fetch("/api/jobs");
          const payload = await response.json().catch(() => ({}));
          if (!response.ok) {
            throw new Error(payload && payload.error ? payload.error : response.statusText || "Request failed");
          }
          state.jobs = payload.jobs || [];
          renderJobs();
        } catch (error) {
          setStatus(els.jobsStatus, "Could not refresh jobs.", "error");
          log(errorMessage(error), "error");
        }
      }

      async function startTransfer() {
        const selections = Array.from(state.awsSelections.keys()).map((key) => {
          const item = state.awsItems.find((entry) => entry.key === key);
          return { kind: item && item.type === "folder" ? "folder" : "file", key };
        });
        if (!selections.length) {
          log("Select at least one file or folder to copy.", "error");
          return;
        }
        const bucket = selectedBucket();
        const zone = selectedZone();
        if (!bucket || !zone) {
          log("Choose both a source bucket and a destination zone first.", "error");
          return;
        }
        els.transferButton.disabled = true;
        setStatus(els.awsStatus, "Queueing background job...");
        setStatus(els.bunnyStatus, "Queueing background job...");
        log("Queueing transfer of " + String(selections.length) + " selected item(s).");
        try {
          const payload = await postJson("/api/transfer", {
            aws: {
              accessKeyId: els.awsAccessKeyId.value.trim(),
              secretAccessKey: els.awsSecretAccessKey.value.trim(),
              region: selectedAwsRegion(),
            },
            sourceBucket: bucket,
            sourcePrefix: ensureTrailingSlash(els.awsPrefix.value),
            selections,
            bunnyApiKey: els.bunnyApiKey.value.trim(),
            destinationZone: zone,
            destinationPrefix: ensureTrailingSlash(els.bunnyPrefix.value),
          });
          log("Job " + payload.job.id + " queued.");
          setStatus(els.awsStatus, "Job queued: " + payload.job.id);
          setStatus(els.bunnyStatus, "Job queued: " + payload.job.id);
          await refreshJobs();
        } catch (error) {
          log(errorMessage(error), "error");
          setStatus(els.awsStatus, "Queue failed.", "error");
          setStatus(els.bunnyStatus, "Queue failed.", "error");
        } finally {
          els.transferButton.disabled = false;
        }
      }

      restoreUiState();

      [
        els.awsAccessKeyId,
        els.awsSecretAccessKey,
        els.awsBucketSelect,
        els.awsPrefix,
        els.bunnyApiKey,
        els.bunnyZoneSelect,
        els.bunnyPrefix,
      ].forEach((element) => {
        element.addEventListener("input", writeUiState);
        element.addEventListener("change", writeUiState);
      });

      els.loadAwsBuckets.addEventListener("click", loadAwsBuckets);
      els.loadAwsPath.addEventListener("click", () => loadAwsPath(false));
      els.loadBunnyZones.addEventListener("click", loadBunnyZones);
      els.loadBunnyPath.addEventListener("click", loadBunnyPath);
      els.transferButton.addEventListener("click", startTransfer);
      els.clearSelection.addEventListener("click", () => {
        state.awsSelections.clear();
        renderAwsList();
        syncSummary();
        log("Selection cleared.");
        writeUiState();
      });
      els.awsUp.addEventListener("click", () => {
        els.awsPrefix.value = parentPrefix(els.awsPrefix.value);
        syncSummary();
        loadAwsPath(false);
      });
      els.bunnyUp.addEventListener("click", () => {
        els.bunnyPrefix.value = parentPrefix(els.bunnyPrefix.value);
        syncSummary();
        loadBunnyPath();
      });
      els.awsBucketSelect.addEventListener("change", () => {
        syncSummary();
        void loadAwsPath(false);
      });
      els.bunnyZoneSelect.addEventListener("change", () => {
        syncSummary();
        void loadBunnyPath();
      });
      els.awsPrefix.addEventListener("change", () => {
        void loadAwsPath(false);
      });
      els.bunnyPrefix.addEventListener("change", () => {
        syncSummary();
        void loadBunnyPath();
      });
      els.awsPrefix.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          void loadAwsPath(false);
        }
      });
      els.bunnyPrefix.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          void loadBunnyPath();
        }
      });
      els.bunnyPrefix.addEventListener("input", syncSummary);
      els.awsList.addEventListener("change", (event) => {
        const target = event.target;
        if (target instanceof HTMLInputElement && target.dataset.key) {
          if (target.checked) state.awsSelections.set(target.dataset.key, true);
          else state.awsSelections.delete(target.dataset.key);
          syncSummary();
        }
      });

      renderAwsList();
      renderBunnyList();
      renderJobs();
      syncSummary();
      writeUiState();
      void refreshJobs();
      setInterval(() => {
        void refreshJobs();
      }, 5000);
      log("Enter credentials, load buckets and zones, then browse to the folders you want to migrate.");
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
  const zone = await resolveBunnyZone(transfer.bunnyApiKey, transfer.destinationZone);
  const manager = getTransferManagerStub(env);
  const job = await manager.createJob({
    aws: transfer.aws,
    sourceBucket: transfer.sourceBucket,
    sourcePrefix: transfer.sourcePrefix,
    selections: transfer.selections,
    destinationPrefix: transfer.destinationPrefix || "",
    bunnyZone: zone,
  });
  return json({ job }, { status: 202 });
}

async function handleJobs(request: Request, env: AppEnv): Promise<Response> {
  const url = new URL(request.url);
  const manager = getTransferManagerStub(env);
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
    sourceBucket: requireString(obj.sourceBucket, "sourceBucket"),
    sourcePrefix: typeof obj.sourcePrefix === "string" ? obj.sourcePrefix : "",
    selections,
    bunnyApiKey: requireString(obj.bunnyApiKey, "bunnyApiKey"),
    destinationZone: requireString(obj.destinationZone, "destinationZone"),
    destinationPrefix: typeof obj.destinationPrefix === "string" ? obj.destinationPrefix : "",
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
): Promise<Response> {
  const url = `https://${host}${encodeUrlPath(path)}${query.toString() ? `?${query.toString()}` : ""}`;
  const amzDate = toAmzDate(new Date());
  const dateStamp = amzDate.slice(0, 8);
  const payloadHash = await sha256Hex(body ?? "");
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
      path: String((item as Record<string, unknown>).Path || item.path || ""),
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

async function buildTransferPlan(
  aws: AwsCredentials,
  bucket: string,
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
    const objects = await listAllS3Objects(aws, bucket, prefix);
    for (const object of objects) {
      if (!object.key.endsWith("/")) {
        plan.push({ sourceKey: object.key, destinationKey: relativePathFromPrefix(sourcePrefix, object.key) });
      }
    }
  }
  return dedupePlan(plan);
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
  bucket: string,
  zone: BunnyZone,
  plan: TransferPlanEntry[],
  destinationPrefix: string,
): Promise<{ copied: number; failed: number; errors: string[] }> {
  let copied = 0;
  let failed = 0;
  const errors: string[] = [];
  for (const entry of plan) {
    try {
      await copyObject(aws, bucket, zone, entry.sourceKey, destinationPrefix, entry.destinationKey);
      copied += 1;
    } catch (error) {
      failed += 1;
      errors.push(`${entry.sourceKey}: ${(error as Error).message}`);
    }
  }
  return { copied, failed, errors };
}

async function copyObject(
  aws: AwsCredentials,
  bucket: string,
  zone: BunnyZone,
  sourceKey: string,
  destinationPrefix: string,
  destinationKey: string,
): Promise<void> {
  const sourceResponse = await getS3Object(aws, bucket, sourceKey);
  if (!sourceResponse.ok || !sourceResponse.body) {
    throw new Error(await sourceResponse.text());
  }
  const finalKey = joinPrefix(destinationPrefix, destinationKey);
  const { path, fileName } = splitDestinationKey(finalKey);
  const endpoint = bunnyEndpointForRegion(zone.region);
  const destinationUrl = `https://${endpoint}/${encodeURIComponent(zone.name)}${path ? `/${encodeBunnyPath(path)}` : ""}/${encodeURIComponent(fileName)}`;
  const upload = await fetch(destinationUrl, {
    method: "PUT",
    headers: {
      AccessKey: zone.password || "",
      "Content-Type": sourceResponse.headers.get("content-type") || "application/octet-stream",
    },
    body: sourceResponse.body,
  });
  if (!upload.ok && upload.status !== 201) {
    throw new Error(await upload.text());
  }
}

async function getS3Object(aws: AwsCredentials, bucket: string, key: string): Promise<Response> {
  return signedS3Request(
    aws,
    "GET",
    `/${bucket}/${key}`,
    new URLSearchParams(),
    undefined,
  );
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
      if (!columns.some((column) => column.name === "source_prefix")) {
        this.ctx.storage.sql.exec(`ALTER TABLE jobs ADD COLUMN source_prefix TEXT NOT NULL DEFAULT ''`);
      }
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
          source_bucket, source_region,
          source_prefix,
          aws_access_key_id, aws_secret_access_key, aws_session_token,
          destination_zone, destination_zone_region, destination_zone_password,
          destination_prefix, selections_json,
          current_selection_index, current_folder_prefix, current_folder_page_json, current_folder_continuation_token,
          copied, failed, processed, last_key, last_error, message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      id,
      now,
      now,
      "queued",
      input.sourceBucket,
      input.aws.region,
      input.sourcePrefix || "",
      input.aws.accessKeyId,
      input.aws.secretAccessKey,
      input.aws.sessionToken || null,
      input.bunnyZone.name,
      input.bunnyZone.region,
      input.bunnyZone.password,
      input.destinationPrefix || "",
      JSON.stringify(input.selections),
      0,
      null,
      null,
      null,
      0,
      0,
      0,
      null,
      null,
      "Queued for transfer",
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

    if (selection.kind === "file") {
      try {
        await copyObject(
          {
            accessKeyId: job.aws_access_key_id,
            secretAccessKey: job.aws_secret_access_key,
            sessionToken: job.aws_session_token || undefined,
            region: job.source_region,
          },
          job.source_bucket,
          {
            id: 0,
            name: job.destination_zone,
            region: job.destination_zone_region,
            password: job.destination_zone_password,
          },
          selection.key,
          job.destination_prefix,
          relativePathFromPrefix(job.source_prefix, selection.key),
        );
        this.updateJob(job.id, {
          copied: job.copied + 1,
          processed: job.processed + 1,
          current_selection_index: job.current_selection_index + 1,
          last_key: selection.key,
          last_error: null,
          message: `Copied ${job.copied + 1} file(s)`,
          current_folder_prefix: null,
          current_folder_page_json: null,
          current_folder_continuation_token: null,
        });
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
        const listing = await listAwsObjects(
          {
            accessKeyId: job.aws_access_key_id,
            secretAccessKey: job.aws_secret_access_key,
            sessionToken: job.aws_session_token || undefined,
            region: job.source_region,
          },
          job.source_bucket,
          prefix,
          job.current_folder_continuation_token || undefined,
          { maxKeys: 100 },
        );
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
      await copyObject(
        {
          accessKeyId: job.aws_access_key_id,
          secretAccessKey: job.aws_secret_access_key,
          sessionToken: job.aws_session_token || undefined,
          region: job.source_region,
        },
        job.source_bucket,
        {
          id: 0,
          name: job.destination_zone,
          region: job.destination_zone_region,
          password: job.destination_zone_password,
        },
        key,
        job.destination_prefix,
        relativePathFromPrefix(job.source_prefix, key),
      );
      page.index += 1;
      const isPageDone = page.index >= page.keys.length;
      this.updateJob(job.id, {
        copied: job.copied + 1,
        processed: job.processed + 1,
        last_key: key,
        last_error: null,
        message: `Copied ${key}`,
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
      sourceBucket: row.source_bucket,
      destinationZone: row.destination_zone,
      destinationPrefix: row.destination_prefix,
      selections: JSON.parse(row.selections_json).length,
      copied: row.copied,
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

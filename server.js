const express    = require('express');
const http       = require('http');
const WebSocket  = require('ws');
const path       = require('path');
const fs         = require('fs');
const { exec, execFile } = require('child_process');
const { v4: uuidv4 }  = require('uuid');
const mime       = require('mime-types');
const archiver   = require('archiver');
const fileUpload = require('express-fileupload');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

// ─────────────────────────────────────────────────────────────────
// STRUCTURED LOGGER
// ─────────────────────────────────────────────────────────────────
const LOG_LEVEL = (process.env.LOG_LEVEL || 'info').toLowerCase();
const LEVELS    = { debug: 0, info: 1, warn: 2, error: 3 };
const lvlNum    = () => LEVELS[LOG_LEVEL] ?? 1;

function log(level, msg, meta = {}) {
  if ((LEVELS[level] ?? 1) < lvlNum()) return;
  const ts   = new Date().toISOString();
  const tag  = level.toUpperCase().padEnd(5);
  const metaStr = Object.keys(meta).length ? ' ' + JSON.stringify(meta) : '';
  const line = `[${ts}] [${tag}] ${msg}${metaStr}`;
  if (level === 'error') console.error(line);
  else if (level === 'warn')  console.warn(line);
  else console.log(line);
}

// ─────────────────────────────────────────────────────────────────
// PROCESS HEALTH — catch anything that would silently crash/hang
// ─────────────────────────────────────────────────────────────────
process.on('uncaughtException', (err, origin) => {
  log('error', 'UNCAUGHT EXCEPTION — process may be unstable', {
    origin, error: err.message, stack: err.stack ? err.stack.split('\n').slice(0,6).join(' | ') : ''
  });
});

process.on('unhandledRejection', (reason) => {
  const msg   = reason instanceof Error ? reason.message : String(reason);
  const stack = reason instanceof Error ? (reason.stack ? reason.stack.split('\n').slice(0,6).join(' | ') : '') : '';
  log('error', 'UNHANDLED PROMISE REJECTION', { reason: msg, stack });
});

process.on('SIGTERM', () => { log('info', 'Received SIGTERM — shutting down gracefully'); process.exit(0); });
process.on('SIGINT',  () => { log('info', 'Received SIGINT  — shutting down'); process.exit(0); });

// ── Event-loop lag monitor ──────────────────────────────────────
// Uses setImmediate to measure how long the event loop was blocked.
// If a synchronous op (like readdirSync on a huge dir) blocks for too
// long, health checks time out and the monitor thinks Nova is DOWN.
(function loopLagWatcher() {
  let last = Date.now();
  setInterval(() => {
    const now = Date.now();
    const lag = now - last - 1000; // expected 1000ms between ticks
    last = now;
    if (lag > 500) {
      log('warn', 'Event-loop blocked', { lag_ms: lag, hint: 'A synchronous operation held the loop. Check active jobs.' });
    } else {
      log('debug', 'Event-loop tick', { lag_ms: Math.max(0, lag) });
    }
  }, 1000).unref();
})();

// ── Process memory monitor ──────────────────────────────────────
setInterval(() => {
  const mem  = process.memoryUsage();
  const running = [...jobs.values()].filter(j => j.status === 'running');
  log('debug', 'Memory snapshot', {
    rss_mb:    Math.round(mem.rss      / 1024 / 1024),
    heap_mb:   Math.round(mem.heapUsed / 1024 / 1024),
    running_jobs: running.length,
    running_ids:  running.map(j => j.jobId.slice(0,8)).join(',') || null,
    uptime_s:  Math.round(process.uptime())
  });
}, 60_000).unref();

const ROOT_PATH = process.env.ROOT_PATH || '/';
const PORT      = process.env.PORT      || 9898;
const ROOT      = ROOT_PATH.replace(/\/+$/, '') || '';
const TRASH_DIR = path.join(ROOT || '/', '.nova_trash');

// Gotify config (optional)
const GOTIFY_URL   = (process.env.GOTIFY_URL   || '').trim();
const GOTIFY_TOKEN = (process.env.GOTIFY_TOKEN || '').trim();

if (!fs.existsSync(TRASH_DIR)) fs.mkdirSync(TRASH_DIR, { recursive: true });

// ── Request logging middleware ─────────────────────────────────
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const ms = Date.now() - start;
    const level = res.statusCode >= 500 ? 'error' : res.statusCode >= 400 ? 'warn' : 'debug';
    log(level, `${req.method} ${req.path}`, { status: res.statusCode, ms });
    if (ms > 5000) log('warn', 'Slow request', { method: req.method, path: req.path, ms });
  });
  next();
});

app.use(express.json());
app.use(fileUpload({
  limits:       { fileSize: 50 * 1024 * 1024 * 1024 },
  useTempFiles: true,
  tempFileDir:  '/tmp/',
  abortOnLimit: false
}));
app.use(express.static(path.join(__dirname, 'public')));

// ─────────────────────────────────────────────────────────────────
// PATH HELPERS
// ─────────────────────────────────────────────────────────────────
function resolvePath(p) {
  const normalized = path.normalize('/' + (p || '/')).replace(/\/\/+/g, '/');
  const result = ROOT + normalized;
  return result.length > ROOT.length + 1 && result.endsWith('/')
    ? result.replace(/\/$/, '') : result;
}
function toVirtual(abs) {
  if (ROOT === '') return abs || '/';
  if (abs.startsWith(ROOT)) {
    const v = abs.slice(ROOT.length);
    return v.startsWith('/') ? v : '/' + v;
  }
  return abs || '/';
}

// ─────────────────────────────────────────────────────────────────
// WEBSOCKET
// ─────────────────────────────────────────────────────────────────
const clients = new Map();

wss.on('connection', (ws) => {
  let clientId = null;
  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      if (msg.type === 'register' && msg.clientId) {
        clientId = msg.clientId;
        if (clients.has(clientId)) { try { clients.get(clientId).terminate(); } catch {} }
        clients.set(clientId, ws);
        ws.clientId = clientId;
        // Replay all running jobs for this client on reconnect
        for (const job of jobs.values()) {
          if (job.clientId === clientId && job.status === 'running') {
            ws.send(JSON.stringify({ type: 'job_update', job: sanitizeJob(job) }));
          }
        }
      }
    } catch {}
  });
  ws.on('close', () => { if (clientId) clients.delete(clientId); });
});

function broadcast(clientId, data) {
  const ws = clients.get(clientId);
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(data)); } catch {}
  }
}

// ─────────────────────────────────────────────────────────────────
// JOB STORE  — lives in the Node process, survives browser close
// ─────────────────────────────────────────────────────────────────
const jobs = new Map();

function createJob(type, clientId, meta = {}) {
  const jobId = uuidv4();
  const job   = {
    jobId, type, clientId,
    status: 'running', percent: 0, filePercent: 0,
    phase: 'starting', currentFile: '', log: [],
    startedAt: new Date().toISOString(), finishedAt: null,
    ...meta
  };
  jobs.set(jobId, job);
  log('info', 'Job created', { jobId, type, operation: meta.operation || null, sources: (meta.sources||[]).length, dest: meta.destination || null });

  // Gotify: notify job started
  const srcs     = meta.sources || [];
  const names    = srcs.slice(0, 3).map(s => s.split('/').pop()).join(', ')
                 + (srcs.length > 3 ? ` +${srcs.length - 3} more` : '');
  const opLabel  = (meta.operation === 'move') ? 'Move'
                 : type === 'delete'            ? 'Delete'
                 : type === 'delete-permanent'  ? 'Permanent delete'
                 : type === 'trash-empty'       ? 'Empty trash'
                 : type === 'upload'            ? 'Upload'
                 : 'Copy';
  const destLine = meta.destination ? `\n→ ${meta.destination}` : '';
  notifyGotify(
    `${opLabel} started ▶`,
    `${opLabel} of ${srcs.length || 'multiple'} item(s)${names ? '\n' + names : ''}${destLine}`,
    3
  );
  return job;
}

function updateJob(job, updates) {
  Object.assign(job, updates);
  broadcast(job.clientId, { type: 'job_update', job: sanitizeJob(job) });

  if (updates.status === 'done') {
    log('info', 'Job done', { jobId: job.jobId, type: job.type, operation: job.operation || null, errorCount: job.errorCount || 0 });
    const srcs  = job.sources || [];
    const names = srcs.slice(0, 3).map(s => s.split('/').pop()).join(', ')
                + (srcs.length > 3 ? ` +${srcs.length - 3} more` : '');
    const opLabel = job.operation === 'move' ? 'Moved'
                  : job.type === 'delete'     ? 'Deleted'
                  : job.type === 'delete-permanent' ? 'Permanently deleted'
                  : job.type === 'trash-empty' ? 'Trash emptied'
                  : job.type === 'upload'     ? 'Uploaded'
                  : 'Copied';
    const errNote = job.errorCount ? `\n⚠ ${job.errorCount} file(s) had errors` : '';
    notifyGotify(
      `${opLabel} complete ✓`,
      `${opLabel} ${srcs.length} item(s)\n${names}\n→ ${job.destination || ''}${errNote}`,
      5
    );
  } else if (updates.status === 'cancelled') {
    log('info', 'Job cancelled', { jobId: job.jobId, type: job.type });
    const cancelSrcs  = job.sources || [];
    const cancelNames = cancelSrcs.slice(0, 3).map(s => s.split('/').pop()).join(', ')
                      + (cancelSrcs.length > 3 ? ` +${cancelSrcs.length - 3} more` : '');
    const cancelPct   = updates.percent || 0;
    const cancelDest  = job.destination ? `\n→ ${job.destination}` : '';
    notifyGotify(
      'Transfer cancelled ⏹',
      `Cancelled at ${cancelPct}% — resumable.\n${cancelNames}${cancelDest}\nID: ${job.jobId.slice(0,8)}`,
      4
    );
  } else if (updates.status === 'error') {
    log('error', 'Job failed', { jobId: job.jobId, type: job.type, error: job.error });
    notifyGotify(
      `${job.type} failed ✗`,
      `Job: ${job.type}\nError: ${job.error || 'Unknown error'}\nID: ${job.jobId.slice(0,8)}`,
      9
    );
  }
}

function sanitizeJob(job) {
  return {
    jobId: job.jobId, type: job.type, status: job.status,
    percent: job.percent, filePercent: job.filePercent,
    phase: job.phase, currentFile: job.currentFile,
    startedAt: job.startedAt, finishedAt: job.finishedAt,
    operation: job.operation, sources: job.sources,
    destination: job.destination, error: job.error,
    log: (job.log || []).slice(-50)
  };
}

// Clean up completed jobs older than 1 hour
setInterval(() => {
  const cutoff = Date.now() - 3_600_000;
  for (const [id, job] of jobs) {
    if (job.status !== 'running' && job.finishedAt && new Date(job.finishedAt) < cutoff)
      jobs.delete(id);
  }
}, 60_000);

// ─────────────────────────────────────────────────────────────────
// GOTIFY
// ─────────────────────────────────────────────────────────────────
function notifyGotify(title, message, priority = 5) {
  if (!GOTIFY_URL || !GOTIFY_TOKEN) return;

  // Normalize base URL: strip trailing slashes, then append /message
  const base    = GOTIFY_URL.replace(/\/+$/, '');
  const fullUrl = `${base}/message?token=${GOTIFY_TOKEN}`;
  const body    = JSON.stringify({ title, message, priority });

  // Use native fetch (Node 18+) if available, else fall back to http/https
  if (typeof fetch !== 'undefined') {
    fetch(fullUrl, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      signal:  AbortSignal.timeout(8000)
    })
    .then(r => {
      if (!r.ok) log('warn', `[gotify] HTTP ${r.status} for "${title}"`);
      else        log('info',  `[gotify] sent: ${title}`);
    })
    .catch(e => log('warn', `[gotify] failed: ${e.message}`, { title }));
    return;
  }

  // Fallback: node http/https
  const https2 = require('https');
  const http2  = require('http');
  try {
    const u   = new URL(fullUrl);
    const mod = u.protocol === 'https:' ? https2 : http2;
    const req = mod.request({
      hostname: u.hostname,
      port:     u.port || (u.protocol === 'https:' ? 443 : 80),
      path:     u.pathname + u.search,
      method:   'POST',
      headers:  { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
    }, res => { res.resume(); log('info', `[gotify] sent: ${title} (status=${res.statusCode})`); });
    req.setTimeout(8000, () => { req.destroy(); log('warn', '[gotify] request timed out', { title }); });
    req.on('error', e => log('warn', `[gotify] request error: ${e.message}`, { title }));
    req.write(body); req.end();
  } catch (e) { log('warn', `[gotify] setup failed: ${e.message}`, { title }); }
}

// ─────────────────────────────────────────────────────────────────
// ROUTES — read-only ops
// ─────────────────────────────────────────────────────────────────
app.get('/api/jobs',   (req, res) => res.json({ jobs: [...jobs.values()].map(sanitizeJob) }));
app.get('/api/health', (req, res) => res.json({
  status: 'ok',
  uptime: Math.round(process.uptime()),
  runningJobs: [...jobs.values()].filter(j => j.status === 'running').length,
  pid: process.pid
}));

app.get('/api/list', (req, res) => {
  const dir = resolvePath(req.query.path || '/');
  try {
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    const files = entries
      .filter(e => e.name !== '.nova_trash')
      .map(e => {
        const fullPath = path.join(dir, e.name);
        let stat = {};
        try { stat = fs.statSync(fullPath); } catch {}
        return {
          name:        e.name,
          path:        toVirtual(fullPath),
          type:        e.isDirectory() ? 'directory' : 'file',
          size:        stat.size  || 0,
          modified:    stat.mtime || null,
          mime:        e.isDirectory() ? null : (mime.lookup(e.name) || 'application/octet-stream'),
          permissions: stat.mode  ? (stat.mode & 0o777).toString(8) : '---'
        };
      });
    res.json({ path: toVirtual(dir), entries: files });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/trash', (req, res) => {
  try {
    const metaFile = path.join(TRASH_DIR, '.meta.json');
    const meta = fs.existsSync(metaFile) ? JSON.parse(fs.readFileSync(metaFile)) : [];
    res.json({ items: meta });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/diskinfo', (req, res) => {
  const target = ROOT || '/';
  exec(`df -B1 "${target}" | tail -1 | awk '{print $2,$3,$4}'`, (err, stdout) => {
    if (err) return res.status(500).json({ error: err.message });
    const [total, used, avail] = stdout.trim().split(' ').map(Number);
    res.json({ total, used, avail });
  });
});

app.get('/api/stat', (req, res) => {
  const p = resolvePath(req.query.path);
  try {
    const stat = fs.statSync(p);
    res.json({
      name:        path.basename(p),
      path:        toVirtual(p),
      size:        stat.size,
      modified:    stat.mtime,
      created:     stat.birthtime,
      permissions: (stat.mode & 0o777).toString(8),
      isDirectory: stat.isDirectory(),
      mime:        mime.lookup(p) || null
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/preview', (req, res) => {
  const p = resolvePath(req.query.path);
  res.setHeader('Content-Type', mime.lookup(p) || 'application/octet-stream');
  fs.createReadStream(p).pipe(res);
});

app.get('/api/download', (req, res) => {
  const p = resolvePath(req.query.path);
  let stat;
  try { stat = fs.statSync(p); } catch (e) { return res.status(404).json({ error: e.message }); }
  if (stat.isDirectory()) {
    res.setHeader('Content-Disposition', `attachment; filename="${path.basename(p)}.zip"`);
    res.setHeader('Content-Type', 'application/zip');
    const archive = archiver('zip', { zlib: { level: 6 } });
    archive.pipe(res);
    archive.directory(p, path.basename(p));
    archive.finalize();
  } else {
    res.download(p);
  }
});

// ─────────────────────────────────────────────────────────────────
// ROUTES — instant ops (these are atomic/fast enough to be sync)
// ─────────────────────────────────────────────────────────────────
app.post('/api/mkdir', (req, res) => {
  const { path: p, name } = req.body;
  if (!name || name.includes('/') || name.includes('\\') || name === '.' || name === '..')
    return res.status(400).json({ error: 'Invalid folder name' });
  try {
    fs.mkdirSync(path.join(resolvePath(p), name), { recursive: true });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/rename', (req, res) => {
  const { oldPath, newName } = req.body;
  if (!newName || newName.includes('/') || newName.includes('\\') || newName === '.' || newName === '..')
    return res.status(400).json({ error: 'Invalid name' });
  const abs = resolvePath(oldPath);
  try {
    fs.renameSync(abs, path.join(path.dirname(abs), newName));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/chmod', (req, res) => {
  const { path: p, mode } = req.body;
  const abs = resolvePath(p);
  if (mode === undefined || mode === null)
    return res.status(400).json({ error: 'Missing mode' });
  const modeNum = parseInt(String(mode), 8);
  if (isNaN(modeNum) || modeNum < 0 || modeNum > 0o777)
    return res.status(400).json({ error: 'Invalid mode' });
  try {
    fs.chmodSync(abs, modeNum);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/trash/restore', (req, res) => {
  const { id } = req.body;
  const metaFile = path.join(TRASH_DIR, '.meta.json');
  let meta = fs.existsSync(metaFile) ? JSON.parse(fs.readFileSync(metaFile)) : [];
  const item = meta.find(m => m.id === id);
  if (!item) return res.status(404).json({ error: 'Not found' });
  const dest = resolvePath(item.originalPath);
  try {
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    fs.renameSync(item.trashPath, dest);
    meta = meta.filter(m => m.id !== id);
    fs.writeFileSync(metaFile, JSON.stringify(meta));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─────────────────────────────────────────────────────────────────
// ROUTES — background jobs (survive browser close)
// ─────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────
// TRANSFER ENGINE  (copy / move)  — v2
//
// MOVE strategy (in priority order):
//   1. Top-level rename   — if source root is on same device as dest,
//                           one rename() moves everything in O(1). No I/O.
//   2. Per-file rename    — for same-device moves where a top-level
//                           rename failed (e.g. dest already exists).
//   3. Stream copy+delete — cross-device: copy to <dest>.nova_part,
//                           verify size, atomic rename to final name,
//                           then delete source. Source never deleted
//                           until copy is confirmed.
//
// COPY strategy:
//   fs.copyFile() → uses copy_file_range / sendfile on Linux
//   (kernel-accelerated, zero userspace buffer). Falls back to stream
//   copy when resuming a partial file (needs byte-offset seek).
//
// Concurrency:
//   N_WORKERS (default 4) files are processed in parallel.
//   Each worker is async so the event loop is never blocked.
//   Directory creation is also async (fs.mkdir with recursive).
//
// Directory walk:
//   Uses fs.opendir() — async, streaming, no stack-overflow risk
//   on deep trees, no blocking readdir on large directories.
//
// Resumability:
//   A ledger file  <destDir>/.nova_xfr_<jobId>.json  tracks every
//   file's state: pending | done | error.
//   On resume, done files are skipped, the in-progress .nova_part
//   file is detected and copied from its current byte offset.
//
// Cancellation:
//   job.cancelRequested = true  stops workers after the current file.
//   Sets status to 'cancelled' and preserves the ledger for resume.
//
// Event-loop safety:
//   No synchronous I/O in hot paths. All fs calls are async variants.
//   Progress/ledger updates are debounced so 33k-file moves don't
//   flood WebSocket or disk.
// ─────────────────────────────────────────────────────────────────

const N_WORKERS        = parseInt(process.env.COPY_WORKERS || '4');  // parallel copy workers
const LEDGER_FLUSH_MS  = 2000;        // write ledger to disk at most every 2s
const PROGRESS_FLUSH_MS = 300;        // send WS progress at most every 300ms
const PART_EXT         = '.nova_part';

// ── Async directory walk ─────────────────────────────────────────
// Yields {srcAbs, relPath} for every non-symlink file under dirAbs.
// Uses async opendir so it never blocks the event loop.
async function* walkDir(dirAbs, relBase, depth = 0) {
  if (depth > 60) {
    log('warn', 'walkDir: max depth reached', { dir: dirAbs });
    return;
  }
  let dir;
  try { dir = await fs.promises.opendir(dirAbs); }
  catch (e) { log('warn', 'walkDir: cannot open dir', { dir: dirAbs, error: e.message }); return; }

  for await (const entry of dir) {
    const abs = path.join(dirAbs, entry.name);
    const rel = path.join(relBase, entry.name);
    if (entry.isSymbolicLink()) {
      log('debug', 'walkDir: skipping symlink', { path: abs });
      continue;
    }
    if (entry.isDirectory()) {
      yield* walkDir(abs, rel, depth + 1);
    } else if (entry.isFile()) {
      yield { srcAbs: abs, relPath: rel };
    }
  }
}

// ── File copy: kernel-accelerated or stream resume ───────────────
// Stream-based copy — always uses Node streams so the event loop
// receives regular backpressure callbacks and is never locked out.
// fs.copyFile / copy_file_range blocks libuv threads for the entire
// file duration; streams yield every STREAM_HWM bytes.
const STREAM_HWM = parseInt(process.env.COPY_HWM || String(2 * 1024 * 1024)); // 2 MB default (tune with COPY_HWM env)

async function copyOneFile(srcAbs, destAbs, startByte, totalSize, onProgress) {
  const partPath  = destAbs + PART_EXT;
  const writeFlag = startByte > 0 ? 'a' : 'w';

  await new Promise((resolve, reject) => {
    const rdOpts = { highWaterMark: STREAM_HWM };
    if (startByte > 0) rdOpts.start = startByte;

    const rd = fs.createReadStream(srcAbs, rdOpts);
    const wr = fs.createWriteStream(partPath, { flags: writeFlag, highWaterMark: STREAM_HWM });

    let written = startByte;

    rd.on('data', chunk => {
      written += chunk.length;
      if (totalSize > 0) onProgress(Math.min(99, Math.round((written / totalSize) * 100)));
    });
    rd.on('error', e => { wr.destroy(); reject(e); });
    wr.on('error', e => { rd.destroy(); reject(e); });
    wr.on('finish', () => { onProgress(100); resolve(); });
    rd.pipe(wr);
  });

  // Verify size using the known totalSize (no extra stat needed)
  if (totalSize > 0) {
    const partStat = await fs.promises.stat(partPath);
    if (partStat.size !== totalSize) {
      throw Object.assign(
        new Error(`Size mismatch: expected ${totalSize}, got ${partStat.size}`),
        { code: 'ESIZE' }
      );
    }
  }

  // Atomic commit: .nova_part → final name
  await fs.promises.rename(partPath, destAbs);
}

// ── Ledger helpers ───────────────────────────────────────────────
function makeLedger(jobId, clientId, operation, destination) {
  return { jobId, clientId, operation, destination, files: {} };
  // files: { [destAbs]: 'pending' | 'done' | 'error' }
}

function ledgerSetState(ledger, destAbs, state) {
  ledger.files[destAbs] = state;
}

async function flushLedger(ledgerPath, ledger) {
  try { await fs.promises.writeFile(ledgerPath, JSON.stringify(ledger)); } catch {}
}

// ── Conflict helpers ────────────────────────────────────────────

/** Returns a path that doesn't exist yet by appending (2), (3)... before the extension.
 *  Uses async fs.promises.access so it doesn't block the event loop. */
async function findFreeName(destPath) {
  try { await fs.promises.access(destPath); } catch { return destPath; } // doesn't exist — use as-is
  const dir  = path.dirname(destPath);
  const ext  = path.extname(destPath);
  const base = path.basename(destPath, ext);
  let n = 2;
  while (n < 10000) {
    const candidate = path.join(dir, `${base} (${n})${ext}`);
    try { await fs.promises.access(candidate); n++; } catch { return candidate; }
  }
  return path.join(dir, `${base} (${Date.now()})${ext}`);
}

// ── API routes ───────────────────────────────────────────────────

// Pre-flight conflict check — called BEFORE starting a transfer.
// Returns a list of destination paths that already exist so the
// client can ask the user what to do before any files move.
app.post('/api/transfer/check', async (req, res) => {
  const { sources, destination } = req.body;
  if (!sources?.length || !destination)
    return res.status(400).json({ error: 'Missing sources or destination' });

  const destAbs   = resolvePath(destination);
  const conflicts = [];

  for (const src of sources) {
    const srcAbs  = resolvePath(src);
    const srcName = path.basename(srcAbs);
    let   srcStat;
    try { srcStat = await fs.promises.stat(srcAbs); } catch { continue; }

    if (srcStat.isDirectory()) {
      // Walk the source tree and check each file at its dest path
      for await (const entry of walkDir(srcAbs, srcName)) {
        const dstPath = path.join(destAbs, entry.relPath);
        try {
          const dstStat = await fs.promises.stat(dstPath);
          let srcFileStat;
          try { srcFileStat = await fs.promises.stat(entry.srcAbs); } catch { srcFileStat = srcStat; }
          conflicts.push({
            name:     entry.relPath,
            srcPath:  src,
            destPath: toVirtual(dstPath),
            srcSize:  srcFileStat.size,
            dstSize:  dstStat.size,
            srcMtime: srcFileStat.mtime,
            dstMtime: dstStat.mtime,
            isDir:    false
          });
        } catch {} // dest doesn't exist — no conflict
        // Cap at 500 conflicts to avoid hanging the browser on massive trees
        if (conflicts.length >= 500) break;
      }
    } else {
      const dstPath = path.join(destAbs, srcName);
      try {
        const dstStat = await fs.promises.stat(dstPath);
        conflicts.push({
          name:     srcName,
          srcPath:  src,
          destPath: toVirtual(dstPath),
          srcSize:  srcStat.size,
          dstSize:  dstStat.size,
          srcMtime: srcStat.mtime,
          dstMtime: dstStat.mtime,
          isDir:    false
        });
      } catch {}
    }
    if (conflicts.length >= 500) break;
  }

  res.json({ conflicts, truncated: conflicts.length >= 500 });
});

app.post('/api/transfer', (req, res) => {
  const { sources, destination, operation, clientId, conflictResolutions } = req.body;
  if (!sources?.length || !destination)
    return res.status(400).json({ error: 'Missing sources or destination' });
  log('info', 'Transfer requested', {
    operation, sources: sources.length, destination,
    clientId: (clientId || '').slice(0, 12),
    conflicts_resolved: conflictResolutions ? Object.keys(conflictResolutions).length : 0
  });
  const destAbs = resolvePath(destination);
  // conflictResolutions: { [destVirtualPath]: 'overwrite' | 'skip' | 'keepboth' }
  const job = createJob('transfer', clientId, { operation, sources, destination, phase: 'starting' });
  res.json({ jobId: job.jobId, started: true });
  setImmediate(() => runTransfer(job, sources, destAbs, operation, null, null, conflictResolutions || {}));
});

app.post('/api/transfer/cancel', (req, res) => {
  const { jobId } = req.body;
  const job = jobs.get(jobId);
  if (!job || job.status !== 'running')
    return res.status(404).json({ error: 'No running job with that ID' });
  job.cancelRequested = true;
  log('info', `[${jobId.slice(0,8)}] Cancel requested`);
  res.json({ ok: true });
});

app.post('/api/transfer/resume', async (req, res) => {
  const { jobId, clientId } = req.body;
  if (!jobId) return res.status(400).json({ error: 'Missing jobId' });

  // Find ledger — check in-memory job store first, then disk scan
  let ledgerPath = null;
  const existingJob = jobs.get(jobId);
  if (existingJob?.destination) {
    const c = path.join(resolvePath(existingJob.destination), `.nova_xfr_${jobId}.json`);
    if (fs.existsSync(c)) ledgerPath = c;
  }
  if (!ledgerPath) {
    // Use async exec so we don't block the event loop during ledger search
    ledgerPath = await new Promise((resolve) => {
      exec(
        `find "${ROOT || '/'}" -maxdepth 10 -name ".nova_xfr_${jobId}.json" 2>/dev/null | head -1`,
        { timeout: 15000 },
        (err, stdout) => resolve((stdout || '').trim() || null)
      );
    });
  }
  if (!ledgerPath || !fs.existsSync(ledgerPath))
    return res.status(404).json({ error: 'No resumable transfer found for this jobId' });

  let ledger;
  try { ledger = JSON.parse(fs.readFileSync(ledgerPath)); }
  catch { return res.status(500).json({ error: 'Corrupt ledger file' }); }

  const pending = Object.entries(ledger.files).filter(([, s]) => s !== 'done').length;
  if (!pending) return res.json({ message: 'Transfer already complete', jobId });

  const resumeJob = createJob('transfer', clientId || ledger.clientId, {
    operation:   ledger.operation,
    sources:     [ledger.destination], // unused — engine reads from ledger
    destination: ledger.destination,
    phase:       'resuming',
    resumedFrom: jobId
  });
  res.json({ jobId: resumeJob.jobId, started: true, pendingFiles: pending });
  setImmediate(() => runTransfer(
    resumeJob, null, resolvePath(ledger.destination),
    ledger.operation, ledgerPath, ledger
  ));
});

// ── Main transfer runner ─────────────────────────────────────────
async function runTransfer(job, sources, destAbs, operation, existingLedgerPath, existingLedger, conflictResolutions = {}) {
  const jid         = job.jobId.slice(0, 8);
  const ledgerPath  = existingLedgerPath
    || path.join(destAbs, `.nova_xfr_${job.jobId}.json`);
  const isResume    = !!existingLedger;
  let   ledger      = existingLedger
    || makeLedger(job.jobId, job.clientId, operation, job.destination);
  const originalSources = sources ? [...sources] : null; // preserve for move cleanup

  log('info', `[${jid}] Transfer started`, { operation, isResume, dest: destAbs });

  // ── MOVE FAST PATH: try top-level rename on each source ─────────
  // If source and destination are on the same device, a single rename()
  // on the top-level item moves the entire tree in O(1) — no I/O at all.
  if (operation === 'move' && !isResume) {
    const fastMoved = [];
    const needsCopy = [];

    for (const src of sources) {
      const srcAbs  = resolvePath(src);
      const srcName = path.basename(srcAbs);
      const dstItem = path.join(destAbs, srcName);

      try { await fs.promises.mkdir(destAbs, { recursive: true }); } catch {}

      // Check if dest already exists and we have a top-level resolution
      let destExists = false;
      try { await fs.promises.stat(dstItem); destExists = true; } catch {}

      if (destExists) {
        const res = conflictResolutions[toVirtual(dstItem)];
        if (res === 'skip') {
          log('info', `[${jid}] Skipping (conflict resolution)`, { src: srcName });
          continue;
        }
        if (res === 'keepboth') {
          // Re-assign dstItem to a non-colliding name
          const newDst = await findFreeName(dstItem);
          try {
            await fs.promises.rename(srcAbs, newDst);
            fastMoved.push(src);
            log('info', `[${jid}] Kept both — renamed to`, { name: path.basename(newDst) });
          } catch (e2) {
            if (e2.code === 'EXDEV') { needsCopy.push({ src, dst: newDst }); }
            else { needsCopy.push({ src, dst: newDst }); }
          }
          continue;
        }
        if (res === 'overwrite') {
          // For directories, fall through to per-file merge (can't rename over non-empty dir)
          // For files, rename will overwrite atomically on Linux
          const dstStat = await fs.promises.stat(dstItem).catch(() => null);
          if (dstStat && dstStat.isDirectory()) {
            needsCopy.push({ src, dst: dstItem });
            continue;
          }
          // File overwrite — fall through to rename below
        }
        // No resolution provided: treat as overwrite (shouldn't happen if UI sends resolutions)
      }

      try {
        await fs.promises.rename(srcAbs, dstItem);
        fastMoved.push(src);
        log('info', `[${jid}] Fast rename ✓`, { src: srcName });
      } catch (e) {
        if (e.code === 'EXDEV') {
          needsCopy.push({ src, dst: dstItem });
        } else if (e.code === 'ENOTEMPTY' || e.code === 'EEXIST') {
          needsCopy.push({ src, dst: dstItem });
          log('debug', `[${jid}] Dest exists, will merge per-file`, { src: srcName });
        } else {
          const errMsg = `Cannot move "${srcName}": ${e.message}`;
          log('error', `[${jid}] Top-level rename failed`, { src: srcName, code: e.code, error: e.message });
          notifyGotify(`Move failed ✗`, `${errMsg}\nJob: ${jid}`, 9);
          updateJob(job, { status: 'error', error: errMsg, finishedAt: new Date().toISOString() });
          return;
        }
      }
    }

    if (fastMoved.length) {
      log('info', `[${jid}] Fast-moved ${fastMoved.length} item(s) via rename`);
    }
    if (!needsCopy.length) {
      // Everything moved instantly — done
      updateJob(job, { status: 'done', percent: 100, filePercent: 100, phase: 'done',
                       finishedAt: new Date().toISOString() });
      return;
    }
    // Re-assign sources to only what still needs copying
    // needsCopy entries are {src, dst} — extract src paths for the walk below
    sources = needsCopy.map(e => (typeof e === 'string' ? e : e.src));
  }

  // ── BUILD FILE LIST via async walk ──────────────────────────────
  updateJob(job, { phase: 'scanning' });

  let fileQueue; // [{srcAbs, destPath, size}]

  if (isResume) {
    // Resume: rebuild list from ledger (pending + error entries)
    fileQueue = Object.entries(ledger.files)
      .filter(([, state]) => state !== 'done')
      .map(([destPath]) => {
        // Find the original src by walking ledger — we stored it
        const entry = ledger.fileMap?.[destPath];
        return entry ? { srcAbs: entry.srcAbs, destPath, size: entry.size || 0 } : null;
      })
      .filter(Boolean);
    log('info', `[${jid}] Resume: ${fileQueue.length} pending files`);
  } else {
    fileQueue = [];
    const scanStart = Date.now();

    for (const src of sources) {
      const srcAbs  = resolvePath(src);
      const srcName = path.basename(srcAbs);
      let   stat;
      try { stat = await fs.promises.stat(srcAbs); }
      catch (e) {
        const errMsg = `Cannot access source: ${src} — ${e.message}`;
        log('error', `[${jid}] Stat failed`, { src, error: e.message });
        updateJob(job, { status: 'error', error: errMsg, finishedAt: new Date().toISOString() });
        return;
      }

      if (stat.isDirectory()) {
        // Collect entries first, then stat in batches to avoid serialising
        // 17k individual stat calls which stalls the event loop.
        const entries = [];
        for await (const entry of walkDir(srcAbs, srcName)) {
          entries.push(entry);
        }
        const STAT_BATCH = 32;
        for (let si = 0; si < entries.length; si += STAT_BATCH) {
          const batch = entries.slice(si, si + STAT_BATCH);
          const sizes = await Promise.all(
            batch.map(e => fs.promises.stat(e.srcAbs).then(s => s.size).catch(() => 0))
          );
          for (let bi = 0; bi < batch.length; bi++) {
            const entry = batch[bi];
            fileQueue.push({
              srcAbs:   entry.srcAbs,
              destPath: path.join(destAbs, entry.relPath),
              size:     sizes[bi],
              srcRoot:  srcAbs
            });
          }
          // Yield between stat batches
          await new Promise(r => setImmediate(r));
        }
      } else {
        fileQueue.push({ srcAbs, destPath: path.join(destAbs, srcName), size: stat.size, srcRoot: srcAbs });
      }
    }

    const scanMs = Date.now() - scanStart;
    log('info', `[${jid}] Scan complete`, { files: fileQueue.length, scan_ms: scanMs });

    // Populate ledger with all files
    ledger.fileMap = {};
    for (const f of fileQueue) {
      ledger.files[f.destPath] = 'pending';
      ledger.fileMap[f.destPath] = { srcAbs: f.srcAbs, size: f.size };
    }
    await flushLedger(ledgerPath, ledger);
  }

  // ── CREATE DESTINATION DIRECTORIES ──────────────────────────────
  // Process in small concurrent batches — never queue all at once.
  // Promise.all with thousands of mkdirs saturates libuv's thread
  // pool (4 threads by default) and starves HTTP/health-check I/O.
  {
    const MKDIR_BATCH = 20;
    const dirsNeeded = [...new Set(fileQueue.map(f => path.dirname(f.destPath)))];
    log('debug', `[${jid}] Creating ${dirsNeeded.length} destination directories`);
    for (let i = 0; i < dirsNeeded.length; i += MKDIR_BATCH) {
      if (job.cancelRequested) break;
      await Promise.all(
        dirsNeeded.slice(i, i + MKDIR_BATCH)
          .map(d => fs.promises.mkdir(d, { recursive: true }).catch(() => {}))
      );
      // Yield after every batch so HTTP (health checks) can be served
      await new Promise(r => setImmediate(r));
    }
  }

  const totalFiles = fileQueue.length;
  const totalBytes = fileQueue.reduce((s, f) => s + f.size, 0);
  let   doneFiles  = 0;
  let   doneBytes  = 0;

  log('info', `[${jid}] Transfer plan`, { total_files: totalFiles, total_bytes: totalBytes, operation });
  updateJob(job, { phase: operation === 'move' ? 'moving' : 'copying', percent: 0 });

  // ── PROGRESS & LEDGER DEBOUNCE ───────────────────────────────────
  let progressTimer = null;
  let ledgerTimer   = null;
  let lastPct       = -1;

  function scheduleProgress(currentFile) {
    if (progressTimer) return;
    progressTimer = setTimeout(() => {
      progressTimer = null;
      const pct = Math.round((doneBytes / Math.max(totalBytes, 1)) * 100);
      if (pct !== lastPct) { lastPct = pct; updateJob(job, { percent: pct, currentFile: currentFile || '' }); }
    }, PROGRESS_FLUSH_MS);
  }

  function scheduleLedger() {
    if (ledgerTimer) return;
    ledgerTimer = setTimeout(async () => { ledgerTimer = null; await flushLedger(ledgerPath, ledger); }, LEDGER_FLUSH_MS);
  }

  // ── PROCESS FILES ────────────────────────────────────────────────
  //
  // TWO-PHASE STRATEGY:
  //
  //  PHASE 1 — RENAME PATH (move, same device):
  //    Serial batches of RENAME_BATCH renames per event-loop tick.
  //    Renames are ~microseconds each — no I/O, no libuv thread needed.
  //    If a cross-device error (EXDEV) is detected, the remaining files
  //    are pushed into the copy queue and Phase 2 runs.
  //
  //  PHASE 2 — PARALLEL COPY PATH:
  //    N_WORKERS concurrent copyOneFile() streams.
  //    JS is single-threaded so shared counters (doneFiles, doneBytes)
  //    are safe to update between await points — no mutex needed.
  //    Each worker checks cancelRequested before each file.
  //    Progress reports are debounced so N workers don't flood WS.
  //
  const RENAME_BATCH = 64; // renames per event-loop yield

  let forceStreamCopy = (operation !== 'move'); // copies always stream
  let copyQueueStart  = 0; // index into fileQueue where copy phase begins

  // ── PHASE 1: Same-device rename (move only) ──────────────────────
  if (!forceStreamCopy) {
    for (let i = 0; i < fileQueue.length; ) {
      if (job.cancelRequested) break;

      const batchEnd = Math.min(i + RENAME_BATCH, fileQueue.length);
      let crossDevice = false;

      for (; i < batchEnd; i++) {
        if (job.cancelRequested) break;
        const f     = fileQueue[i];
        const fname = path.basename(f.destPath);

        if (ledger.files[f.destPath] === 'done') {
          doneFiles++; doneBytes += f.size; continue;
        }

        let effectiveDest = f.destPath;
        try {
          await fs.promises.stat(effectiveDest);
          const resolution = conflictResolutions[toVirtual(effectiveDest)] || 'overwrite';
          if (resolution === 'skip') {
            ledgerSetState(ledger, f.destPath, 'done');
            doneFiles++; doneBytes += f.size; scheduleProgress(fname); scheduleLedger(); continue;
          }
          if (resolution === 'keepboth') effectiveDest = await findFreeName(effectiveDest);
        } catch {} // dest doesn't exist — proceed

        try {
          await fs.promises.rename(f.srcAbs, effectiveDest);
          ledgerSetState(ledger, f.destPath, 'done');
          doneFiles++; doneBytes += f.size;
          scheduleProgress(fname); scheduleLedger();
        } catch (e) {
          if (e.code === 'EXDEV' || e.code === 'ENOTEMPTY' || e.code === 'EEXIST') {
            // Switch remaining to stream-copy mode
            if (e.code === 'EXDEV')
              log('info', `[${jid}] Cross-device detected at file ${i}, switching to copy mode`);
            crossDevice = true;
            copyQueueStart = i; // this file and everything after needs copying
            break;
          }
          // Real error
          const errMsg = `Cannot move "${fname}": ${e.message} (${e.code})`;
          log('error', `[${jid}] Rename failed`, { file: fname, code: e.code });
          job.log.push(errMsg);
          ledgerSetState(ledger, f.destPath, 'error');
          notifyGotify(`Transfer error ✗`, `${errMsg}\nJob: ${jid}`, 9);
          updateJob(job, { status: 'error', error: errMsg, finishedAt: new Date().toISOString() });
          await flushLedger(ledgerPath, ledger);
          if (progressTimer) clearTimeout(progressTimer);
          if (ledgerTimer)   clearTimeout(ledgerTimer);
          return;
        }
      }

      if (crossDevice) { forceStreamCopy = true; break; }
      await new Promise(r => setImmediate(r));
    }
  }

  // ── PHASE 2: Parallel stream copy ────────────────────────────────
  if (forceStreamCopy && !job.cancelRequested) {
    const copySlice = fileQueue.slice(copyQueueStart);
    let   copyIdx   = 0; // shared atomic index (safe: JS single-threaded)
    let   fatalErr  = null;

    async function copyWorker() {
      while (copyIdx < copySlice.length && !job.cancelRequested && !fatalErr) {
        const f     = copySlice[copyIdx++];
        const fname = path.basename(f.destPath);

        if (ledger.files[f.destPath] === 'done') {
          doneFiles++; doneBytes += f.size; continue;
        }

        // Resume: check for partial .part file
        let startByte = 0;
        try {
          const ps = await fs.promises.stat(f.destPath + PART_EXT);
          if (ps.size > 0 && ps.size < f.size) {
            startByte = ps.size;
            log('info', `[${jid}] Resuming partial`, { file: fname, from: startByte, total: f.size });
          }
        } catch {}

        // Conflict resolution
        let effectiveDest = f.destPath;
        if (startByte === 0) {
          try {
            await fs.promises.stat(effectiveDest);
            const resolution = conflictResolutions[toVirtual(effectiveDest)] || 'overwrite';
            if (resolution === 'skip') {
              log('debug', `[${jid}] Skipping (conflict)`, { file: fname });
              ledgerSetState(ledger, f.destPath, 'done');
              doneFiles++; doneBytes += f.size; scheduleProgress(fname); scheduleLedger(); continue;
            }
            if (resolution === 'keepboth') effectiveDest = await findFreeName(effectiveDest);
          } catch {}
        }

        const copyStart = Date.now();
        try {
          await copyOneFile(f.srcAbs, effectiveDest, startByte, f.size, pct => {
            // With multiple workers the currentFile shown is "last updated" — that's fine
            const bytesDone = doneBytes + startByte + (pct / 100) * Math.max(0, f.size - startByte);
            updateJob(job, {
              currentFile: fname, filePercent: pct,
              percent: Math.round((bytesDone / Math.max(totalBytes, 1)) * 100)
            });
          });
        } catch (e) {
          const errMsg = `Failed to copy "${fname}": ${e.message}`;
          log('error', `[${jid}] Copy failed`, { file: fname, code: e.code, error: e.message, start_byte: startByte });
          job.log.push(errMsg);
          ledgerSetState(ledger, f.destPath, 'error');
          notifyGotify(`Transfer error ✗`, `${errMsg}\nJob: ${jid}\nClick Resume to continue.`, 9);
          fatalErr = { errMsg, finishedAt: new Date().toISOString() };
          break;
        }

        const copyMs = Date.now() - copyStart;
        const copied  = f.size - startByte;
        log('debug', `[${jid}] Copied`, {
          file: fname,
          mb:   Math.round(f.size / 1024 / 1024),
          ms:   copyMs,
          mbps: copied > 0 ? Math.round((copied / 1024 / 1024) / Math.max(copyMs / 1000, 0.001)) : 0
        });

        if (operation === 'move') {
          try { await fs.promises.unlink(f.srcAbs); }
          catch (e) { if (e.code !== 'ENOENT') job.log.push(`Warning: could not delete source "${fname}": ${e.message}`); }
        }

        if (effectiveDest !== f.destPath)
          log('info', `[${jid}] Kept both`, { original: fname, saved_as: path.basename(effectiveDest) });

        ledgerSetState(ledger, f.destPath, 'done');
        doneFiles++;
        doneBytes += f.size;
        scheduleProgress(fname);
        scheduleLedger();
      }
    }

    // Launch N_WORKERS workers in parallel
    const workerCount = Math.min(N_WORKERS, Math.max(1, copySlice.length));
    log('info', `[${jid}] Starting ${workerCount} copy worker(s) for ${copySlice.length} file(s)`);
    await Promise.all(Array.from({ length: workerCount }, copyWorker));

    // If a worker hit a fatal error, abort now
    if (fatalErr) {
      updateJob(job, { status: 'error', error: fatalErr.errMsg, finishedAt: fatalErr.finishedAt });
      await flushLedger(ledgerPath, ledger);
      if (progressTimer) clearTimeout(progressTimer);
      if (ledgerTimer)   clearTimeout(ledgerTimer);
      return;
    }
  }

  // Flush any pending debounced timers
  if (progressTimer) { clearTimeout(progressTimer); progressTimer = null; }
  if (ledgerTimer)   { clearTimeout(ledgerTimer);   ledgerTimer   = null; }

  // ── CANCELLED ────────────────────────────────────────────────────
  if (job.cancelRequested) {
    await flushLedger(ledgerPath, ledger);
    log('info', `[${jid}] Transfer cancelled`, { done: doneFiles, remaining: totalFiles - doneFiles });
    updateJob(job, { status: 'cancelled', percent: Math.round((doneFiles / Math.max(totalFiles, 1)) * 100), phase: 'cancelled', finishedAt: new Date().toISOString() });
    return;
  }

  // ── ERRORED ──────────────────────────────────────────────────────
  if (job.status === 'error') { await flushLedger(ledgerPath, ledger); return; }

  // ── CLEAN UP SOURCE DIRS (move only) ─────────────────────────────
  if (operation === 'move' && originalSources) {
    updateJob(job, { phase: 'cleaning' });
    for (const src of originalSources) {
      const srcAbs = resolvePath(src);
      try {
        const stat = await fs.promises.stat(srcAbs).catch(() => null);
        if (stat && stat.isDirectory()) {
          await fs.promises.rm(srcAbs, { recursive: true, force: true });
          log('debug', `[${jid}] Removed source dir`, { dir: srcAbs });
        }
      } catch {}
    }
  }

  // ── DONE ─────────────────────────────────────────────────────────
  const totalMs = Date.now() - (new Date(job.startedAt).getTime());
  log('info', `[${jid}] Transfer complete`, {
    operation, files: doneFiles, bytes: doneBytes, total_ms: totalMs,
    avg_mbps: doneBytes > 0 ? Math.round((doneBytes / 1024 / 1024) / Math.max(totalMs / 1000, 0.001)) : 0
  });

  try { await fs.promises.unlink(ledgerPath); } catch {}

  updateJob(job, { status: 'done', percent: 100, filePercent: 100, phase: 'done', finishedAt: new Date().toISOString() });
}

// DELETE (to trash) ──────────────────────────────────────────────
app.post('/api/delete', (req, res) => {
  const { paths, clientId } = req.body;
  if (!paths?.length) return res.status(400).json({ error: 'No paths' });

  const metaFile = path.join(TRASH_DIR, '.meta.json');

  // For tiny deletes (≤5 items) do it synchronously so UI feels instant
  if (paths.length <= 5) {
    let meta = fs.existsSync(metaFile) ? JSON.parse(fs.readFileSync(metaFile)) : [];
    const errors = [];
    for (const p of paths) {
      const abs      = resolvePath(p);
      const id       = uuidv4();
      const trashPath = path.join(TRASH_DIR, id);
      try {
        fs.renameSync(abs, trashPath);
        meta.push({ id, originalPath: p, name: path.basename(abs), deletedAt: new Date().toISOString(), trashPath });
      } catch (err) { errors.push({ path: p, error: err.message }); }
    }
    fs.writeFileSync(metaFile, JSON.stringify(meta));
    return errors.length ? res.status(207).json({ errors }) : res.json({ success: true });
  }

  // Large batch delete — run as background job
  const job = createJob('delete', clientId, { sources: paths, phase: 'deleting' });
  res.json({ jobId: job.jobId, started: true });
  setImmediate(() => runDelete(job, paths, metaFile));
});

async function runDelete(job, paths, metaFile) {
  let meta = [];
  try { meta = fs.existsSync(metaFile) ? JSON.parse(fs.readFileSync(metaFile)) : []; } catch {}
  const total = paths.length;
  let done = 0;

  for (const p of paths) {
    const abs       = resolvePath(p);
    const id        = uuidv4();
    const trashPath = path.join(TRASH_DIR, id);
    try {
      fs.renameSync(abs, trashPath);
      meta.push({ id, originalPath: p, name: path.basename(abs), deletedAt: new Date().toISOString(), trashPath });
    } catch (e) {
      job.log.push(`Failed to trash ${p}: ${e.message}`);
    }
    done++;
    if (done % 10 === 0 || done === total) {
      // Write meta incrementally so we don't lose track if process dies
      try { fs.writeFileSync(metaFile, JSON.stringify(meta)); } catch {}
      updateJob(job, { percent: Math.round((done / total) * 100), currentFile: path.basename(p) });
      // Yield to event loop every batch so health checks and HTTP stay responsive
      await new Promise(r => setImmediate(r));
    }
  }
  try { fs.writeFileSync(metaFile, JSON.stringify(meta)); } catch {}
  updateJob(job, { status: 'done', percent: 100, phase: 'done', finishedAt: new Date().toISOString(), sources: paths });
}

// PERMANENT DELETE ───────────────────────────────────────────────
app.post('/api/delete-permanent', (req, res) => {
  const { paths, clientId } = req.body;
  if (!paths?.length) return res.status(400).json({ error: 'No paths' });

  if (paths.length <= 5) {
    const errors = [];
    for (const p of paths) {
      try { fs.rmSync(resolvePath(p), { recursive: true, force: true }); }
      catch (err) { errors.push({ path: p, error: err.message }); }
    }
    return errors.length ? res.status(207).json({ errors }) : res.json({ success: true });
  }

  const job = createJob('delete-permanent', clientId, { sources: paths, phase: 'deleting' });
  res.json({ jobId: job.jobId, started: true });
  setImmediate(() => {
    const total = paths.length;
    let done = 0;
    for (const p of paths) {
      try { fs.rmSync(resolvePath(p), { recursive: true, force: true }); }
      catch (e) { job.log.push(`Failed: ${p}: ${e.message}`); }
      done++;
      if (done % 10 === 0 || done === total)
        updateJob(job, { percent: Math.round((done / total) * 100), currentFile: path.basename(p) });
    }
    updateJob(job, { status: 'done', percent: 100, phase: 'done', finishedAt: new Date().toISOString(), sources: paths });
  });
});

// EMPTY TRASH ────────────────────────────────────────────────────
app.post('/api/trash/empty', (req, res) => {
  const { clientId } = req.body;
  const metaFile = path.join(TRASH_DIR, '.meta.json');
  let meta = [];
  try { meta = fs.existsSync(metaFile) ? JSON.parse(fs.readFileSync(metaFile)) : []; } catch {}

  if (meta.length === 0) { fs.writeFileSync(metaFile, '[]'); return res.json({ success: true }); }

  if (meta.length <= 10) {
    for (const item of meta) { try { fs.rmSync(item.trashPath, { recursive: true, force: true }); } catch {} }
    fs.writeFileSync(metaFile, '[]');
    return res.json({ success: true });
  }

  const job = createJob('trash-empty', clientId, { sources: meta.map(i => i.trashPath), phase: 'deleting' });
  res.json({ jobId: job.jobId, started: true });
  setImmediate(() => {
    const total = meta.length;
    let done = 0;
    for (const item of meta) {
      try { fs.rmSync(item.trashPath, { recursive: true, force: true }); }
      catch (e) { job.log.push(`Failed: ${item.name}: ${e.message}`); }
      done++;
      if (done % 5 === 0 || done === total) {
        // Shrink meta file as we go — so a crash doesn't leave orphans
        const remaining = meta.slice(done);
        try { fs.writeFileSync(metaFile, JSON.stringify(remaining)); } catch {}
        updateJob(job, { percent: Math.round((done / total) * 100), currentFile: item.name });
      }
    }
    try { fs.writeFileSync(metaFile, '[]'); } catch {}
    updateJob(job, { status: 'done', percent: 100, phase: 'done', finishedAt: new Date().toISOString() });
  });
});

// UPLOAD ─────────────────────────────────────────────────────────
// Files land in /tmp first (express-fileupload useTempFiles:true)
// We then move them in a background job — so closing the browser
// after the upload HTTP request completes doesn't lose the file.
app.post('/api/upload', (req, res) => {
  if (!req.files || !Object.keys(req.files).length) return res.status(400).json({ error: 'No files' });
  const destDir  = resolvePath(req.body.path || '/');
  const clientId = req.body.clientId || 'unknown';
  const fileList = Array.isArray(req.files.files) ? req.files.files : [req.files.files];
  const names    = fileList.map(f => f.name);

  const job = createJob('upload', clientId, {
    sources:     names,
    destination: req.body.path || '/',
    phase:       'saving',
    operation:   'upload'
  });
  // Respond immediately — browser can close after this
  res.json({ jobId: job.jobId, started: true, files: names });

  setImmediate(async () => {
    const total = fileList.length;
    let done    = 0;
    for (const file of fileList) {
      const dest = path.join(destDir, file.name);
      updateJob(job, { currentFile: file.name, filePercent: 0, percent: Math.round((done / total) * 100) });
      try {
        await file.mv(dest);
        done++;
        updateJob(job, { percent: Math.round((done / total) * 100), filePercent: 100 });
      } catch (e) {
        job.log.push(`Failed to save ${file.name}: ${e.message}`);
        done++;
        updateJob(job, { percent: Math.round((done / total) * 100) });
      }
    }
    updateJob(job, { status: 'done', percent: 100, phase: 'done', finishedAt: new Date().toISOString() });
  });
});


// ─────────────────────────────────────────────────────────────────
// FILE EDITOR — read / write text files
// ─────────────────────────────────────────────────────────────────
const EDITABLE_EXTS = new Set([
  'txt','md','markdown','log','json','yaml','yml','toml','xml','html','css',
  'js','ts','jsx','tsx','py','rb','go','rs','sh','bash','zsh','env',
  'conf','cfg','ini','properties','gitignore','dockerfile','makefile',
  'vue','svelte','graphql','proto','csv','tsv','sql','php','java','c',
  'cpp','h','hpp','cs','swift','kt','r','lua','pl','fish','bat','ps1'
]);

app.get('/api/file/read', (req, res) => {
  const p   = resolvePath(req.query.path);
  const ext = path.extname(p).toLowerCase().replace('.', '');
  if (!EDITABLE_EXTS.has(ext)) return res.status(400).json({ error: 'File type not editable' });
  try {
    const content = fs.readFileSync(p, 'utf8');
    const stat    = fs.statSync(p);
    res.json({ content, size: stat.size, modified: stat.mtime });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/file/write', (req, res) => {
  const { path: p, content } = req.body;
  if (content === undefined) return res.status(400).json({ error: 'No content' });
  const abs = resolvePath(p);
  const ext = path.extname(abs).toLowerCase().replace('.', '');
  if (!EDITABLE_EXTS.has(ext)) return res.status(400).json({ error: 'File type not editable' });
  try {
    // Atomic write: write to .tmp then rename
    const tmp = abs + '.nova_edit_tmp';
    fs.writeFileSync(tmp, content, 'utf8');
    fs.renameSync(tmp, abs);
    const stat = fs.statSync(abs);
    log('info', 'File saved', { path: toVirtual(abs), size: stat.size });
    res.json({ success: true, size: stat.size, modified: stat.mtime });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─────────────────────────────────────────────────────────────────
// ZIP / UNZIP — background jobs, survive browser close, resumable
// ─────────────────────────────────────────────────────────────────
app.post('/api/zip', (req, res) => {
  const { sources, destination, clientId } = req.body;
  if (!sources?.length || !destination) return res.status(400).json({ error: 'Missing sources or destination' });
  const destAbs = resolvePath(destination);
  const job     = createJob('zip', clientId, { sources, destination, phase: 'zipping' });
  res.json({ jobId: job.jobId, started: true });
  setImmediate(() => runZip(job, sources, destAbs));
});

async function runZip(job, sources, destAbs) {
  const jid = job.jobId.slice(0, 8);
  log('info', `[${jid}] Zip started`, { sources: sources.length, dest: destAbs });

  // Count total size for progress
  let totalBytes = 0;
  for (const src of sources) {
    try {
      const s = fs.statSync(resolvePath(src));
      if (s.isDirectory()) {
        const entries = fs.readdirSync(resolvePath(src));
        for (const e of entries) {
          try { const es = fs.statSync(path.join(resolvePath(src), e)); totalBytes += es.size; } catch {}
        }
      } else { totalBytes += s.size; }
    } catch {}
  }
  totalBytes = Math.max(totalBytes, 1);

  // Use a .nova_part file so partial zips are never left as the real filename
  const partPath = destAbs + PART_EXT;

  try {
    await new Promise((resolve, reject) => {
      const output  = fs.createWriteStream(partPath);
      const archive = archiver('zip', { zlib: { level: 6 } });
      let processedBytes = 0;
      let aborted = false;

      // Store refs so cancel can abort cleanly
      job._zipArchive = archive;
      job._zipOutput  = output;

      // Poll cancelRequested
      const cancelPoll = setInterval(() => {
        if (job.cancelRequested && !aborted) {
          aborted = true;
          clearInterval(cancelPoll);
          archive.abort();
          output.destroy();
          try { fs.unlinkSync(partPath); } catch {}
          updateJob(job, {
            status: 'cancelled', percent: job.percent || 0,
            finishedAt: new Date().toISOString(),
            _zipSources: sources, _zipDest: toVirtual(destAbs)
          });
          resolve(); // resolve so we don't double-error
        }
      }, 200);

      archive.on('progress', data => {
        if (aborted) return;
        processedBytes = data.fs.processedBytes || 0;
        const pct = Math.min(99, Math.round((processedBytes / totalBytes) * 100));
        updateJob(job, { percent: pct, phase: 'zipping' });
      });

      archive.on('entry', entry => {
        if (aborted) return;
        updateJob(job, { currentFile: entry.name, phase: 'zipping' });
      });

      archive.on('error', e => { clearInterval(cancelPoll); if (!aborted) reject(e); });
      output.on('error', e => { clearInterval(cancelPoll); if (!aborted) reject(e); });
      output.on('close', () => {
        clearInterval(cancelPoll);
        if (!aborted) resolve();
      });

      archive.pipe(output);
      for (const src of sources) {
        const abs  = resolvePath(src);
        const name = path.basename(abs);
        try {
          const s = fs.statSync(abs);
          if (s.isDirectory()) archive.directory(abs, name);
          else                  archive.file(abs, { name });
        } catch (e) { job.log.push(`Skipped ${name}: ${e.message}`); }
      }
      archive.finalize();
    });

    // Cancelled case handled inside promise
    if (job.status === 'cancelled') {
      log('info', `[${jid}] Zip cancelled`);
      notifyGotify('Zip cancelled ⏹', `${path.basename(destAbs)} at ${job.percent||0}%\nID: ${jid} — resumable`, 4);
      return;
    }

    // Atomic commit: .nova_part → final name
    await fs.promises.rename(partPath, destAbs);
    log('info', `[${jid}] Zip complete`, { dest: destAbs });
    notifyGotify('Zip complete ✓', `Created: ${path.basename(destAbs)}\nFrom ${sources.length} item(s)`, 5);
    updateJob(job, { status: 'done', percent: 100, phase: 'done', finishedAt: new Date().toISOString() });
  } catch (e) {
    log('error', `[${jid}] Zip failed`, { error: e.message });
    try { fs.unlinkSync(partPath); } catch {}
    try { fs.unlinkSync(destAbs); } catch {}
    notifyGotify('Zip failed ✗', `${path.basename(destAbs)}\n${e.message}`, 9);
    updateJob(job, { status: 'error', error: e.message, finishedAt: new Date().toISOString() });
  }
}

// Resume a cancelled zip — restart from scratch (can't partially continue a zip archive)
app.post('/api/zip/resume', (req, res) => {
  const { jobId, clientId } = req.body;
  if (!jobId) return res.status(400).json({ error: 'Missing jobId' });
  const oldJob = jobs.get(jobId);
  if (!oldJob) return res.status(404).json({ error: 'Job not found' });
  const sources = oldJob._zipSources || oldJob.sources;
  const dest    = oldJob._zipDest    || oldJob.destination;
  if (!sources?.length || !dest) return res.status(400).json({ error: 'Cannot resume: missing sources/destination' });
  const destAbs = resolvePath(dest);
  const newJob  = createJob('zip', clientId || oldJob.clientId, {
    sources, destination: dest, phase: 'zipping', resumedFrom: jobId
  });
  res.json({ jobId: newJob.jobId, started: true });
  setImmediate(() => runZip(newJob, sources, destAbs));
});

app.post('/api/unzip', (req, res) => {
  const { path: p, destination, clientId } = req.body;
  if (!p) return res.status(400).json({ error: 'Missing path' });
  const abs     = resolvePath(p);
  const destDir = destination ? resolvePath(destination) : path.dirname(abs);
  const job     = createJob('unzip', clientId, {
    sources: [p], destination: toVirtual(destDir), phase: 'extracting'
  });
  res.json({ jobId: job.jobId, started: true });
  setImmediate(() => runUnzip(job, abs, destDir));
});

async function runUnzip(job, zipPath, destDir) {
  const jid  = job.jobId.slice(0, 8);
  const name = path.basename(zipPath);
  log('info', `[${jid}] Unzip started`, { file: name, dest: destDir });

  try { await fs.promises.mkdir(destDir, { recursive: true }); } catch {}

  try {
    await new Promise((resolve, reject) => {
      const child = execFile('unzip', ['-o', zipPath, '-d', destDir], { timeout: 300_000 }, (err) => {
        if (job.cancelRequested) return; // handled by kill handler
        if (err && err.killed) { return; } // killed by cancel
        if (err && err.code !== 0 && err.code !== 1) {
          // code 1 = "at least one warning" — still success
          // Try Node fallback
          (async () => {
            try {
              const StreamZip = require('node-stream-zip');
              const zip = new StreamZip.async({ file: zipPath });
              await zip.extract(null, destDir);
              await zip.close();
              resolve();
            } catch (e2) {
              reject(new Error('Unzip failed: ' + (err.message || e2.message)));
            }
          })();
          return;
        }
        resolve();
      });

      job._unzipProc = child;

      // Parse unzip progress lines
      let lineBuffer = '';
      let fileCount = 0;
      if (child.stdout) {
        child.stdout.on('data', chunk => {
          lineBuffer += chunk;
          const lines = lineBuffer.split('\n');
          lineBuffer  = lines.pop() || '';
          for (const line of lines) {
            const m = line.match(/inflating:\s+(.+)/);
            if (m) {
              fileCount++;
              updateJob(job, { currentFile: path.basename(m[1].trim()), phase: 'extracting',
                               percent: Math.min(99, fileCount) });
            }
          }
        });
      }

      // Poll cancelRequested
      const cancelPoll = setInterval(() => {
        if (job.cancelRequested) {
          clearInterval(cancelPoll);
          try { child.kill('SIGTERM'); } catch {}
          updateJob(job, {
            status: 'cancelled', finishedAt: new Date().toISOString(),
            _unzipPath: toVirtual(zipPath), _unzipDest: toVirtual(destDir)
          });
          resolve();
        }
      }, 200);

      child.on('exit', () => clearInterval(cancelPoll));
    });

    if (job.status === 'cancelled') {
      log('info', `[${jid}] Unzip cancelled`);
      notifyGotify('Unzip cancelled ⏹', `${name} — resumable\nID: ${jid}`, 4);
      return;
    }

    log('info', `[${jid}] Unzip complete`, { dest: destDir });
    notifyGotify('Unzip complete ✓', `Extracted: ${name}\n→ ${toVirtual(destDir)}`, 5);
    updateJob(job, { status: 'done', percent: 100, phase: 'done', finishedAt: new Date().toISOString() });
  } catch (e) {
    log('error', `[${jid}] Unzip failed`, { error: e.message });
    notifyGotify('Unzip failed ✗', `${name}\n${e.message}`, 9);
    updateJob(job, { status: 'error', error: e.message, finishedAt: new Date().toISOString() });
  }
}

// Cancel zip/unzip job
app.post('/api/zip/cancel', (req, res) => {
  const { jobId } = req.body;
  const job = jobs.get(jobId);
  if (!job || job.status !== 'running') return res.status(404).json({ error: 'No running job' });
  job.cancelRequested = true;
  if (job._unzipProc) { try { job._unzipProc.kill('SIGTERM'); } catch {} }
  log('info', `[${jobId.slice(0,8)}] Zip/unzip cancel requested`);
  res.json({ ok: true });
});

// Resume a cancelled unzip — re-extract (unzip -o overwrites safely)
app.post('/api/unzip/resume', (req, res) => {
  const { jobId, clientId } = req.body;
  if (!jobId) return res.status(400).json({ error: 'Missing jobId' });
  const oldJob = jobs.get(jobId);
  if (!oldJob) return res.status(404).json({ error: 'Job not found' });
  const zipPath = oldJob._unzipPath || (oldJob.sources && oldJob.sources[0]);
  const dest    = oldJob._unzipDest || oldJob.destination;
  if (!zipPath) return res.status(400).json({ error: 'Cannot resume: missing zip path' });
  const absZip  = resolvePath(zipPath);
  const absDest = dest ? resolvePath(dest) : path.dirname(absZip);
  const newJob  = createJob('unzip', clientId || oldJob.clientId, {
    sources: [zipPath], destination: toVirtual(absDest), phase: 'extracting', resumedFrom: jobId
  });
  res.json({ jobId: newJob.jobId, started: true });
  setImmediate(() => runUnzip(newJob, absZip, absDest));
});

// ─────────────────────────────────────────────────────────────────
// DIRECTORY SIZE — async du with SSE streaming so UI stays live
// ─────────────────────────────────────────────────────────────────
app.get('/api/dirsize', (req, res) => {
  const p = resolvePath(req.query.path);
  let stat;
  try { stat = fs.statSync(p); }
  catch (e) { return res.status(404).json({ error: e.message }); }

  // Single file — respond immediately, no streaming needed
  if (!stat.isDirectory()) return res.json({ size: stat.size, files: 1, done: true });

  // Directory — walk with Node.js (no shell dependency, works on Alpine busybox)
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  let aborted  = false;
  let totalBytes = 0;
  let totalFiles = 0;

  const hb = setInterval(() => {
    if (res.writableEnded) { clearInterval(hb); aborted = true; }
    else res.write(': hb\n\n');
  }, 1000);

  // Async dir walk using a queue — never blocks the event loop
  async function walk(dir, depth) {
    if (aborted || depth > 50) return;
    let entries;
    try { entries = await fs.promises.readdir(dir, { withFileTypes: true }); }
    catch { return; } // permission denied etc — skip silently

    for (const entry of entries) {
      if (aborted) return;
      const full = path.join(dir, entry.name);
      if (entry.isSymbolicLink()) continue;
      if (entry.isDirectory()) {
        await walk(full, depth + 1);
      } else if (entry.isFile()) {
        try {
          const s = await fs.promises.stat(full);
          totalBytes += s.size;
          totalFiles++;
        } catch {}
        // Yield every 500 files so HTTP stays responsive
        if (totalFiles % 500 === 0) await new Promise(r => setImmediate(r));
      }
    }
  }

  walk(p, 0).then(() => {
    clearInterval(hb);
    if (!res.writableEnded) {
      res.write(`data: ${JSON.stringify({ size: totalBytes, files: totalFiles, done: true, partial: false })}\n\n`);
      res.end();
    }
  }).catch(() => {
    // Even on unexpected error, send what we collected so far — never show "Error"
    clearInterval(hb);
    if (!res.writableEnded) {
      res.write(`data: ${JSON.stringify({ size: totalBytes, files: totalFiles, done: true, partial: true })}\n\n`);
      res.end();
    }
  });

  req.on('close', () => { aborted = true; clearInterval(hb); });
});

// ─────────────────────────────────────────────────────────────────
// OLLAMA INTEGRATION
// ─────────────────────────────────────────────────────────────────
const OLLAMA_URL   = (process.env.OLLAMA_URL || 'http://localhost:11434').replace(/\/+$/, '');
const MODEL_TEXT   = process.env.OLLAMA_TEXT_MODEL   || 'llama3.2:1b';
const MODEL_VISION = process.env.OLLAMA_VISION_MODEL || 'moondream:latest';
const MODEL_EMBED  = process.env.OLLAMA_EMBED_MODEL  || 'nomic-embed-text:latest';
const MODEL_AGENT  = process.env.OLLAMA_AGENT_MODEL  || MODEL_TEXT;

async function ollamaFetch(endpoint, body, timeoutMs = 60_000) {
  const ctrl   = new AbortController();
  const timer  = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const r = await fetch(`${OLLAMA_URL}${endpoint}`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ ...body, stream: false }),
      signal:  ctrl.signal
    });
    if (!r.ok) throw new Error(`Ollama HTTP ${r.status}`);
    return await r.json();
  } finally { clearTimeout(timer); }
}

// Status — which models are loaded
app.get('/api/ollama/status', async (req, res) => {
  try {
    const r = await fetch(`${OLLAMA_URL}/api/tags`, { signal: AbortSignal.timeout(5000) });
    if (!r.ok) return res.json({ available: false, error: `HTTP ${r.status}` });
    const d = await r.json();
    const names = (d.models || []).map(m => m.name);
    res.json({
      available: true,
      models:    names,
      text:      names.find(n => n.startsWith(MODEL_TEXT.split(':')[0])) || null,
      vision:    names.find(n => n.startsWith(MODEL_VISION.split(':')[0])) || null,
      embed:     names.find(n => n.startsWith(MODEL_EMBED.split(':')[0])) || null,
      url:       OLLAMA_URL
    });
  } catch (e) { res.json({ available: false, error: e.message }); }
});

// Model suggestions — tells the UI what to pull for more file type coverage
app.get('/api/ollama/suggest-models', async (req, res) => {
  let installedNames = [];
  try {
    const r = await fetch(`${OLLAMA_URL}/api/tags`, { signal: AbortSignal.timeout(5000) });
    if (r.ok) {
      const d = await r.json();
      installedNames = (d.models || []).map(m => m.name);
    }
  } catch {}

  const hasModel = (name) => installedNames.some(n => n.startsWith(name.split(':')[0]));

  const suggestions = [
    {
      model:       'llama3.2:1b',
      size:        '1.3 GB',
      use:         'Text summarize, rename, tag (fast)',
      types:       ['txt','md','json','yaml','csv','code','log','conf'],
      priority:    1,
      installed:   hasModel('llama3.2:1b'),
      pullCmd:     'ollama pull llama3.2:1b'
    },
    {
      model:       'moondream:latest',
      size:        '1.7 GB',
      use:         'Image description (jpg, png, heic, webp)',
      types:       ['jpg','jpeg','png','gif','webp','bmp','heic','heif','avif'],
      priority:    2,
      installed:   hasModel('moondream'),
      pullCmd:     'ollama pull moondream'
    },
    {
      model:       'nomic-embed-text:latest',
      size:        '274 MB',
      use:         'Semantic search across all text files',
      types:       ['all text'],
      priority:    1,
      installed:   hasModel('nomic-embed-text'),
      pullCmd:     'ollama pull nomic-embed-text'
    },
    {
      model:       'llama3.2:3b',
      size:        '2.0 GB',
      use:         'PDFs, long docs, complex analysis (smarter than 1b)',
      types:       ['pdf','docx','xlsx','long documents'],
      priority:    3,
      installed:   hasModel('llama3.2:3b'),
      pullCmd:     'ollama pull llama3.2:3b'
    },
    {
      model:       'qwen2.5:3b',
      size:        '1.9 GB',
      use:         'Code files, scripts, config analysis',
      types:       ['py','js','ts','go','rs','java','c','cpp','sh','dockerfile'],
      priority:    3,
      installed:   hasModel('qwen2.5'),
      pullCmd:     'ollama pull qwen2.5:3b'
    }
  ];

  res.json({ suggestions });
});

// ── Shared content extractor — handles text, PDF (via strings), images ──────
const TEXT_EXTS = new Set([
  'txt','md','markdown','log','csv','tsv','json','yaml','yml','toml','xml',
  'html','css','js','ts','jsx','tsx','py','rb','go','rs','sh','bash','zsh',
  'fish','php','java','c','cpp','h','hpp','cs','swift','kt','r','sql','env',
  'conf','cfg','ini','properties','gitignore','dockerfile','makefile',
  'vue','svelte','astro','graphql','proto','bat','ps1','lua','pl','ex','exs'
]);
const IMAGE_EXTS = new Set(['jpg','jpeg','png','gif','webp','bmp','tiff','tif','svg','heic','heif','avif']);

async function extractFileContent(abs) {
  const name  = path.basename(abs);
  const ext   = path.extname(name).toLowerCase().replace('.', '');
  const mtype = mime.lookup(name) || '';

  if (IMAGE_EXTS.has(ext) || mtype.startsWith('image/')) {
    return { kind: 'image', text: '', ext, name };
  }

  if (TEXT_EXTS.has(ext) || mtype.startsWith('text/')) {
    try {
      const txt = await fs.promises.readFile(abs, 'utf8');
      return { kind: 'text', text: txt.slice(0, 14000), ext, name };
    } catch (e) { return { kind: 'error', text: '', ext, name, error: e.message }; }
  }

  if (ext === 'pdf' || mtype === 'application/pdf') {
    // Extract printable strings from raw PDF bytes — works without pdftotext
    try {
      const buf = await fs.promises.readFile(abs);
      // Pull visible ASCII runs from the PDF binary
      const raw = buf.toString('binary');
      const runs = raw.match(/[ -~\t\n\r]{5,}/g) || [];
      const text = runs.filter(s => /[a-zA-Z]{3,}/.test(s)).join(' ').replace(/\s+/g,' ').slice(0, 12000);
      return { kind: 'text', text, ext: 'pdf', name };
    } catch (e) { return { kind: 'error', text: '', ext: 'pdf', name, error: e.message }; }
  }

  // For directories — return a listing summary
  if (ext === '' || !ext) {
    try {
      const s = fs.statSync(abs);
      if (s.isDirectory()) {
        const entries = fs.readdirSync(abs).slice(0, 60);
        return { kind: 'text', text: `Directory listing:\n${entries.join('\n')}`, ext: 'dir', name };
      }
    } catch {}
  }

  // Unknown binary — return minimal metadata
  try {
    const s = await fs.promises.stat(abs);
    return { kind: 'binary', text: `File: ${name}\nSize: ${s.size} bytes\nType: ${mtype || ext || 'unknown'}`, ext, name };
  } catch (e) { return { kind: 'error', text: '', ext, name, error: e.message }; }
}

// Summarize any file
app.post('/api/ollama/summarize', async (req, res) => {
  const { path: p } = req.body;
  const abs = resolvePath(p);

  let content;
  try { content = await extractFileContent(abs); }
  catch (e) { return res.status(400).json({ error: e.message }); }

  if (content.kind === 'error') return res.status(400).json({ error: content.error || 'Cannot read file' });

  if (content.kind === 'image') {
    // Redirect client to use describe-image instead
    return res.status(400).json({ error: 'IMAGE', isImage: true });
  }

  const model = MODEL_TEXT;
  log('info', '[ollama] summarize', { file: content.name, model, chars: content.text.length });
  try {
    const d = await ollamaFetch('/api/generate', {
      model,
      prompt: `Summarize this file in 2-4 sentences. Be factual and concise.\nFile: ${content.name}\n\n${content.text}`
    }, 90_000);
    const summary = (d.response || '').trim();
    notifyGotify('Nova AI ✦', `Summarized: ${content.name}\n${summary.slice(0, 200)}`, 3);
    res.json({ summary });
  } catch (e) {
    log('error', '[ollama] summarize failed', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// Agent: plan file operations in a directory
app.post('/api/ollama/agent-plan', async (req, res) => {
  const { dir, prompt } = req.body || {};
  const vDir = typeof dir === 'string' && dir ? dir : '/';
  if (!prompt || typeof prompt !== 'string' || !prompt.trim()) {
    return res.status(400).json({ error: 'Missing prompt' });
  }

  const abs = resolvePath(vDir);
  let listing = '';
  try {
    const entries = fs.readdirSync(abs, { withFileTypes: true }).slice(0, 200);
    listing = entries.map(e => {
      const mark = e.isDirectory() ? '[D]' : '[F]';
      if (e.isDirectory()) return `${mark} ${e.name}`;
      const ext = path.extname(e.name).toLowerCase().replace('.', '');
      const m   = mime.lookup(e.name) || '';
      const isImg = (m && String(m).startsWith('image/')) || IMAGE_EXTS.has(ext);
      return `${mark} ${e.name} | ext=${ext || '-'} | mime=${m || '-'} | image=${isImg ? 'yes' : 'no'}`;
    }).join('\n');
  } catch (e) {
    listing = '(unable to read directory contents)';
  }

  const sysPrompt = [
    'You are a cautious file-manager AI inside the Nova file manager.',
    'Your job is to turn the user request into safe file operations inside the current directory tree.',
    'Supported operations (ONLY these):',
    '- mkdir: create a directory. Fields: { "op": "mkdir", "path": "<absolute nova path starting with />" }',
    '- move: move/rename a file or folder. Fields: { "op": "move", "from": "<abs source>", "to": "<abs destination>" }',
    '- move_filter: move a set of items in ONE directory. Fields: { "op": "move_filter", "dir": "<abs dir>", "target": "<abs target dir>", "filter": "non_image_files"|"image_files"|"directories"|"files" }',
    '- move_except: move all items in a directory EXCEPT specific names. Fields: { "op": "move_except", "dir": "<abs dir>", "target": "<abs target dir>", "except": ["name1", "name2"] }',
    '',
    'Rules:',
    '- Stay within the Nova root. Never use relative paths like "./" or "../".',
    '- Prefer a single target folder instead of many small ones when the user says "into a single folder".',
    '- Do NOT delete anything; do not emit delete, copy, or other operations.',
    '- Use move_filter when the request describes moving a category of items (e.g. "all non-image files") to avoid listing hundreds of moves.',
    '- Use move_except when the user wants to move "everything except" specific files or folders by name.',
    '- move_filter and move_except move ONLY immediate children of "dir" (not recursive).',
    '- It is OK if you output zero operations when the request is unsafe or impossible.',
    '',
    `Current directory: ${toVirtual(abs)}`,
    'Directory listing (truncated):',
    listing || '(empty)',
    '',
    'Return STRICT JSON only, with this shape:',
    '{ "operations": [ { "op": "mkdir", "path": "..." }, { "op": "move", "from": "...", "to": "..." } ] }',
    'No markdown, no comments, no extra text.'
  ].join('\n');

  try {
    const d = await ollamaFetch('/api/generate', {
      model: MODEL_AGENT,
      prompt: `${sysPrompt}\n\nUser request:\n${prompt.trim()}`,
      format: 'json'
    }, 90_000);
    const raw = (d.response || '').trim();
    let plan;
    try { plan = JSON.parse(raw); }
    catch (e) { return res.status(500).json({ error: 'AI did not return valid JSON', raw }); }
    if (!plan || typeof plan !== 'object')
      return res.status(500).json({ error: 'Invalid plan from AI', raw });
    const ops = Array.isArray(plan.operations) ? plan.operations : [];
    const safeOps = ops
      .filter(op => op && typeof op === 'object' &&
        (op.op === 'mkdir' || op.op === 'move' || op.op === 'move_filter' || op.op === 'move_except'))
      .map(op => {
        if (op.op === 'mkdir') return { op: 'mkdir', path: String(op.path || '') };
        if (op.op === 'move_filter') return { op: 'move_filter', dir: String(op.dir || ''), target: String(op.target || ''), filter: String(op.filter || '') };
        if (op.op === 'move_except') return { op: 'move_except', dir: String(op.dir || ''), target: String(op.target || ''), except: Array.isArray(op.except) ? op.except.map(String) : [] };
        return { op: 'move', from: String(op.from || ''), to: String(op.to || '') };
      })
      .filter(op => {
        if (op.op === 'mkdir') return op.path.startsWith('/');
        if (op.op === 'move') return op.from.startsWith('/') && op.to.startsWith('/');
        if (op.op === 'move_filter') return op.dir.startsWith('/') && op.target.startsWith('/') && !!op.filter;
        if (op.op === 'move_except') return op.dir.startsWith('/') && op.target.startsWith('/') && Array.isArray(op.except);
        return false;
      });
    res.json({ plan: { operations: safeOps }, raw });
  } catch (e) {
    log('error', '[ollama] agent-plan failed', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// Agent: execute a previously returned plan
app.post('/api/ollama/agent-run', async (req, res) => {
  const { plan } = req.body || {};
  const ops = plan && Array.isArray(plan.operations) ? plan.operations : [];
  if (!ops.length) return res.status(400).json({ error: 'No operations to run' });

  const results = [];
  for (const op of ops) {
    if (!op || typeof op !== 'object') continue;
    try {
      if (op.op === 'mkdir') {
        const abs = resolvePath(op.path);
        await fs.promises.mkdir(abs, { recursive: true });
        results.push({ op: 'mkdir', path: op.path, status: 'ok' });
      } else if (op.op === 'move') {
        const fromAbs = resolvePath(op.from);
        const toAbs   = resolvePath(op.to);
        await fs.promises.mkdir(path.dirname(toAbs), { recursive: true });
        let finalToAbs = toAbs;
        try { await fs.promises.access(finalToAbs); finalToAbs = await findFreeName(finalToAbs); } catch {}
        await fs.promises.rename(fromAbs, finalToAbs);
        results.push({ op: 'move', from: op.from, to: toVirtual(finalToAbs), status: 'ok' });
      } else if (op.op === 'move_filter') {
        const dirAbs    = resolvePath(op.dir);
        const targetAbs = resolvePath(op.target);
        await fs.promises.mkdir(targetAbs, { recursive: true });
        const entries = await fs.promises.readdir(dirAbs, { withFileTypes: true });
        let moved = 0, skipped = 0;
        for (const e of entries) {
          const name = e.name;
          const ext = path.extname(name).toLowerCase().replace('.', '');
          const m   = mime.lookup(name) || '';
          const isImg = (m && String(m).startsWith('image/')) || IMAGE_EXTS.has(ext);
          const f = String(op.filter || '');
          const match =
            (f === 'directories'     && e.isDirectory()) ||
            (f === 'files'           && e.isFile()) ||
            (f === 'image_files'     && e.isFile() && isImg) ||
            (f === 'non_image_files' && e.isFile() && !isImg);
          if (!match) { skipped++; continue; }
          let dstAbs = path.join(targetAbs, name);
          dstAbs = await findFreeName(dstAbs);
          await fs.promises.rename(path.join(dirAbs, name), dstAbs);
          moved++;
        }
        results.push({ op: 'move_filter', dir: op.dir, target: op.target, filter: op.filter, moved, skipped, status: 'ok' });
      } else if (op.op === 'move_except') {
        const dirAbs    = resolvePath(op.dir);
        const targetAbs = resolvePath(op.target);
        await fs.promises.mkdir(targetAbs, { recursive: true });
        const exceptNames = (op.except || []).map(n => String(n).toLowerCase());
        const entries = await fs.promises.readdir(dirAbs, { withFileTypes: true });
        let moved = 0, skipped = 0;
        for (const e of entries) {
          if (exceptNames.includes(e.name.toLowerCase())) { skipped++; continue; }
          let dstAbs = path.join(targetAbs, e.name);
          dstAbs = await findFreeName(dstAbs);
          await fs.promises.rename(path.join(dirAbs, e.name), dstAbs);
          moved++;
        }
        results.push({ op: 'move_except', dir: op.dir, target: op.target, except: op.except, moved, skipped, status: 'ok' });
      }
    } catch (e) {
      results.push({ ...op, status: 'error', error: e.message });
    }
  }
  res.json({ results });
});
app.post('/api/ollama/suggest-name', async (req, res) => {
  const { path: p } = req.body;
  const abs = resolvePath(p);

  let content;
  try { content = await extractFileContent(abs); }
  catch (e) { return res.status(400).json({ error: e.message }); }

  if (content.kind === 'error') return res.status(400).json({ error: content.error || 'Cannot read file' });

  const ext   = path.extname(abs);
  const input = content.kind === 'image'
    ? `Image file named: ${content.name}`
    : content.text.slice(0, 6000) || `File: ${content.name}`;

  log('info', '[ollama] suggest-name', { file: content.name, model: MODEL_TEXT });
  try {
    const d = await ollamaFetch('/api/generate', {
      model: MODEL_TEXT,
      prompt: `Suggest ONE short descriptive filename (no extension, use hyphens not spaces, lowercase). Reply with only the filename.\nFile: ${content.name}\nContent: ${input}`
    }, 60_000);
    const raw  = (d.response || '').trim().split('\n')[0]
                   .replace(/[^a-zA-Z0-9_\-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
    res.json({ name: (raw || 'unnamed') + ext });
  } catch (e) {
    log('error', '[ollama] suggest-name failed', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// Tag any file
app.post('/api/ollama/tag', async (req, res) => {
  const { path: p } = req.body;
  const abs = resolvePath(p);

  let content;
  try { content = await extractFileContent(abs); }
  catch (e) { return res.status(400).json({ error: e.message }); }

  if (content.kind === 'error') return res.status(400).json({ error: content.error || 'Cannot read file' });

  const input = content.kind === 'image'
    ? `Image file: ${content.name}`
    : content.text.slice(0, 8000) || `File: ${content.name}`;

  log('info', '[ollama] tag', { file: content.name, model: MODEL_TEXT });
  try {
    const d = await ollamaFetch('/api/generate', {
      model: MODEL_TEXT,
      prompt: `Return 3-6 lowercase single-word tags, comma-separated. Only tags, nothing else.\nFile: ${content.name}\n${input}`
    }, 60_000);
    const tags = (d.response || '').replace(/[^a-z0-9,\- ]/gi, '').split(',')
                   .map(t => t.trim().toLowerCase()).filter(t => t.length > 1).slice(0, 8);
    res.json({ tags });
  } catch (e) {
    log('error', '[ollama] tag failed', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// Describe image (vision model)
app.post('/api/ollama/describe-image', async (req, res) => {
  const { path: p } = req.body;
  const abs  = resolvePath(p);
  const name = path.basename(abs);
  let b64;
  try { b64 = fs.readFileSync(abs).toString('base64'); }
  catch (e) { return res.status(400).json({ error: 'Cannot read image: ' + e.message }); }

  log('info', '[ollama] describe-image', { file: name, model: MODEL_VISION });
  try {
    const d = await ollamaFetch('/api/generate', {
      model:  MODEL_VISION,
      prompt: 'Describe this image in 2-3 sentences. Mention subjects, colors, mood.',
      images: [b64]
    }, 90_000);
    res.json({ description: (d.response || '').trim() });
  } catch (e) {
    log('error', '[ollama] describe-image failed', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// Semantic search with nomic-embed-text
app.post('/api/ollama/search', async (req, res) => {
  const { query, dir } = req.body;
  if (!query || !dir) return res.status(400).json({ error: 'Missing query or dir' });
  const absDir = resolvePath(dir);
  log('info', '[ollama] search', { query, dir, model: MODEL_EMBED });

  let queryEmbed;
  try {
    const qd = await ollamaFetch('/api/embeddings', { model: MODEL_EMBED, prompt: query }, 30_000);
    queryEmbed = qd.embedding;
  } catch (e) { return res.status(500).json({ error: 'Embedding failed: ' + e.message }); }

  let entries;
  try { entries = fs.readdirSync(absDir, { withFileTypes: true }); }
  catch (e) { return res.status(500).json({ error: e.message }); }

  const scored = [];
  for (const entry of entries.filter(e => e.isFile())) {
    const full = path.join(absDir, entry.name);
    const content = await extractFileContent(full).catch(() => null);
    if (!content || content.kind === 'error' || content.kind === 'binary' || !content.text.trim()) continue;
    const textChunk = content.kind === 'image'
      ? `image file: ${entry.name}`
      : content.text.slice(0, 3000);
    try {
      const fd = await ollamaFetch('/api/embeddings', { model: MODEL_EMBED, prompt: textChunk }, 20_000);
      const de = fd.embedding;
      const dot  = queryEmbed.reduce((s, v, i) => s + v * de[i], 0);
      const magQ = Math.sqrt(queryEmbed.reduce((s, v) => s + v*v, 0));
      const magD = Math.sqrt(de.reduce((s, v) => s + v*v, 0));
      scored.push({ name: entry.name, path: toVirtual(full), score: magQ && magD ? dot / (magQ * magD) : 0 });
    } catch { continue; }
  }
  scored.sort((a, b) => b.score - a.score);
  res.json({ results: scored.slice(0, 10) });
});

// ─────────────────────────────────────────────────────────────────
// CATCH-ALL — must be LAST, after every /api/* route is registered
// ─────────────────────────────────────────────────────────────────
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

server.listen(PORT, () => {
  log('info', 'Nova File Manager started', { port: PORT, root: ROOT || '/', pid: process.pid, node: process.version });
});

const express    = require('express');
const http       = require('http');
const WebSocket  = require('ws');
const path       = require('path');
const fs         = require('fs');
const { exec, execFile } = require('child_process');
const archiver = require('archiver');
const mime     = require('mime-types');
const { v4: uuidv4 } = require('uuid');
const sqlite3  = require('sqlite3').verbose();
const fileUpload = require('express-fileupload');
const crypto     = require('crypto');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

// ─────────────────────────────────────────────────────────────────
// LOGGER — clean structured output, easy to read in any log viewer
// Set LOG_LEVEL=debug|info|warn|error  (default: info)
// Set LOG_FORMAT=pretty|json           (default: pretty)
// ─────────────────────────────────────────────────────────────────
const LOG_LEVEL  = (process.env.LOG_LEVEL  || 'info').toLowerCase();
const LOG_FORMAT = (process.env.LOG_FORMAT || 'pretty').toLowerCase();
const LEVELS     = { debug: 0, info: 1, warn: 2, error: 3 };
const lvlNum     = () => LEVELS[LOG_LEVEL] ?? 1;

// ANSI colour helpers (stripped automatically when not a TTY)
const isTTY = process.stdout.isTTY;
const C = {
  reset:  isTTY ? '\x1b[0m'  : '',
  bold:   isTTY ? '\x1b[1m'  : '',
  dim:    isTTY ? '\x1b[2m'  : '',
  blue:   isTTY ? '\x1b[34m' : '',
  cyan:   isTTY ? '\x1b[36m' : '',
  green:  isTTY ? '\x1b[32m' : '',
  yellow: isTTY ? '\x1b[33m' : '',
  red:    isTTY ? '\x1b[31m' : '',
  magenta:isTTY ? '\x1b[35m' : '',
};

const LEVEL_STYLE = {
  debug: { color: C.cyan,   label: 'DEBUG' },
  info:  { color: C.green,  label: 'INFO ' },
  warn:  { color: C.yellow, label: 'WARN ' },
  error: { color: C.red,    label: 'ERROR' },
};

function log(level, msg, meta = {}) {
  if ((LEVELS[level] ?? 1) < lvlNum()) return;

  const ts  = new Date().toISOString();
  const out = level === 'error' ? process.stderr : process.stdout;

  if (LOG_FORMAT === 'json') {
    // Compact JSON — one line, machine-parseable
    out.write(JSON.stringify({ time: ts, level, msg, ...meta }) + '\n');
    return;
  }

  // ── Pretty format ────────────────────────────────────────────────
  const { color, label } = LEVEL_STYLE[level] || LEVEL_STYLE.info;
  const time = `${C.dim}${ts.slice(11, 23)}${C.reset}`;           // HH:MM:SS.mmm
  const lvl  = `${color}${C.bold}${label}${C.reset}`;
  const text = `${C.bold}${msg}${C.reset}`;


  // Format meta fields inline: key=value key2=value2
  const metaStr = Object.entries(meta)
    .filter(([, v]) => v !== undefined && v !== null && v !== '')
    .map(([k, v]) => {
      const val = typeof v === 'object' ? JSON.stringify(v) : String(v);
      return `${C.dim}${k}${C.reset}=${C.cyan}${val}${C.reset}`;
    })
    .join('  ');

  const line = [time, lvl, text, metaStr].filter(Boolean).join('  ');
  out.write(line + '\n');
}

// Shortcuts
const logDebug = (msg, meta) => log('debug', msg, meta);

// ─────────────────────────────────────────────────────────────────
// PROCESS HEALTH — catch anything that would silently crash/hang
// ─────────────────────────────────────────────────────────────────
process.on('uncaughtException', (err, origin) => {
  log('error', 'UNCAUGHT EXCEPTION — process may be unstable. Shutting down in 1s.', {
    origin, error: err.message, stack: err.stack ? err.stack.split('\n').slice(0,6).join(' | ') : ''
  });
  // Perform emergency flush and exit. Standard best practice for stability.
  setTimeout(() => process.exit(1), 1000);
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
  
  // High-uptime Memory Leak Prevention: Purge in-memory jobs that completed >1hr ago.
  // They are safely persisted in SQLite if needed by /api/jobs history.
  const now = Date.now();
  for (const [id, job] of jobs.entries()) {
    if (job.status !== 'running' && job.finishedAt && (now - new Date(job.finishedAt).getTime() > 60 * 60 * 1000)) {
      jobs.delete(id);
    }
  }

  // High-uptime Storage Leak Prevention: DB keeps 7 days history
  run(`DELETE FROM jobs WHERE status != 'running' AND startedAt < datetime('now', '-7 days')`).catch(()=>{});

  const running = [...jobs.values()].filter(j => j.status === 'running');
  log('debug', 'Memory snapshot', {
    rss_mb:    Math.round(mem.rss      / 1024 / 1024),
    heap_mb:   Math.round(mem.heapUsed / 1024 / 1024),
    tracking_jobs: jobs.size,
    running_jobs: running.length,
    uptime_s:  Math.round(process.uptime())
  });
}, 60_000).unref();

const ROOT = path.resolve(process.env.ROOT_PATH || '/');
const PORT = process.env.PORT || 9898;
const TRASH_DIR = path.join(ROOT, '.nova_trash');

// Gotify config (optional)
const GOTIFY_URL   = (process.env.GOTIFY_URL   || '').trim();
const GOTIFY_TOKEN = (process.env.GOTIFY_TOKEN || '').trim();

// Database initialization
const dbFile = path.join(ROOT, '.nova.db');
const db = new sqlite3.Database(dbFile);

// Enable WAL (Write-Ahead Logging) for significantly better concurrency
db.serialize(() => {
  db.run("PRAGMA journal_mode=WAL");
  db.run("PRAGMA synchronous=NORMAL");
});

// Helper for async queries
const query = (sql, params = []) => new Promise((resolve, reject) => {
  db.all(sql, params, (err, rows) => { if (err) reject(err); else resolve(rows); });
});
const run = (sql, params = []) => new Promise((resolve, reject) => {
  db.run(sql, params, function(err) { if (err) reject(err); else resolve(this); });
});

(async () => {
  try {
    await run(`CREATE TABLE IF NOT EXISTS trash (
      id TEXT PRIMARY KEY,
      name TEXT,
      originalPath TEXT,
      trashPath TEXT,
      deletedAt TEXT
    )`);
    await run(`CREATE TABLE IF NOT EXISTS embeddings (
      filePath TEXT,
      mtime INTEGER,
      model TEXT,
      embedding TEXT,
      PRIMARY KEY (filePath, mtime, model)
    )`);
    await run(`CREATE TABLE IF NOT EXISTS jobs (
      jobId TEXT PRIMARY KEY,
      clientId TEXT,
      type TEXT,
      status TEXT,
      percent INTEGER,
      phase TEXT,
      logs TEXT,
      error TEXT,
      data TEXT,
      startedAt TEXT,
      finishedAt TEXT
    )`);

    await run(`CREATE TABLE IF NOT EXISTS users (
      username TEXT PRIMARY KEY,
      password TEXT
    )`);
    
    // Performance indexes
    await run(`CREATE INDEX IF NOT EXISTS idx_embeddings_path ON embeddings(filePath)`);
    await run(`CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)`);
    await run(`CREATE INDEX IF NOT EXISTS idx_jobs_started ON jobs(startedAt DESC)`);

    // ── CREDENTIALS ──────────────────────────────────────────
    // MANDATORY: ADMIN_USER and ADMIN_PASS. If not set, generate a random one and log it, but warn.
    const existing = await query(`SELECT * FROM users LIMIT 1`);
    if (!existing.length) {
      const ADMIN_USER = process.env.ADMIN_USER || 'admin';
      const ADMIN_PASS = process.env.ADMIN_PASS || uuidv4().slice(0, 12);
      
      log('warn', '⚠️  INITIALIZING SECURE CREDENTIALS — PLEASE CHECK LOGS OR SET IN .env');
      log('info', `### NOVA CREDENTIALS: user=${ADMIN_USER} pass=${ADMIN_PASS} ###`);
      
      const salt = crypto.randomBytes(16).toString('hex');
      const hash = crypto.scryptSync(ADMIN_PASS, salt, 64).toString('hex');
      await run(`INSERT INTO users (username, password) VALUES (?, ?)`, [ADMIN_USER, `${salt}.${hash}`]);
    }

    // ── RESUME/RESTORE JOBS ────────────────────────────────────
    // Mark any interrupted 'running' jobs as 'cancelled' so they can be resumed
    await run(`UPDATE jobs SET status = 'cancelled' WHERE status = 'running'`);
    log('info', 'Recovered job states from database');
    
    // Seed in-memory jobs map with recent history
    const recent = await query(`SELECT * FROM jobs ORDER BY startedAt DESC LIMIT 50`);
    recent.forEach(r => {
      jobs.set(r.jobId, {
        ...r,
        log: JSON.parse(r.logs || '[]'),
        data: JSON.parse(r.data || '{}')
      });
    });
    
    // Migration: Migrate .meta.json to trash table (atomic startup migration)
    const metaFile = path.join(TRASH_DIR, '.meta.json');
    try {
      await fs.promises.access(metaFile);
      const meta = JSON.parse(await fs.promises.readFile(metaFile, 'utf8'));
      log('info', `Migrating ${meta.length} trash items from .meta.json to database…`);
      for (const item of meta) {
        await run(`INSERT OR IGNORE INTO trash (id, name, originalPath, trashPath, deletedAt) VALUES (?, ?, ?, ?, ?)`, 
                  [item.id, item.name, item.originalPath, item.trashPath, item.deletedAt]);
      }
      await fs.promises.rename(metaFile, metaFile + '.bak');
      log('info', 'Trash migration complete.');
    } catch (e) {
      if (e.code !== 'ENOENT') log('error', 'Trash migration failed', { error: e.message });
    }
  } catch (e) { log('error', 'Database initialization failed', { error: e.message }); }
})();

// Ensure TRASH_DIR (async startup check)
(async () => {
  try { await fs.promises.access(TRASH_DIR); }
  catch (e) { await fs.promises.mkdir(TRASH_DIR, { recursive: true }).catch(() => {}); }
})();

// ── Security Headers & CSRF ────────────────────────────────────
const CSRF_SECRET = process.env.CSRF_SECRET || (() => {
  log('warn', '⚠️  CSRF_SECRET not set in environment — generating volatile token (sessions will expire on restart)');
  return crypto.randomBytes(32).toString('hex');
})();
const getCsrfToken = (ip, salt) => crypto.createHmac('sha256', CSRF_SECRET).update(`${ip}:${salt}`).digest('hex');

// Authentication & Session Secrets
const AUTH_SECRET = process.env.AUTH_SECRET || (() => {
  log('warn', '⚠️  AUTH_SECRET not set in environment — generating volatile token (users will be logged out on restart)');
  return crypto.randomBytes(32).toString('hex');
})();
const SESSION_COOKIE = 'nova_session';

// ── Authentication & Cookie Helpers ──────────────────────────────
/** Centralized cookie parser: handles multiple cookies and keeps values with '=' intact */
function parseCookies(cookieHeader) {
  return (cookieHeader || '').split(';').reduce((acc, c) => {
    const idx = c.indexOf('=');
    if (idx !== -1) acc[c.slice(0, idx).trim()] = c.slice(idx + 1);
    return acc;
  }, {});
}

/** Verifies session and returns user info (works for both Express and Node native requests) */
function getAuthUser(req) {
  const cookies = parseCookies(req.headers.cookie);
  return verifySession(cookies[SESSION_COOKIE]);
}


const hashPassword = (pw) => {
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(pw, salt, 64).toString('hex');
  return `${salt}.${hash}`;
};

const verifyPassword = (pw, combined) => {
  if (!combined || !combined.includes('.')) return false;
  const [salt, originalHash] = combined.split('.');
  const candidateHash = crypto.scryptSync(pw, salt, 64).toString('hex');
  return candidateHash === originalHash;
};

const signSession = (username) => {
  const payload = JSON.stringify({ u: username, t: Date.now() });
  const sig = crypto.createHmac('sha256', AUTH_SECRET).update(payload).digest('hex');
  return `${Buffer.from(payload).toString('base64')}.${sig}`;
};

const verifySession = (token) => {
  if (!token || !token.includes('.')) return null;
  const [b64, sig] = token.split('.');
  const payloadStr = Buffer.from(b64, 'base64').toString('utf8');
  const expectedSig = crypto.createHmac('sha256', AUTH_SECRET).update(payloadStr).digest('hex');
  if (sig !== expectedSig) return null;
  const payload = JSON.parse(payloadStr);
  // Optional: check expiration (e.g., 30 days max for 'remember me')
  if (Date.now() - payload.t > 30 * 24 * 60 * 60 * 1000) return null;
  return payload.u;
};

app.use((req, res, next) => {
  // 1. Basic Security Headers
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'same-origin');
  
  // 2. CSRF Implementation
  // We use a simple cookie + header approach. 
  // The client reads the 'nova_csrf_token' cookie and sends it back in 'X-Nova-CSRF' header.
  if (!req.headers.cookie || !req.headers.cookie.includes('nova_csrf_token=')) {
    const salt = crypto.randomBytes(16).toString('hex');
    const token = getCsrfToken(req.ip, salt);
    const cookieVal = `${salt}.${token}`;
    res.setHeader('Set-Cookie', `nova_csrf_token=${cookieVal}; Path=/; SameSite=Lax`);
  }

  // Validate CSRF for state-changing methods
  const protectedMethods = ['POST', 'PUT', 'DELETE', 'PATCH'];
  if (protectedMethods.includes(req.method)) {
    const clientHeader = req.headers['x-nova-csrf'];
    const cookies = parseCookies(req.headers.cookie);
    const serverCookie = cookies['nova_csrf_token'];
    
    if (!clientHeader || !serverCookie || clientHeader !== serverCookie) {
      log('warn', 'CSRF validation failed', { 
        method: req.method, path: req.path, ip: req.ip,
        hasHeader: !!clientHeader, hasCookie: !!serverCookie
      });
      return res.status(403).json({ error: 'CSRF validation failed' });
    }
  }
  next();
});

const checkAuth = (req, res, next) => {
  // Allow login/logout/health and public static assets without auth
  const publicPaths = ['/api/login', '/api/logout', '/api/health'];
  const isPublicAsset = req.path.match(/\.(png|svg|ico|json|js)$/) || req.path === '/favicon.svg';
  
  if (publicPaths.includes(req.path) || isPublicAsset) return next();
  
  const user = getAuthUser(req);
  if (!user) {
    if (req.path.startsWith('/api/')) {
      return res.status(401).json({ error: 'Authentication required' });
    }
    // Serve login page for the main UI if not authenticated
    return res.sendFile(path.join(__dirname, 'login.html'));
  }
  req.user = user;
  next();
};

app.use(checkAuth);

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
async function resolvePath(p, followSymlinks = false) {
  const normalized = path.normalize('/' + (p || '/')).replace(/\/\/+/g, '/');
  let currentAbs = path.join(ROOT, normalized);

  // Path traversal guard: resolved path must still start with ROOT
  if (ROOT && !currentAbs.startsWith(ROOT)) {
    throw new Error(`Path traversal rejected: ${p}`);
  }

  // Symlink escape protection (async manual walk for Docker safety)
  if (followSymlinks && ROOT) {
    let curr = '/';
    const parts = currentAbs.split('/').filter(Boolean);
    for (let i = 0; i < parts.length; i++) {
        curr = path.join(curr, parts[i]);
        let maxLinks = 40; // Prevention against infinite cyclic symlink loops
        while (maxLinks-- > 0) {
            try {
                const st = await fs.promises.lstat(curr);
                if (st.isSymbolicLink()) {
                    const target = await fs.promises.readlink(curr);
                    // Host-root reconciliation for absolute symlinks inside unprivileged Docker
                    if (path.isAbsolute(target)) curr = path.join(ROOT, target);
                    else curr = path.resolve(path.dirname(curr), target);
                    
                    if (!curr.startsWith(ROOT)) throw new Error(`Symlink escape rejected: ${curr}`);
                } else break;
            } catch (e) {
                // If a component is genuinely broken/missing, bubble the security breach but otherwise ignore standard ENOENT failures (they'll fail accurately at Native 'fs' levels later)
                if (e.message && e.message.includes('Symlink escape')) throw e;
                const remaining = parts.slice(i+1).join('/');
                curr = remaining ? path.join(curr, remaining) : curr;
                i = parts.length; // Abort structural iteration, it's natively unreachable gracefully
                break;
            }
        }
    }
    
    currentAbs = curr;
    if (!currentAbs.startsWith(ROOT)) {
        throw new Error(`Symlink escape rejected: ${p} -> ${currentAbs}`);
    }
  }

  return currentAbs.length > ROOT.length + 1 && currentAbs.endsWith('/')
    ? currentAbs.replace(/\/$/, '') : currentAbs;
}
const isSubpath = (parent, child) => {
  const relative = path.relative(parent, child);
  return relative === '' || (relative && !relative.startsWith('..') && !path.isAbsolute(relative));
};
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

wss.on('connection', (ws, req) => {
  const cookies = parseCookies(req.headers.cookie);
  const user = verifySession(cookies[SESSION_COOKIE]);
  const csrfCookie = cookies['nova_csrf_token'];

  // 1. Session check
  if (!user) {
    log('warn', 'Unauthorized WS connection rejected (no session)', { ip: req.socket.remoteAddress });
    ws.close(1008, 'Authentication required');
    return;
  }

  // 2. CSWSH / CSRF check
  // The client must pass the CSRF token in the query string because browsers don't send custom headers on WS upgrades.
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const clientToken = url.searchParams.get('token');

  if (!csrfCookie || !clientToken || csrfCookie !== clientToken) {
    log('warn', 'CSWSH / CSRF validation failed for WS connection', { 
      ip: req.socket.remoteAddress, 
      hasCookie: !!csrfCookie, 
      hasToken: !!clientToken,
      match: csrfCookie === clientToken
    });
    ws.close(1008, 'CSRF validation failed');
    return;
  }

  let clientId = null;
  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      if (msg.type === 'register' && msg.clientId) {
        clientId = msg.clientId;
        // If this clientId already has a connection, terminate the old one (last one wins)
        if (clients.has(clientId)) {
          const oldWs = clients.get(clientId);
          if (oldWs !== ws) {
            try { oldWs.terminate(); } catch (e) { /* ignore */ }
          }
        }
        clients.set(clientId, ws);
        ws.clientId = clientId;

        // Replay all running jobs for this client on reconnect
        for (const job of jobs.values()) {
          if (job.clientId === clientId && job.status === 'running') {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify({ type: 'job_update', job: sanitizeJob(job) }));
            }
          }
        }
      }
    } catch (e) {
      logDebug('WS message parse failed', { error: e.message, data: String(data).slice(0, 64) });
    }
  });

  ws.on('close', () => {
    // Only delete from clients map if THIS specific socket is the one registered
    if (clientId && clients.get(clientId) === ws) {
      clients.delete(clientId);
    }
  });
});

function broadcast(clientId, data) {
  const ws = clients.get(clientId);
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(data)); } catch (e) { logDebug('WS broadcast failed', { clientId, error: e.message }); }
  }
}

// ─────────────────────────────────────────────────────────────────
// JOB STORE  — lives in the Node process, survives browser close
// ─────────────────────────────────────────────────────────────────
const jobs = new Map();

// ── AUTH ROUTES ────────────────────────────────────────────────
app.post('/api/login', async (req, res) => {
  const { username, password, remember } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Missing credentials' });

  const rows = await query(`SELECT * FROM users WHERE username = ?`, [username]);
  if (!rows.length || !verifyPassword(password, rows[0].password)) {
    log('warn', 'Login failed', { username, ip: req.ip });
    return res.status(401).json({ error: 'Invalid username or password' });
  }

  const token = signSession(username);
  const maxAge = remember ? 30 * 24 * 60 * 60 * 1000 : null; // 30 days
  const cookieOpts = `Path=/; SameSite=Lax; HttpOnly${maxAge ? `; Max-Age=${maxAge / 1000}` : ''}`;
  res.setHeader('Set-Cookie', `${SESSION_COOKIE}=${token}; ${cookieOpts}`);
  
  log('info', 'User logged in', { username, remember, ip: req.ip });
  res.json({ success: true, user: username });
});

app.post('/api/logout', async (req, res) => {
  res.setHeader('Set-Cookie', `${SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax`);
  res.json({ success: true });
});

app.post('/api/account/update', checkAuth, async (req, res) => {
  const { newUsername, newPassword } = req.body;
  if (!newUsername && !newPassword) return res.status(400).json({ error: 'Nothing to update' });

  const currentUsername = req.user;
  try {
    if (newUsername && newPassword) {
      await run(`UPDATE users SET username = ?, password = ? WHERE username = ?`, [newUsername, hashPassword(newPassword), currentUsername]);
      const token = signSession(newUsername);
      res.setHeader('Set-Cookie', `${SESSION_COOKIE}=${token}; Path=/; SameSite=Lax; HttpOnly`);
    } else if (newUsername) {
      await run(`UPDATE users SET username = ? WHERE username = ?`, [newUsername, currentUsername]);
      const token = signSession(newUsername);
      res.setHeader('Set-Cookie', `${SESSION_COOKIE}=${token}; Path=/; SameSite=Lax; HttpOnly`);
    } else if (newPassword) {
      await run(`UPDATE users SET password = ? WHERE username = ?`, [hashPassword(newPassword), currentUsername]);
    }

    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

function createJob(type, clientId, meta = {}) {
  const jobId = uuidv4();
  const job   = {
    jobId, type, clientId,
    status: 'running', percent: 0, filePercent: 0, lastSyncedPercent: 0,
    phase: 'starting', currentFile: '', log: [],
    startedAt: new Date().toISOString(), finishedAt: null,
    ...meta
  };
  jobs.set(jobId, job);
  
  // Persist to DB
  run(`INSERT INTO jobs (jobId, clientId, type, status, percent, phase, logs, error, data, startedAt, finishedAt)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [jobId, clientId, type, job.status, job.percent, job.phase, JSON.stringify(job.log), job.error || null, JSON.stringify(meta), job.startedAt, job.finishedAt])
      .catch(e => log('error', 'Failed to persist job creation', { jobId, error: e.message }));

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

  // Sync to DB on major updates or periodically
  const isFinal = ['done', 'error', 'cancelled'].includes(updates.status);
  if (isFinal) {
    run(`UPDATE jobs SET status = ?, percent = ?, phase = ?, logs = ?, error = ?, finishedAt = ? WHERE jobId = ?`,
        [job.status, job.percent, job.phase, JSON.stringify(job.log), job.error || null, job.finishedAt || new Date().toISOString(), job.jobId])
        .catch(e => log('error', 'Failed to persist job final state', { jobId: job.jobId, error: e.message }));
  } else if (updates.percent !== undefined && Math.abs(updates.percent - (job.lastSyncedPercent || 0)) >= 5) {
    // Throttled sync for progress — sync every 5% jump
    job.lastSyncedPercent = updates.percent;
    run(`UPDATE jobs SET percent = ?, phase = ?, logs = ? WHERE jobId = ?`,
        [job.percent, job.phase, JSON.stringify(job.log), job.jobId])
        .catch(e => logDebug('Job progress persist throttled fail', { jobId: job.jobId, error: e.message }));
  }

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

function addJobLog(job, msg) {
  if (!job.log) job.log = [];
  job.log.push(msg);
  if (job.log.length > 100) job.log.shift(); // Constant memory cap
}

function sanitizeJob(job) {
  return {
    jobId: job.jobId, type: job.type, status: job.status,
    percent: job.percent, filePercent: job.filePercent,
    phase: job.phase, currentFile: job.currentFile,
    startedAt: job.startedAt, finishedAt: job.finishedAt,
    operation: job.operation, sources: job.sources,
    destination: job.destination, error: job.error,
    // Truncate logs for real-time updates to minimize network load
    log: (job.log || []).slice(-20)
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
app.get('/api/jobs', async (req, res) => {
  try {
    const historical = await query(`SELECT * FROM jobs ORDER BY startedAt DESC LIMIT 200`);
    const formatted = historical.map(row => {
      // If the job is currently active in memory, prefer the memory state
      if (jobs.has(row.jobId)) return sanitizeJob(jobs.get(row.jobId));
      
      let data = {}; try { data = JSON.parse(row.data); } catch(e) { logDebug('Job data parse failed', { jobId: row.jobId, error: e.message }); }
      let log  = []; try { log  = JSON.parse(row.logs); } catch(e) { logDebug('Job logs parse failed', { jobId: row.jobId, error: e.message }); }
      
      return {
        ...data,
        jobId: row.jobId, type: row.type, clientId: row.clientId,
        status: row.status, percent: row.percent, phase: row.phase,
        error: row.error, startedAt: row.startedAt, finishedAt: row.finishedAt,
        log: log.slice(-50)
      };
    });
    res.json({ jobs: formatted });
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.get('/api/health', async (req, res) => res.json({
  status: 'ok',
  uptime: Math.round(process.uptime()),
  runningJobs: [...jobs.values()].filter(j => j.status === 'running').length,
  pid: process.pid
}));

app.get('/api/list', async (req, res) => {
  try {
    const showHidden = (req.query.showHidden === 'true');
    const dir = await resolvePath(req.query.path || '/', true); 
    const entries = await fs.promises.readdir(dir, { withFileTypes: true });
    
    const filePromises = entries
      .filter(e => {
        if (e.name === '.nova_trash') return false;
        if (e.name === '.nova.db' || e.name.startsWith('.nova_edit_') || e.name.startsWith('.nova_xfr_')) return false;
        if (!showHidden && e.name.startsWith('.') && e.name !== '..') return false;
        return true;
      })
      .map(async (e) => {
        const fullPath = path.join(dir, e.name);
        const isSymlink = e.isSymbolicLink();

        let lstStat = {}, tgtStat = null, linkTarget = null, brokenLink = false;
        try { lstStat = await fs.promises.lstat(fullPath); } catch (e2) { logDebug('lstat failed', { path: fullPath, error: e2.message }); }

        if (isSymlink) {
          try { linkTarget = await fs.promises.readlink(fullPath); } catch (e2) { /* ignore */ }
          try {
            tgtStat = await fs.promises.stat(fullPath); 
          } catch {
            // Try host-root reconciliation for broken/absolute links
            if (linkTarget && linkTarget.startsWith('/') && !linkTarget.startsWith(ROOT)) {
              const hostTarget = path.join(ROOT, linkTarget);
              try {
                tgtStat = await fs.promises.stat(hostTarget);
                brokenLink = false;
              } catch { brokenLink = true; }
            } else {
              brokenLink = true;
            }
          }
        }

        const statToUse = tgtStat || lstStat;
        const isDir  = isSymlink ? (!brokenLink && tgtStat?.isDirectory()) : e.isDirectory();
        const type   = isSymlink ? (brokenLink ? 'symlink-broken' : isDir ? 'symlink-dir' : 'symlink-file') :
                       e.isDirectory() ? 'directory' : 'file';

        return {
          name:        e.name,
          path:        toVirtual(fullPath),
          type,
          isSymlink,
          linkTarget:  linkTarget || null,
          brokenLink:  brokenLink || false,
          size:        statToUse.size  || 0,
          modified:    statToUse.mtime || null,
          mime:        isDir ? null : (mime.lookup(isSymlink ? (linkTarget || e.name) : e.name) || 'application/octet-stream'),
          permissions: lstStat.mode ? (lstStat.mode & 0o777).toString(8) : '---'
        };
      });
    const files = await Promise.all(filePromises);
    res.json({ path: toVirtual(dir), entries: files });
  } catch (err) { 
    res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message }); 
  }
});

app.get('/api/trash', async (req, res) => {
  try {
    const items = await query(`SELECT * FROM trash ORDER BY deletedAt DESC`);
    res.json({ items });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/diskinfo', async (req, res) => {
  const target = ROOT || '/';
  execFile('df', ['-B1', target], (err, stdout) => {
    if (err) return res.status(500).json({ error: err.message });
    const lines = stdout.trim().split('\n');
    if (lines.length < 2) return res.status(500).json({ error: 'Unexpected df output' });
    const cols = lines[1].trim().split(/\s+/);
    if (cols.length < 4) return res.status(500).json({ error: 'Invalid df output format' });
    const [total, used, avail] = [cols[1], cols[2], cols[3]].map(Number);
    res.json({ total, used, avail });
  });
});



// ── Mounted drives — STRICT allowlist: only real block/network devices ──
// Strategy:
//   1. Allowlist: device must start with /dev/ OR be a network filesystem
//   2. Skip filesystem types that are never real storage
//   3. Skip mountpoints that look like Docker bind-mounted single files
//   4. Skip mountpoints with container hash names (64-char hex)
//   5. Deduplicate: run df and group by the REAL underlying device df reports —
//      if /home, /srv, /tmp all sit on the same /dev/sda1 they become one entry
//      and we pick the most descriptive mountpoint (prefer /mnt, /media, shortest)
app.get('/api/mounts', async (req, res) => {
  const NETWORK_FS = new Set(['nfs','nfs4','cifs','smbfs','davfs','fuse.sshfs',
    'fuse.rclone','fuse.encfs','fuse.s3fs','glusterfs','cephfs','lustre','beegfs']);
  const SKIP_FS    = new Set(['proc','sysfs','devtmpfs','devpts','tmpfs','cgroup',
    'cgroup2','pstore','securityfs','debugfs','tracefs','configfs','fusectl',
    'hugetlbfs','mqueue','bpf','rpc_pipefs','nfsd','autofs','efivarfs',
    'overlay','squashfs','ramfs','9p','virtiofs','fuse.lxcfs','nsfs',
    'binfmt_misc','mqueue','sunrpc']);
  // Mountpoints that look like Docker bind-mounted single files (contain a dot but no slash after)
  const FILE_MOUNT  = /\/[^/]+\.[^/]+$/;
  // Container hash-style names (12 or 64 hex chars as a path component)
  const HASH_MOUNT  = /\/[0-9a-f]{12,64}(\/|$)/;
  const SKIP_MPS    = new Set(['/','/boot/efi','/boot/esp','/hostroot','/hostroot/boot','/hostroot/boot/efi',
                               '/etc/hosts','/etc/hostname','/etc/resolv.conf',
                               '/hostroot/etc/hosts','/hostroot/etc/hostname','/hostroot/etc/resolv.conf']);
  const SKIP_PFXS   = ['/proc','/sys','/dev','/run/user','/run/credentials','/snap','/var/lib/docker',
                       '/hostroot/proc','/hostroot/sys','/hostroot/dev','/hostroot/run/user','/hostroot/snap'];

  fs.readFile('/proc/mounts', 'utf8', (err, data) => {
    if (err) return res.json({ mounts: [] });

    const raw = [];
    for (const line of data.trim().split('\n')) {
      const parts = line.split(' ');
      const device = parts[0], mountpoint = parts[1], fstype = parts[2];
      if (!device || !mountpoint || !fstype) continue;
      if (SKIP_FS.has(fstype)) continue;
      if (SKIP_MPS.has(mountpoint)) continue;
      if (SKIP_PFXS.some(p => mountpoint.startsWith(p))) continue;
      if (FILE_MOUNT.test(mountpoint)) continue;  // e.g. /etc/resolv.conf
      if (HASH_MOUNT.test(mountpoint)) continue;  // container hashes
      // ALLOWLIST: real block device or known network fs
      const isBlock   = device.startsWith('/dev/');
      const isNetwork = NETWORK_FS.has(fstype);
      if (!isBlock && !isNetwork) continue;
      raw.push({ device, mountpoint, fstype });
    }

    if (!raw.length) return res.json({ mounts: [] });

    // Command injection protection: use execFile with an argument array
    const args = ['-B1', ...raw.map(m => m.mountpoint)];
    
    execFile('df', args, (dfErr, dfOut) => {
      // df throws exit code 1 if even a single mount restricts stat permissions, but valid outputs are still processed cleanly to dfOut
      if (!dfOut || dfOut.trim() === '') return res.json({ mounts: [] });

      const byDevice = new Map();
      const lines = dfOut.trim().split('\n').slice(1); // skip header
      for (const line of lines) {
        if (!line.trim()) continue;
        const cols = line.trim().split(/\s+/);
        if (cols.length < 6) continue;
        const dfDev = cols[0];
        const total = +cols[1];
        const used  = +cols[2];
        const avail = +cols[3];
        const mp    = cols.slice(5).join(' '); // Reconstruct mountpoint with spaces
        
        if (!total || total < 1) continue;
        const meta = raw.find(r => r.mountpoint === mp) || {};
        const entry = { device: dfDev, mountpoint: mp, fstype: meta.fstype || '',
                        total, used, avail };
                        
        if (!byDevice.has(dfDev)) {
          byDevice.set(dfDev, entry);
        } else {
          const prev = byDevice.get(dfDev);
          const score = mp => (mp.startsWith('/mnt')||mp.startsWith('/media')) ? 0 : mp.length;
          if (score(mp) < score(prev.mountpoint)) byDevice.set(dfDev, entry);
        }
      }

      const mounts = [...byDevice.values()]
        .filter(m => m.total > 0)
        .map(m => ({
          ...m,
          mountpoint: toVirtual(m.mountpoint),
          label: m.mountpoint.split('/').filter(Boolean).pop()
                 || path.basename(m.device) || m.device
        }))
        .sort((a,b) => b.total - a.total);

      res.json({ mounts });
    });
  });
});
app.get('/api/stat', async (req, res) => {
  try {
    const p = await resolvePath(req.query.path, true);
    const lst = await fs.promises.lstat(p); // info about link itself
    const isSymlink = lst.isSymbolicLink();
    let stat = lst, linkTarget = null, brokenLink = false;
    if (isSymlink) {
      try { linkTarget = await fs.promises.readlink(p); } catch (e) { logDebug('readlink failed', { path: p, error: e.message }); }
      try {
        stat = await fs.promises.stat(p); // follow to target
        if (ROOT && !stat.isDirectory()) {
          // Verify that the link target is also inside ROOT
          const real = await fs.promises.realpath(p);
          if (!real.startsWith(ROOT)) throw new Error(`Forbidden: Symlink escape rejected`);
        }
      } catch (e) {
        if (e.message.includes('Forbidden')) throw e;
        brokenLink = true; stat = lst;
      }
    }
    res.json({
      name:        path.basename(p),
      path:        toVirtual(p),
      size:        stat.size,
      modified:    stat.mtime,
      created:     stat.birthtime,
      permissions: (lst.mode & 0o777).toString(8),
      isDirectory: stat.isDirectory(),
      isSymlink,
      linkTarget:  linkTarget || null,
      brokenLink:  brokenLink || false,
      mime:        mime.lookup(p) || null
    });
  } catch (err) { res.status(err.message.includes('Symlink escape') || err.message.includes('traversal') ? 403 : err.code === 'ENOENT' ? 404 : 500).json({ error: err.message }); }
});

app.get('/api/preview', async (req, res) => {
  try {
    const p = await resolvePath(req.query.path, true); // true = follow and verify symlink
    res.setHeader('Content-Type', mime.lookup(p) || 'application/octet-stream');
    fs.createReadStream(p).pipe(res);
  } catch (err) {
    res.status(err.message.includes('Symlink escape') || err.message.includes('traversal') ? 403 : 404).send(err.message);
  }
});

app.get('/api/download', async (req, res) => {
  try {
    const p = await resolvePath(req.query.path, true);
    let stat;
    try { stat = await fs.promises.stat(p); } catch (e) { return res.status(404).json({ error: e.message }); }
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
  } catch (err) { res.status(err.message.includes('Symlink escape') ? 403 : 404).json({ error: err.message }); }
});

// ─────────────────────────────────────────────────────────────────
// ROUTES — instant ops (these are atomic/fast enough to be sync)
// ─────────────────────────────────────────────────────────────────
app.post('/api/mkdir', async (req, res) => {
  try {
    const { path: p, name } = req.body;
    if (!name || name.includes('/') || name.includes('\\') || name === '.' || name === '..')
      return res.status(400).json({ error: 'Invalid folder name' });
    const parent = await resolvePath(p, true);
    const full = path.join(parent, name);
    await fs.promises.mkdir(full, { recursive: true });
    log('info', 'Directory created', { path: toVirtual(full), clientId: (req.body.clientId || '').slice(0, 12) });
    res.json({ success: true });
  } catch (err) { res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message }); }
});

app.post('/api/rename', async (req, res) => {
  try {
    const { oldPath, newName } = req.body;
    if (!newName || newName.includes('/') || newName.includes('\\') || newName === '.' || newName === '..')
      return res.status(400).json({ error: 'Invalid name' });
    
    const abs = await resolvePath(oldPath, true);
    const dir = path.dirname(abs);
    const oldName = path.basename(abs);
    let dest = path.join(dir, newName);

    // Conflict resolution: if destination exists and it's not a case-only rename, find a free name
    if (fs.existsSync(dest) && oldName.toLowerCase() !== newName.toLowerCase()) {
      dest = await findFreeName(dest);
    }

    // case-only rename trick (a.txt -> A.txt) for case-insensitive filesystems
    try {
      if (oldName.toLowerCase() === newName.toLowerCase() && oldName !== newName) {
        const tmp = dest + '.' + uuidv4().slice(0, 8) + '.tmp';
        await fs.promises.rename(abs, tmp);
        await fs.promises.rename(tmp, dest);
      } else {
        await fs.promises.rename(abs, dest);
      }
    } finally {
      releaseName(dest);
    }
    
    log('info', 'Renamed/Moved', { from: toVirtual(abs), to: toVirtual(dest), clientId: (req.body.clientId || '').slice(0, 12) });
    res.json({ success: true, newName: path.basename(dest) });
  } catch (err) { res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message }); }
});

app.post('/api/chmod', async (req, res) => {
  try {
    const { path: p, mode } = req.body;
    const abs = await resolvePath(p, true);
    
    // Security Guard: refusing chmod on symlinks to prevent accidental out-of-root target modification
    const lst = await fs.promises.lstat(abs);
    if (lst.isSymbolicLink()) {
      return res.status(400).json({ error: 'Cannot change permissions of symbolic links (security guard)' });
    }

    if (mode === undefined || mode === null)
      return res.status(400).json({ error: 'Missing mode' });
    const modeNum = parseInt(String(mode), 8);
    if (isNaN(modeNum) || modeNum < 0 || modeNum > 0o777)
      return res.status(400).json({ error: 'Invalid mode' });
    await fs.promises.chmod(abs, modeNum);
    log('info', 'Permissions changed', { path: toVirtual(abs), mode: mode.toString(8), clientId: (req.body.clientId || '').slice(0, 12) });
    res.json({ success: true });
  } catch (err) { res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message }); }
});

app.post('/api/trash/restore', async (req, res) => {
  try {
    const { id } = req.body;
    const rows = await query(`SELECT * FROM trash WHERE id = ?`, [id]);
    const item = rows[0];
    if (!item) return res.status(404).json({ error: 'Not found' });
    
    let dest = await resolvePath(item.originalPath, true);
    // Conflict resolution: if destination exists, find a free name
    if (fs.existsSync(dest)) {
      dest = await findFreeName(dest);
    }

    try {
      await fs.promises.mkdir(path.dirname(dest), { recursive: true });
      await fs.promises.rename(item.trashPath, dest);
    } finally {
      releaseName(dest);
    }
    await run(`DELETE FROM trash WHERE id = ?`, [id]);
    
    log('info', 'Trash item restored', { from: item.trashPath, to: toVirtual(dest), clientId: (req.body.clientId || '').slice(0, 12) });
    res.json({ success: true, restoredPath: toVirtual(dest) });
  } catch (err) { res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message }); }
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
// Yields {srcAbs, relPath} for every file under dirAbs.
// Follows symlinks to directories but tracks realpath to avoid loops.
async function* walkDir(dirAbs, relBase, depth = 0, _seen = new Set()) {
  if (depth > 60) {
    log('warn', 'walkDir: max depth reached', { dir: dirAbs });
    return;
  }

  // Resolve to detect symlink loops
  let realDir;
  try { realDir = await fs.promises.realpath(dirAbs); }
  catch { realDir = dirAbs; }
  if (_seen.has(realDir)) {
    log('debug', 'walkDir: skipping symlink loop', { dir: dirAbs, realpath: realDir });
    return;
  }
  _seen.add(realDir);

  let dir;
  try { dir = await fs.promises.opendir(dirAbs); }
  catch (e) { log('warn', 'walkDir: cannot open dir', { dir: dirAbs, error: e.message }); return; }

  for await (const entry of dir) {
    const abs = path.join(dirAbs, entry.name);
    const rel = path.join(relBase, entry.name);

    if (entry.isSymbolicLink()) {
      // Resolve the link target
      let tgt;
      try { tgt = await fs.promises.stat(abs); } catch { continue; } // broken link — skip
      if (tgt.isDirectory()) {
        yield* walkDir(abs, rel, depth + 1, _seen); // recurse into symlinked dir
      } else if (tgt.isFile()) {
        yield { srcAbs: abs, relPath: rel };
      }
    } else if (entry.isDirectory()) {
      yield* walkDir(abs, rel, depth + 1, _seen);
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
  try { await fs.promises.writeFile(ledgerPath, JSON.stringify(ledger)); } catch (e) { logDebug('flushLedger failed', { path: ledgerPath, error: e.message }); }
}

// ── Conflict helpers ────────────────────────────────────────────
// Atomic in-memory lock to prevent two simultaneous operations from claiming the same "free" name
const PENDING_NAMES = new Set();

/** Returns a path that doesn't exist yet by appending (2), (3)... before the extension.
 *  Uses async fs.promises.access so it doesn't block the event loop. */
async function findFreeName(destPath) {
  const check = async (p) => {
    if (PENDING_NAMES.has(p)) return false;
    try { await fs.promises.access(p); return false; }
    catch { return true; }
  };

  if (await check(destPath)) { PENDING_NAMES.add(destPath); return destPath; }

  const dir  = path.dirname(destPath);
  const ext  = path.extname(destPath);
  const base = path.basename(destPath, ext);
  let n = 2;
  while (n < 10000) {
    const candidate = path.join(dir, `${base} (${n})${ext}`);
    if (await check(candidate)) {
      PENDING_NAMES.add(candidate);
      return candidate;
    }
    n++;
  }
  const final = path.join(dir, `${base} (${Date.now()})${ext}`);
  PENDING_NAMES.add(final);
  return final;
}

/** Release a previously claimed name so others can use it if the op finishes or fails. */
function releaseName(p) { if (p) PENDING_NAMES.delete(p); }


// ── API routes ───────────────────────────────────────────────────

// Pre-flight conflict check — called BEFORE starting a transfer.
// Returns a list of destination paths that already exist so the
// client can ask the user what to do before any files move.
app.post('/api/transfer/check', async (req, res) => {
  try {
    const { sources, destination } = req.body;
    if (!sources?.length || !destination)
      return res.status(400).json({ error: 'Missing sources or destination' });

    const destAbs   = await resolvePath(destination, true);
    const conflicts = [];

    for (const src of sources) {
      const srcAbs  = await resolvePath(src, true);
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
          } catch (e) { /* dest doesn't exist — no conflict */ }
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
        } catch (e) { /* dest doesn't exist — no conflict */ }
      }
      if (conflicts.length >= 500) break;
    }

    res.json({ conflicts, truncated: conflicts.length >= 500 });
  } catch (err) { res.status(err.message.includes('Symlink escape') || err.message.includes('traversal') ? 403 : 500).json({ error: err.message }); }
});

app.post('/api/transfer', async (req, res) => {
  try {
    const { sources, destination, operation, clientId, conflictResolutions } = req.body;
    if (!sources?.length || !destination)
      return res.status(400).json({ error: 'Missing sources or destination' });
    log('info', 'Transfer requested', {
      operation, sources: sources.length, destination,
      clientId: (clientId || '').slice(0, 12),
      conflicts_resolved: conflictResolutions ? Object.keys(conflictResolutions).length : 0
    });
    const destAbs = await resolvePath(destination, true);
    // conflictResolutions: { [destVirtualPath]: 'overwrite' | 'skip' | 'keepboth' }
    const job = createJob('transfer', clientId, { operation, sources, destination, phase: 'starting' });
    res.json({ jobId: job.jobId, started: true });
    setImmediate(() => runTransfer(job, sources, destAbs, operation, null, null, conflictResolutions || {}));
  } catch (err) { res.status(err.message.includes('Symlink escape') || err.message.includes('traversal') ? 403 : 500).json({ error: err.message }); }
});

app.post('/api/transfer/cancel', async (req, res) => {
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
    const c = path.join(await resolvePath(existingJob.destination, true), `.nova_xfr_${jobId}.json`);
    try { await fs.promises.access(c); ledgerPath = c; } catch (e) { /* not found in specific dest */ }
  }
  if (!ledgerPath) {
    ledgerPath = await new Promise((resolve) => {
      execFile(
        'find', [ROOT || '/', '-maxdepth', '10', '-name', `.nova_xfr_${jobId}.json`],
        { timeout: 15000 },
        (err, stdout) => {
          const lines = (stdout || '').trim().split('\n');
          resolve(lines[0] || null);
        }
      );
    });
  }
  if (!ledgerPath)
    return res.status(404).json({ error: 'No resumable transfer found for this jobId' });

  try { await fs.promises.access(ledgerPath); }
  catch { return res.status(404).json({ error: 'Ledger file no longer exists or is unreadable' }); }

  let ledger;
  try { ledger = JSON.parse(await fs.promises.readFile(ledgerPath, 'utf8')); }
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
  setImmediate(async () => runTransfer(
    resumeJob, null, await resolvePath(ledger.destination, true),
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

  // ── PRE-FLIGHT CHECKS ──────────────────────────────────────────
  // 1. Recursive move: check if moving a folder into itself
  // 2. Same-file: check if source and destination are identical

  // Guard: Resume operations skip initial scan pre-flight as sources is null
  const flightSources = sources || [];
  for (const src of flightSources) {
    const srcAbs  = await resolvePath(src, true);
    const srcName = path.basename(srcAbs);
    const dstItem = path.join(destAbs, srcName);

    // Recursive Move Check: Are we moving a folder into its own sub-folder?
    if (operation === 'move' && isSubpath(srcAbs, destAbs)) {
      const errMsg = `Cannot move "${srcName}" into itself or its subdirectory.`;
      log('error', `[${jid}] Recursive move detected`, { src: srcName, dest: destAbs });
      updateJob(job, { status: 'error', error: errMsg, finishedAt: new Date().toISOString() });
      return;
    }

    // Same-file Move Check: Are we moving a file to its own path?
    if (operation === 'move' && srcAbs === dstItem) {
      const errMsg = `Source and destination are identical for "${srcName}".`;
      log('error', `[${jid}] Same-file move blocked`, { src: srcName });
      updateJob(job, { status: 'error', error: errMsg, finishedAt: new Date().toISOString() });
      return;
    }
  }

  // ── MOVE FAST PATH: try top-level rename on each source ─────────
  // If source and destination are on the same device, a single rename()
  // on the top-level item moves the entire tree in O(1) — no I/O at all.
  if (operation === 'move' && !isResume) {
    const fastMoved = [];
    const needsCopy = [];

    for (const src of sources) {
      const srcAbs  = await resolvePath(src, true);
      const srcName = path.basename(srcAbs);
      const dstItem = path.join(destAbs, srcName);

      try { await fs.promises.mkdir(destAbs, { recursive: true }); } catch (e) { logDebug('mkdir failed during move', { path: destAbs, error: e.message }); }

      // Check if dest already exists and we have a top-level resolution
      let destExists = false;
      try { await fs.promises.stat(dstItem); destExists = true; } catch (e) { /* dest doesn't exist */ }

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
          } finally {
            releaseName(newDst);
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

    const scanSources = originalSources || [];
    for (const src of scanSources) {
      const srcAbs  = await resolvePath(src, true);
      let   srcName = path.basename(srcAbs);
      
      // If copying to same parent, add unique suffix to avoid self-overwrite
      if (operation === 'copy' && srcAbs === path.join(destAbs, srcName)) {
        const free = await findFreeName(path.join(destAbs, srcName));
        srcName = path.basename(free);
        log('info', `[${jid}] Auto-renamed same-path copy`, { old: path.basename(srcAbs), new: srcName });
      }

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
        let resolution = 'overwrite';
        try { await fs.promises.stat(effectiveDest); resolution = conflictResolutions[toVirtual(effectiveDest)] || 'overwrite'; } catch (e) { /* dest doesn't exist */ }
        if (resolution === 'skip') {
          ledgerSetState(ledger, f.destPath, 'done');
          doneFiles++; doneBytes += f.size; scheduleProgress(fname); scheduleLedger(); continue;
        }
        if (resolution === 'keepboth') effectiveDest = await findFreeName(effectiveDest);

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
          addJobLog(job, errMsg);
          job.errorCount = (job.errorCount || 0) + 1;
          ledgerSetState(ledger, f.destPath, 'error');
          notifyGotify(`Transfer error ✗`, `${errMsg}\nJob: ${jid}`, 9);
          updateJob(job, { status: 'error', error: errMsg, finishedAt: new Date().toISOString() });
          await flushLedger(ledgerPath, ledger);
          if (progressTimer) clearTimeout(progressTimer);
          if (ledgerTimer)   clearTimeout(ledgerTimer);
          return;
        } finally {
          if (resolution === 'keepboth') releaseName(effectiveDest);
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

    const copyWorker = async () => {
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
        } catch (e) { /* partial file doesn't exist */ }

        // Conflict resolution
        let effectiveDest = f.destPath;
        let pNameReserved = false;
        if (startByte === 0) {
          try {
            await fs.promises.stat(effectiveDest);
            const resolution = conflictResolutions[toVirtual(effectiveDest)] || 'overwrite';
            if (resolution === 'skip') {
              log('debug', `[${jid}] Skipping (conflict)`, { file: fname });
              ledgerSetState(ledger, f.destPath, 'done');
              doneFiles++; doneBytes += f.size; scheduleProgress(fname); scheduleLedger(); continue;
            }
            if (resolution === 'keepboth') {
              effectiveDest = await findFreeName(effectiveDest);
              pNameReserved = true;
            }
          } catch (e) { /* no conflict */ }
        }

        const copyStart = Date.now();
        try {
          // If source is a symlink, recreate the link at destination
          const srcLst = await fs.promises.lstat(f.srcAbs).catch(() => null);
          if (srcLst && srcLst.isSymbolicLink()) {
            const target = await fs.promises.readlink(f.srcAbs);
            await fs.promises.mkdir(path.dirname(effectiveDest), { recursive: true });
            try { await fs.promises.unlink(effectiveDest); } catch (e) { /* ignore unlink error if not exists */ }
            await fs.promises.symlink(target, effectiveDest);
            log('debug', `[${jid}] Symlink recreated`, { link: effectiveDest, target });
          } else {
            await copyOneFile(f.srcAbs, effectiveDest, startByte, f.size, pct => {
              const bytesDone = doneBytes + startByte + (pct / 100) * Math.max(0, f.size - startByte);
              updateJob(job, {
                currentFile: fname, filePercent: pct,
                percent: Math.round((bytesDone / Math.max(totalBytes, 1)) * 100)
              });
            });
          }
        } catch (e) {
          const errMsg = `Failed to copy "${fname}": ${e.message}`;
          log('error', `[${jid}] Copy failed`, { file: fname, code: e.code, error: e.message, start_byte: startByte });
          addJobLog(job, errMsg);
          job.errorCount = (job.errorCount || 0) + 1;
          ledgerSetState(ledger, f.destPath, 'error');
          notifyGotify(`Transfer error ✗`, `${errMsg}\nJob: ${jid}\nClick Resume to continue.`, 9);
          fatalErr = { errMsg, finishedAt: new Date().toISOString() };
          break;
        } finally {
          if (pNameReserved) releaseName(effectiveDest);
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
          catch (e) { if (e.code !== 'ENOENT') addJobLog(job, `Warning: could not delete source "${fname}": ${e.message}`); }
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
      const srcAbs = await resolvePath(src, true);
      try {
        const stat = await fs.promises.stat(srcAbs).catch(() => null);
        if (stat && stat.isDirectory()) {
          // SURGICAL CLEANUP: Use a surgical approach to avoid deleting skipped or extraneous files.
          // We walk the source again and only delete files if we're sure they were part of this move
          // and marked as 'done' (successfully moved/overwritten).
          for await (const entry of walkDir(srcAbs, '')) {
            const destPath = path.join(destAbs, entry.relPath);
            if (ledger.files[destPath] === 'done') {
              try { await fs.promises.unlink(entry.srcAbs); } catch (e) { /* ignore unlink error */ }
            }
          }
          // After unlinking moved files, attempt to prune now-empty directories.
          await pruneEmpty(srcAbs);
          log('debug', `[${jid}] Surgical cleanup complete`, { dir: srcAbs });
        } else if (stat && stat.isFile()) {
          // If it was a single file move, and it's marked done, delete it.
          const srcName = path.basename(srcAbs);
          const destPath = path.join(destAbs, srcName);
          if (ledger.files[destPath] === 'done') {
            try { await fs.promises.unlink(srcAbs); } catch (e) { /* ignore */ }
          }
        }
      } catch (e) { logDebug('Cleanup: stat failed', { dir: srcAbs, error: e.message }); }
    }
  }

  // ── DONE ─────────────────────────────────────────────────────────
  const totalMs = Date.now() - (new Date(job.startedAt).getTime());
  log('info', `[${jid}] Transfer complete`, {
    operation, files: doneFiles, bytes: doneBytes, total_ms: totalMs,
    avg_mbps: doneBytes > 0 ? Math.round((doneBytes / 1024 / 1024) / Math.max(totalMs / 1000, 0.001)) : 0
  });

  try { await fs.promises.unlink(ledgerPath); } catch (e) { logDebug('Ledger unlink failed (final cleanup)', { path: ledgerPath, error: e.message }); }

  updateJob(job, { status: 'done', percent: 100, filePercent: 100, phase: 'done', finishedAt: new Date().toISOString() });
}

// Depth-first prune of empty directories after a move operation.
async function pruneEmpty(dir) {
  let entries;
  try { entries = await fs.promises.readdir(dir); } catch (e) { return; }
  for (const e of entries) {
    const full = path.join(dir, e);
    try {
      if ((await fs.promises.stat(full)).isDirectory()) await pruneEmpty(full);
    } catch (e2) { /* ignore */ }
  }
  try {
    if ((await fs.promises.readdir(dir)).length === 0) await fs.promises.rmdir(dir);
  } catch (e) { /* ignore rmdir error */ }
}

app.post('/api/delete', async (req, res) => {
  try {
    const { paths, clientId } = req.body;
    if (!paths?.length) return res.status(400).json({ error: 'No paths' });

    log('info', 'Trash requested', { paths: paths.length, clientId: (clientId || '').slice(0, 12) });

    const safeToDelete = [];
    for (const p of paths) {
      if (!p || p === '/') continue;
      try {
        if (await resolvePath(p, true) !== ROOT) safeToDelete.push(p);
      } catch (e) { /* ignore error in filter */ }
    }
    if (safeToDelete.length === 0) return res.status(400).json({ error: 'Cannot delete root or escaped paths' });
    const filteredPaths = safeToDelete;

    const job = createJob('delete', clientId, { sources: filteredPaths, phase: 'deleting' });
    log('info', 'Trash job started', { jobId: job.jobId.slice(0, 8), count: filteredPaths.length });
    res.json({ jobId: job.jobId, started: true });
    setImmediate(() => runDelete(job, filteredPaths));
  } catch (err) { res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message }); }
});

async function runDelete(job, paths) {
  const total = paths.length;
  let done = 0;
  for (const p of paths) {
    try {
      const abs       = await resolvePath(p, true);
      const name      = path.basename(abs);
      const id        = uuidv4();
      const trashPath = path.join(TRASH_DIR, id);
      try {
        await fs.promises.rename(abs, trashPath);
      } catch (e) {
        if (e.code === 'EXDEV') {
          // Cross-device trash: fallback to recursive copy + delete for directories and files
          await fs.promises.cp(abs, trashPath, { recursive: true });
          await fs.promises.rm(abs, { recursive: true, force: true });
        } else throw e;
      }
      await run(`INSERT INTO trash (id, name, originalPath, trashPath, deletedAt) VALUES (?, ?, ?, ?, ?)`, 
                [id, name, p, trashPath, new Date().toISOString()]);
    } catch (e) {
      logDebug('Delete task failed for item', { path: p, error: e.message });
      addJobLog(job, `Failed to trash ${p}: ${e.message}`);
      job.errorCount = (job.errorCount || 0) + 1;
    }
    done++;
    if (done % 10 === 0 || done === total) {
      updateJob(job, { percent: Math.round((done / total) * 100), currentFile: path.basename(p) });
      await new Promise(r => setImmediate(r));
    }
  }
  updateJob(job, { status: 'done', percent: 100, phase: 'done', finishedAt: new Date().toISOString(), sources: paths });
}

// PERMANENT DELETE ───────────────────────────────────────────────
app.post('/api/delete-permanent', async (req, res) => {
  try {
    const { paths, clientId } = req.body;
    if (!paths?.length) return res.status(400).json({ error: 'No paths' });

    // Guard: never allow deleting the virtual root '/' or ROOT itself
    const safePaths = [];
    for (const p of paths) {
      if (!p || p === '/') continue;
      try {
        if (await resolvePath(p, true) !== ROOT) safePaths.push(p);
      } catch (e) { /* ignore */ }
    }
    if (safePaths.length === 0) return res.status(400).json({ error: 'Cannot delete root' });

    const job = createJob('delete-permanent', clientId, { sources: safePaths, phase: 'deleting' });
    log('info', 'Permanent delete requested', { jobId: job.jobId.slice(0, 8), paths: safePaths.length, clientId: (clientId || '').slice(0, 12) });
    res.json({ jobId: job.jobId, started: true });

    setImmediate(async () => {
      try {
        const total = safePaths.length;
        let done = 0;
        for (const p of safePaths) {
          try { await fs.promises.rm(await resolvePath(p, true), { recursive: true, force: true }); }
          catch (e) { 
            log('error', `Permanent delete failed: ${p}`, { error: e.message }); 
            addJobLog(job, `Failed: ${p}: ${e.message}`); 
            job.errorCount = (job.errorCount || 0) + 1;
          }
          done++;
          if (done % 5 === 0 || done === total)
            updateJob(job, { percent: Math.round((done / total) * 100), currentFile: path.basename(p) });
        }
        updateJob(job, { status: 'done', percent: 100, phase: 'done', finishedAt: new Date().toISOString() });
      } catch (err) {
        log('error', `Permanent delete routine crashed`, { error: err.message });
        updateJob(job, { status: 'error', error: err.message, finishedAt: new Date().toISOString() });
      }
    });
  } catch (err) { res.status(err.message.includes('Symlink escape') || err.message.includes('traversal') ? 403 : 500).json({ error: err.message }); }
});

// SYMLINK — recreate a broken symlink with a new target
app.post('/api/symlink/recreate', async (req, res) => {
  try {
    const { path: p, target } = req.body;
    if (!p || !target) return res.status(400).json({ error: 'Missing path or target' });
    const abs = await resolvePath(p, true);
    // Security check: target must be inside ROOT (virtual or absolute)
    try { await resolvePath(target, false); }
    catch (e) { return res.status(403).json({ error: 'Symlink target must be inside ROOT: ' + target }); }

    // Remove the broken link first
    await fs.promises.unlink(abs);
    // Re-create with new target
    await fs.promises.symlink(target, abs);
    log('info', 'Symlink recreated', { link: toVirtual(abs), target, clientId: (req.body.clientId || '').slice(0, 12) });
    res.json({ success: true });
  } catch (err) { res.status(err.message.includes('Symlink escape') || err.message.includes('traversal') ? 403 : 500).json({ error: err.message }); }
});

// EMPTY TRASH ────────────────────────────────────────────────────
app.post('/api/trash/empty', async (req, res) => {
  const { clientId } = req.body;
  try {
    const meta = await query(`SELECT * FROM trash`);
    if (meta.length === 0) return res.json({ success: true });

    const job = createJob('trash-empty', clientId, { sources: meta.map(i => i.trashPath), phase: 'deleting' });
    log('info', 'Trash empty requested', { jobId: job.jobId.slice(0, 8), items: meta.length, clientId: (clientId || '').slice(0, 12) });
    res.json({ jobId: job.jobId, started: true });
    setImmediate(async () => {
      const total = meta.length;
      let done = 0;
      for (const item of meta) {
        try { await fs.promises.rm(item.trashPath, { recursive: true, force: true }); }
        catch (e) { 
          addJobLog(job, `Failed: ${item.name}: ${e.message}`);
          job.errorCount = (job.errorCount || 0) + 1;
        }
        done++;
        if (done % 10 === 0 || done === total) {
          updateJob(job, { percent: Math.round((done / total) * 100), currentFile: item.name });
        }
      }
      await run(`DELETE FROM trash`);
      updateJob(job, { status: 'done', percent: 100, phase: 'done', finishedAt: new Date().toISOString() });
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// UPLOAD ─────────────────────────────────────────────────────────
// Files land in /tmp first (express-fileupload useTempFiles:true)
// We then move them in a background job — so closing the browser
// after the upload HTTP request completes doesn't lose the file.
app.post('/api/upload', async (req, res) => {
  try {
    if (!req.files || !Object.keys(req.files).length) return res.status(400).json({ error: 'No files' });
    const destDir  = await resolvePath(req.body.path || '/', true);
    const clientId = req.body.clientId || 'unknown';
    const fileList = Array.isArray(req.files.files) ? req.files.files : [req.files.files];
    // Sanitize filenames: remove directory separators and other risky characters
    const sanitize = (n) => n.replace(/[/\\]/g, '_').replace(/^\.+/, '_').trim() || 'unnamed';
    const names    = fileList.map(f => sanitize(f.name));

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
      for (let i = 0; i < fileList.length; i++) {
        const file = fileList[i];
        const sName = names[i];
        let dest = path.join(destDir, sName);
        dest = await findFreeName(dest);
        updateJob(job, { currentFile: sName, filePercent: 0, percent: Math.round((done / total) * 100) });
        try {
          await file.mv(dest);
          done++;
          updateJob(job, { percent: Math.round((done / total) * 100), filePercent: 100 });
        } catch (e) {
          addJobLog(job, `Failed to save ${file.name}: ${e.message}`);
          job.errorCount = (job.errorCount || 0) + 1;
          done++;
          updateJob(job, { percent: Math.round((done / total) * 100) });
        } finally {
          releaseName(dest);
        }
      }
      updateJob(job, { status: 'done', percent: 100, phase: 'done', finishedAt: new Date().toISOString() });
    });
  } catch (err) { res.status(err.message.includes('Symlink escape') || err.message.includes('traversal') ? 403 : 500).json({ error: err.message }); }
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

app.get('/api/file/read', async (req, res) => {
  try {
    const p   = await resolvePath(req.query.path, true);
    const ext = path.extname(p).toLowerCase().replace('.', '');
    if (!EDITABLE_EXTS.has(ext)) return res.status(400).json({ error: 'File type not editable' });
    const content = await fs.promises.readFile(p, 'utf8');
    const stat    = await fs.promises.stat(p);
    res.json({ content, size: stat.size, modified: stat.mtime });
  } catch (e) {
    const status = e.message.includes('Symlink escape') ? 403 : 500;
    res.status(status).json({ error: e.message });
  }
});

app.post('/api/file/write', async (req, res) => {
  let tmp = null;
  try {
    const { path: p, content } = req.body;
    if (content === undefined) return res.status(400).json({ error: 'No content' });
    const abs = await resolvePath(p, true);
    const ext = path.extname(abs).toLowerCase().replace('.', '');
    if (!EDITABLE_EXTS.has(ext)) return res.status(400).json({ error: 'File type not editable' });

    // Atomic write: write to unique .tmp then rename
    tmp = abs + '.nova_edit_' + Date.now() + '_' + Math.random().toString(36).slice(2);
    await fs.promises.writeFile(tmp, content, 'utf8');
    await fs.promises.rename(tmp, abs);
    tmp = null; // Marked as successfully moved
    
    const stat = await fs.promises.stat(abs);
    log('info', 'File saved', { path: toVirtual(abs), size: stat.size, clientId: (req.body.clientId || '').slice(0, 12) });
    res.json({ success: true, size: stat.size, modified: stat.mtime });
  } catch (e) { 
    res.status(e.message.includes('Symlink escape') ? 403 : 500).json({ error: e.message }); 
  } finally {
    if (tmp) { try { await fs.promises.unlink(tmp); } catch(e) {} }
  }
});

// ─────────────────────────────────────────────────────────────────
// ZIP / UNZIP — background jobs, survive browser close, resumable
// ─────────────────────────────────────────────────────────────────
app.post('/api/zip', async (req, res) => {
  try {
    const { sources, destination, clientId } = req.body;
    if (!sources?.length || !destination) return res.status(400).json({ error: 'Missing sources or destination' });
    const destAbs = await resolvePath(destination, true);
    const job     = createJob('zip', clientId, { sources, destination, phase: 'zipping' });
    res.json({ jobId: job.jobId, started: true });
    setImmediate(() => runZip(job, sources, destAbs));
  } catch (err) { res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message }); }
});

async function sumDirSizeRecursive(abs, depth = 0) {
  if (depth > 100) return { size: 0, files: 0 }; // Protection against infinite recursion
  let size = 0, files = 0;
  const entries = await fs.promises.readdir(abs, { withFileTypes: true });
  for (const entry of entries) {
    const full = path.join(abs, entry.name);
    if (entry.isDirectory()) {
      const sub = await sumDirSizeRecursive(full, depth + 1);
      size += sub.size; files += sub.files;
    } else if (entry.isFile()) {
      try { const s = await fs.promises.stat(full); size += s.size; files++; } catch (e) { /* skip unreadable files */ }
    }
  }
  return { size, files };
}

async function runZip(job, sources, destAbs) {
  const jid = job.jobId.slice(0, 8);
  log('info', `[${jid}] Zip started`, { sources: sources.length, dest: destAbs });

  let totalBytes = 0;
    for (const src of sources) {
    try {
      const srcAbs = await resolvePath(src, true);
      const s = await fs.promises.stat(srcAbs);
      if (s.isDirectory()) { 
        const res = await sumDirSizeRecursive(srcAbs); 
        totalBytes += res.size; 
      }
      else { totalBytes += s.size; }
    } catch (e) { /* ignore stat error */ }
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
          
          // Wait for stream to close before unlinking to prevent file locks/leaks
          output.on('close', async () => {
            try { await fs.promises.unlink(partPath); } catch (e) { /* ignore unlink error */ }
            updateJob(job, {
              status: 'cancelled', percent: job.percent || 0,
              finishedAt: new Date().toISOString(),
              _zipSources: sources, _zipDest: toVirtual(destAbs)
            });
            resolve();
          });
          output.destroy();
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
      (async () => {
        for (const src of sources) {
          const abs  = await resolvePath(src, true);
          const name = path.basename(abs);
          try {
            const s = await fs.promises.stat(abs); // follows symlinks — zip the target content
            if (s.isDirectory()) { archive.directory(abs, name); }
            else { archive.file(abs, { name }); }
          } catch (e) { 
            addJobLog(job, `Skipped ${name}: ${e.message}`); 
            job.errorCount = (job.errorCount || 0) + 1;
          }
        }
        archive.finalize();
      })();
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
    (async () => {
      try { await fs.promises.unlink(partPath); } catch (e2) { /* ignore unlink error */ }
      try { await fs.promises.unlink(destAbs); } catch (e3) { /* ignore unlink error */ }
    })();
    notifyGotify('Zip failed ✗', `${path.basename(destAbs)}\n${e.message}`, 9);
    updateJob(job, { status: 'error', error: e.message, finishedAt: new Date().toISOString() });
  }
}

// Resume a cancelled zip — restart from scratch (can't partially continue a zip archive)
app.post('/api/zip/resume', async (req, res) => {
  try {
    const { jobId, clientId } = req.body;
    if (!jobId) return res.status(400).json({ error: 'Missing jobId' });
    const oldJob = jobs.get(jobId);
    if (!oldJob) return res.status(404).json({ error: 'Job not found' });
    const sources = oldJob._zipSources || oldJob.sources;
    const dest    = oldJob._zipDest    || oldJob.destination;
    if (!sources?.length || !dest) return res.status(400).json({ error: 'Cannot resume: missing sources/destination' });
    const destAbs = await resolvePath(dest, true);
    const newJob  = createJob('zip', clientId || oldJob.clientId, {
      sources, destination: dest, phase: 'zipping', resumedFrom: jobId
    });
    res.json({ jobId: newJob.jobId, started: true });
    setImmediate(() => runZip(newJob, sources, destAbs));
  } catch (err) { res.status(err.message.includes('Symlink escape') || err.message.includes('traversal') ? 403 : 500).json({ error: err.message }); }
});

app.post('/api/unzip', async (req, res) => {
  try {
    const { path: p, destination, clientId } = req.body;
    if (!p) return res.status(400).json({ error: 'Missing path' });
    const abs     = await resolvePath(p, true);
    const destDir = destination ? await resolvePath(destination, true) : path.dirname(abs);
    const job     = createJob('unzip', clientId, {
      sources: [p], destination: toVirtual(destDir), phase: 'extracting'
    });
    res.json({ jobId: job.jobId, started: true });
    setImmediate(() => runUnzip(job, abs, destDir));
  } catch (err) { res.status(err.message.includes('Symlink escape') || err.message.includes('traversal') ? 403 : 500).json({ error: err.message }); }
});

async function runUnzip(job, zipPath, destDir) {
  const jid  = job.jobId.slice(0, 8);
  const name = path.basename(zipPath);
  log('info', `[${jid}] Unzip started`, { file: name, dest: destDir });

  try { await fs.promises.mkdir(destDir, { recursive: true }); } catch (e) { /* ignore mkdir error */ }

  try {
    // SECURITY: Zip Slip Prevention. Scan archive contents before extraction.
    const listResult = await new Promise((res) => {
      // -Z1 = Zip info mode, filenames only. Much safer to parse.
      execFile('unzip', ['-Z1', zipPath], { timeout: 30000 }, (err, stdout) => {
        if (err) res({ err });
        else res({ items: stdout.trim().split('\n') });
      });
    });

    if (listResult.err) throw new Error('Could not read zip archive header');
    const totalZipItems = listResult.items.filter(f => f.trim().length > 0).length;
    const isZipSlip = listResult.items.some(fpath => {
      const f = fpath.trim();
      return f.includes('..') || f.startsWith('/');
    });

    if (isZipSlip) {
      log('warn', 'Zip Slip attempt blocked', { file: name, zip: zipPath });
      throw new Error('Security: Zip archive contains invalid paths (Zip Slip detected)');
    }

    await new Promise((resolve, reject) => {
      const child = execFile('unzip', ['-o', zipPath, '-d', destDir], { timeout: 300_000 }, (err) => {
        if (job.cancelRequested) return;
        if (err && err.killed) { return; }
        if (err && err.code !== 0 && err.code !== 1) {
          // code 1 = "at least one warning" — still success
          reject(new Error('Unzip failed (exit ' + err.code + '): ' + (err.message || 'unknown error')));
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
              // Use real percentage based on total item count from -Z1 scan
              const pct = totalZipItems > 0
                ? Math.min(95, Math.round((fileCount / totalZipItems) * 100))
                : Math.min(95, fileCount);
              updateJob(job, { currentFile: path.basename(m[1].trim()), phase: 'extracting',
                               percent: pct });
            }
          }
        });
      }

      // Poll cancelRequested
      const cancelPoll = setInterval(() => {
        if (job.cancelRequested) {
          clearInterval(cancelPoll);
          try { 
            child.kill('SIGTERM'); 
            // Failsafe: if still alive after 2s, send SIGKILL
            setTimeout(() => {
              try { if (child.exitCode === null) child.kill('SIGKILL'); } catch(e) {}
            }, 2000);
          } catch (e) { /* ignore kill error */ }
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
app.post('/api/zip/cancel', async (req, res) => {
  const { jobId } = req.body;
  const job = jobs.get(jobId);
  if (!job || job.status !== 'running') return res.status(404).json({ error: 'No running job' });
  job.cancelRequested = true;
  if (job._unzipProc) { try { job._unzipProc.kill('SIGTERM'); } catch (e) { /* ignore kill error */ } }
  log('info', `[${jobId.slice(0,8)}] Zip/unzip cancel requested`);
  res.json({ ok: true });
});

// Resume a cancelled unzip — re-extract (unzip -o overwrites safely)
app.post('/api/unzip/resume', async (req, res) => {
  try {
    const { jobId, clientId } = req.body;
    if (!jobId) return res.status(400).json({ error: 'Missing jobId' });
    const oldJob = jobs.get(jobId);
    if (!oldJob) return res.status(404).json({ error: 'Job not found' });
    const zipPath = oldJob._unzipPath || (oldJob.sources && oldJob.sources[0]);
    const dest    = oldJob._unzipDest || oldJob.destination;
    if (!zipPath) return res.status(400).json({ error: 'Cannot resume: missing zip path' });
    const absZip  = await resolvePath(zipPath, true);
    const absDest = dest ? await resolvePath(dest, true) : path.dirname(absZip);
    const newJob  = createJob('unzip', clientId || oldJob.clientId, {
      sources: [zipPath], destination: toVirtual(absDest), phase: 'extracting', resumedFrom: jobId
    });
    res.json({ jobId: newJob.jobId, started: true });
    setImmediate(() => runUnzip(newJob, absZip, absDest));
  } catch (err) { res.status(err.message.includes('Symlink escape') || err.message.includes('traversal') ? 403 : 500).json({ error: err.message }); }
});

// ─────────────────────────────────────────────────────────────────
// DIRECTORY SIZE — async du with SSE streaming so UI stays live
// ─────────────────────────────────────────────────────────────────
app.get('/api/dirsize', async (req, res) => {
  try {
    const p = await resolvePath(req.query.path, true);
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

  const _seen = new Set(); // track real paths to avoid symlink loops

  // Async dir walk using a queue — never blocks the event loop
  const walk = async (dir, depth) => {
    if (aborted || depth > 50) return;
    // Track realpath for symlink loop detection
    let realDir; try { realDir = await fs.promises.realpath(dir); } catch { realDir = dir; }
    if (depth === 0) _seen.add(realDir); // seed with root on first call
    let entries;
    try { entries = await fs.promises.readdir(dir, { withFileTypes: true }); }
    catch { return; }

    for (const entry of entries) {
      if (aborted) return;
      const full = path.join(dir, entry.name);
      if (entry.isSymbolicLink()) {
        // Follow symlink — skip if broken or loops back to a seen dir
        let tgt;
        try { tgt = await fs.promises.stat(full); } catch { continue; }
        if (tgt.isDirectory()) {
          let real; try { real = await fs.promises.realpath(full); } catch { real = full; }
          if (!_seen.has(real)) { _seen.add(real); await walk(full, depth + 1); }
        } else if (tgt.isFile()) {
          totalBytes += tgt.size; totalFiles++;
          if (totalFiles % 500 === 0) await new Promise(r => setImmediate(r));
        }
        continue;
      }
      if (entry.isDirectory()) {
        await walk(full, depth + 1);
      } else if (entry.isFile()) {
        try {
          const s = await fs.promises.stat(full);
          totalBytes += s.size;
          totalFiles++;
        } catch (e) { /* ignore stat error */ }
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
  } catch (err) {
    res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message });
  }
});

// ─────────────────────────────────────────────────────────────────
// OLLAMA INTEGRATION
const OLLAMA_URL   = (process.env.OLLAMA_URL || 'http://localhost:11434').replace(/\/+$/, '');
const MODEL_TEXT   = process.env.OLLAMA_TEXT_MODEL   || 'llama3.2:1b';
const MODEL_VISION = process.env.OLLAMA_VISION_MODEL || 'moondream:latest';
const MODEL_EMBED  = process.env.OLLAMA_EMBED_MODEL  || 'nomic-embed-text:latest';
const MODEL_AGENT  = process.env.OLLAMA_AGENT_MODEL  || MODEL_TEXT;


// ── Ollama rate limiter ─────────────────────────────────────────
// Prevents misbehaving clients from hammering AI endpoints.
// Configurable via OLLAMA_RATE_LIMIT (reqs) and OLLAMA_RATE_WINDOW (ms).
const OLLAMA_RATE_MAX    = parseInt(process.env.OLLAMA_RATE_LIMIT  || '30');
const OLLAMA_RATE_WINDOW = parseInt(process.env.OLLAMA_RATE_WINDOW || '60000');
const _ollamaHits = new Map(); // ip → { count, resetAt }
setInterval(() => {
  const now = Date.now();
  for (const [ip, rec] of _ollamaHits) { if (now > rec.resetAt) _ollamaHits.delete(ip); }
}, 120_000).unref();

function ollamaRateLimit(req, res, next) {
  const ip  = req.ip || req.socket?.remoteAddress || 'unknown';
  const now = Date.now();
  let rec   = _ollamaHits.get(ip);
  if (!rec || now > rec.resetAt) { rec = { count: 0, resetAt: now + OLLAMA_RATE_WINDOW }; _ollamaHits.set(ip, rec); }
  rec.count++;
  if (rec.count > OLLAMA_RATE_MAX) {
    const retry = Math.ceil((rec.resetAt - now) / 1000);
    log('warn', '[ollama] rate limit', { ip, count: rec.count, retry_s: retry });
    return res.status(429).json({ error: `AI rate limit — retry in ${retry}s.` });
  }
  next();
}
app.use('/api/ollama', ollamaRateLimit);

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
  } catch (e) { logDebug('Ollama tags fetch failed', { error: e.message }); }

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

  // Helper for memory-safe partial reads
  const readPrefix = (p, max) => new Promise((resolve, reject) => {
    const chunks = [];
    let total = 0;
    const stream = fs.createReadStream(p, { start: 0, end: max - 1 });
    stream.on('data', chunk => {
      chunks.push(chunk);
      total += chunk.length;
      if (total >= max) stream.destroy();
    });
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', reject);
    stream.on('close', () => resolve(Buffer.concat(chunks)));
  });

  if (IMAGE_EXTS.has(ext) || mtype.startsWith('image/')) {
    return { kind: 'image', text: '', ext, name };
  }

  if (TEXT_EXTS.has(ext) || mtype.startsWith('text/')) {
    try {
      const buf = await readPrefix(abs, 14000);
      return { kind: 'text', text: buf.toString('utf8'), ext, name };
    } catch (e) { return { kind: 'error', text: '', ext, name, error: e.message }; }
  }

  if (ext === 'pdf' || mtype === 'application/pdf') {
    try {
      const buf = await readPrefix(abs, 30000); // Take more for PDF to catch visible runs
      const raw = buf.toString('binary');
      const runs = raw.match(/[ -~\t\n\r]{5,}/g) || [];
      const text = runs.filter(s => /[a-zA-Z]{3,}/.test(s)).join(' ').replace(/\s+/g,' ').slice(0, 12000);
      return { kind: 'text', text, ext: 'pdf', name };
    } catch (e) { return { kind: 'error', text: '', ext: 'pdf', name, error: e.message }; }
  }

  // For directories — stat first so dotted dir names work too (e.g. /var/lib/node.js)
  try {
    const _st = await fs.promises.stat(abs);
    if (_st.isDirectory()) {
      const allEntries = await fs.promises.readdir(abs);
      const stats = { dirs: [], images: [], code: [], text: [], other: [] };
      for (const e of allEntries.slice(0, 120)) {
        let st;
        try {
          st = await fs.promises.stat(path.join(abs, e));
          if (st.isDirectory()) { stats.dirs.push(e); continue; }
        } catch (e2) { continue; }
        const x = path.extname(e).toLowerCase().slice(1);
        if (['jpg','jpeg','png','gif','webp','svg','avif','heic'].includes(x)) stats.images.push(e);
        else if (['js','ts','py','go','rs','java','sh','c','cpp','rb','php'].includes(x)) stats.code.push(e);
        else if (['txt','md','log','json','yaml','yml','toml','xml','csv'].includes(x)) stats.text.push(e);
        else stats.other.push(e);
      }
      const total = allEntries.length;
      const lines = [
        `Folder: ${name} (${total} items total)`,
        stats.dirs.length   ? `Subfolders (${stats.dirs.length}): ${stats.dirs.slice(0,10).join(', ')}${stats.dirs.length>10?' …':''}` : '',
        stats.images.length ? `Images (${stats.images.length}): ${stats.images.slice(0,8).join(', ')}` : '',
        stats.code.length   ? `Code files (${stats.code.length}): ${stats.code.slice(0,8).join(', ')}` : '',
        stats.text.length   ? `Documents/text (${stats.text.length}): ${stats.text.slice(0,8).join(', ')}` : '',
        stats.other.length  ? `Other files (${stats.other.length}): ${stats.other.slice(0,8).join(', ')}` : '',
      ].filter(Boolean).join('\n');
      return { kind: 'text', text: lines, ext: 'dir', name };
    }
  } catch (e) { /* ignore stat/readdir error */ }

  // Unknown binary — return minimal metadata
  try {
    const s = await fs.promises.stat(abs);
    return { kind: 'binary', text: `File: ${name}\nSize: ${s.size} bytes\nType: ${mtype || ext || 'unknown'}`, ext, name };
  } catch (e) { return { kind: 'error', text: '', ext, name, error: e.message }; }
}

// Summarize any file
app.post('/api/ollama/summarize', async (req, res) => {
  try {
    const { path: p } = req.body;
    const abs = await resolvePath(p, true);

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
      prompt: content.ext === 'dir'
        ? `You are a file manager assistant. Based on this folder inventory, write 2-3 sentences describing what this directory is for and what kinds of files it contains. Be specific and helpful.\n\n${content.text}`
        : `Summarize this file in 2-4 sentences. Be factual and concise.\nFile: ${content.name}\n\n${content.text}`
    }, 90_000);
    const summary = (d.response || '').trim();
    notifyGotify('Nova AI ✦', `Summarized: ${content.name}\n${summary.slice(0, 200)}`, 3);
    res.json({ summary });
  } catch (e) {
    log('error', '[ollama] summarize failed', { error: e.message });
    res.status(500).json({ error: e.message });
  }
} catch (err) { res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message }); }
});

// Agent: plan file operations in a directory
app.post('/api/ollama/agent-plan', async (req, res) => {
  try {
    const { dir, prompt } = req.body || {};
    const vDir = typeof dir === 'string' && dir ? dir : '/';
    if (!prompt || typeof prompt !== 'string' || !prompt.trim()) {
      return res.status(400).json({ error: 'Missing prompt' });
    }

    const abs = await resolvePath(vDir, true);
    let listing = '';
    try {
      const entries = (await fs.promises.readdir(abs, { withFileTypes: true })).slice(0, 200);
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
      'You are a file-manager AI inside the Nova file manager.',
      'Turn the user request into safe file operations. You can move files anywhere in the filesystem.',
      '',
      'Supported operations (ONLY these):',
      '- mkdir:             create a directory.',
      '  Fields: { "op": "mkdir", "path": "<absolute path>" }',
      '- move:              move/rename a single file or folder.',
      '  Fields: { "op": "move", "from": "<abs source>", "to": "<abs destination including filename>" }',
      '- move_filter:       move items of a specific category from ONE directory to a target.',
      '  Fields: { "op": "move_filter", "dir": "<abs dir>", "target": "<abs target dir>", "filter": "non_image_files"|"image_files"|"directories"|"files" }',
      '- move_except_filter: move everything in a dir EXCEPT items of a category.',
      '  Fields: { "op": "move_except_filter", "dir": "<abs dir>", "target": "<abs target dir>", "except_filter": "image_files"|"non_image_files"|"directories"|"files" }',
      '- move_except',       'move everything in a dir EXCEPT specific named files/folders.',
      '  Fields: { "op": "move_except", "dir": "<abs dir>", "target": "<abs target dir>", "except": ["name1", "name2"] }',
      '',
      'Rules:',
      '- Paths are absolute from the Nova root. Current dir is shown below.',
      '- If the user names a destination folder that does not exist, emit a mkdir op for it FIRST.',
      '- If user says "Downloads" resolve to /home/<user>/Downloads or the closest match visible in path.',
      '  Current path components: ' + vDir.split('/').filter(Boolean).join(', '),
      '- Use move_except_filter when user says "move everything except images" or "except jpegs/pngs/any image type".',
      '- Use move_filter when user says "move only images" or "move only files".',
      '- Use move_except when user says "except <specific filename>".',
      '- move_filter / move_except / move_except_filter operate on immediate children of "dir" only (not recursive).',
      '- Do NOT delete anything. Do not emit delete, copy, or chmod operations.',
      '- If target is outside current dir, use the full absolute path (e.g. /home/user/Downloads/noob).',
      '- Infer home directory from current path. E.g. if current is /home/zoro/Pictures/Wallpaper, home is /home/zoro.',
      '- **NEW: If the user is asking a question (e.g. "how many files are here?") rather than requesting a file operation, provide a natural language answer in the "response" field.**',
      '- **Always provide a helpful textual summary of your actions or an answer to their question in the "response" field.**',
      '',
      `Current directory: ${toVirtual(abs)}`,
      'Files in current directory (up to 200):',
      listing || '(empty)',
      '',
      'Return STRICT JSON only — no markdown, no comments, no extra text:',
      '{ "response": "Your answer or explanation here", "operations": [ ... ] }',
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
          (op.op === 'mkdir' || op.op === 'move' || op.op === 'move_filter' || op.op === 'move_except' || op.op === 'move_except_filter'))
        .map(op => {
          if (op.op === 'mkdir')              return { op: 'mkdir', path: String(op.path || '') };
          if (op.op === 'move_filter')        return { op: 'move_filter',        dir: String(op.dir || ''), target: String(op.target || ''), filter: String(op.filter || '') };
          if (op.op === 'move_except_filter') return { op: 'move_except_filter', dir: String(op.dir || ''), target: String(op.target || ''), except_filter: String(op.except_filter || '') };
          if (op.op === 'move_except')        return { op: 'move_except',        dir: String(op.dir || ''), target: String(op.target || ''), except: Array.isArray(op.except) ? op.except.map(String) : [] };
          return { op: 'move', from: String(op.from || ''), to: String(op.to || '') };
        })
        .filter(op => {
          if (op.op === 'mkdir')              return op.path.startsWith('/');
          if (op.op === 'move')               return op.from.startsWith('/') && op.to.startsWith('/');
          if (op.op === 'move_filter')        return op.dir.startsWith('/') && op.target.startsWith('/') && !!op.filter;
          if (op.op === 'move_except_filter') return op.dir.startsWith('/') && op.target.startsWith('/') && !!op.except_filter;
          if (op.op === 'move_except')        return op.dir.startsWith('/') && op.target.startsWith('/') && Array.isArray(op.except);
          return false;
        });

      res.json({ plan: { operations: safeOps, response: plan.response || '' }, raw });
    } catch (e) {
      log('error', '[ollama] agent-plan failed', { error: e.message });
      res.status(500).json({ error: e.message });
    }
  } catch (err) { res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message }); }
});

// Agent: dry-run preview — returns exactly which files would be affected, no changes made
app.post('/api/ollama/agent-preview', async (req, res) => {
  try {
    const { plan } = req.body || {};
    const ops = plan && Array.isArray(plan.operations) ? plan.operations : [];
    if (!ops.length) return res.status(400).json({ error: 'No operations to preview' });

    const preview = [];
    for (const op of ops) {
      if (!op || typeof op !== 'object') continue;
      try {
        if (op.op === 'mkdir') {
          const abs = await resolvePath(op.path, true);
          let exists = false;
          try { await fs.promises.access(abs); exists = true; } catch (e) { /* doesn't exist */ }
          preview.push({ op: 'mkdir', path: op.path, exists, files: [] });

        } else if (op.op === 'move') {
          const fromAbs = await resolvePath(op.from, true);
          let stat = null;
          try { stat = await fs.promises.lstat(fromAbs); } catch (e) { /* doesn't exist */ }
          preview.push({
            op: 'move', from: op.from, to: op.to,
            exists: !!stat,
            isDir: stat ? stat.isDirectory() : false,
            size: stat ? stat.size : 0,
            files: stat ? [{ name: path.basename(op.from), from: op.from, to: op.to, size: stat.size }] : []
          });

        } else if (op.op === 'move_filter' || op.op === 'move_except_filter' || op.op === 'move_except') {
          const dirAbs    = await resolvePath(op.dir, true);
          const targetAbs = await resolvePath(op.target, true);
          let entries = [];
          try { entries = await fs.promises.readdir(dirAbs, { withFileTypes: true }); } catch (e) { /* ignore readdir error */ }

          const files = [];
          for (const e of entries) {
            const name  = e.name;
            const ext   = path.extname(name).toLowerCase().replace('.', '');
            const m     = mime.lookup(name) || '';
            const isImg = (m && String(m).startsWith('image/')) || IMAGE_EXTS.has(ext);
            let include = false;

            if (op.op === 'move_filter') {
              const f = String(op.filter || '');
              include =
                (f === 'directories'     && e.isDirectory()) ||
                (f === 'files'           && e.isFile()) ||
                (f === 'image_files'     && e.isFile() && isImg) ||
                (f === 'non_image_files' && e.isFile() && !isImg);
            } else if (op.op === 'move_except_filter') {
              const excF = String(op.except_filter || '');
              const excluded =
                (excF === 'image_files'     && e.isFile() && isImg) ||
                (excF === 'non_image_files' && e.isFile() && !isImg) ||
                (excF === 'directories'     && e.isDirectory()) ||
                (excF === 'files'           && e.isFile());
              include = !excluded;
            } else if (op.op === 'move_except') {
              const exc = (op.except || []).map(n => String(n).toLowerCase());
              include = !exc.includes(name.toLowerCase());
            }

            if (include) {
              let size = 0;
              try { const s = await fs.promises.stat(path.join(dirAbs, name)); size = s.size; } catch (e) { /* ignore stat error */ }
              files.push({
                name,
                from: toVirtual(path.join(dirAbs, name)),
                to:   toVirtual(path.join(targetAbs, name)),
                size,
                isDir: e.isDirectory()
              });
            }
          }
          preview.push({ op: op.op, dir: op.dir, target: op.target, files, total: files.length });
        }
      } catch (e) {
        preview.push({ op: op.op, error: e.message, files: [] });
      }
    }
    res.json({ preview });
  } catch (err) { res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message }); }
});

async function runAgent(job, ops) {
  const jid = job.jobId.slice(0, 8);
  const total = ops.length;
  let done = 0;
  const results = [];
  const ALLOWED_OPS = new Set(['mkdir','move','move_filter','move_except','move_except_filter']);

  for (const op of ops) {
    if (job.cancelRequested) break;
    if (!op || typeof op !== 'object') continue;
    if (!ALLOWED_OPS.has(op.op)) {
      results.push({ op: op.op, status: 'error', error: 'Unknown operation type — rejected' });
      continue;
    }
    try {
      if (op.op === 'mkdir') {
        const abs = await resolvePath(op.path, true);
        await fs.promises.mkdir(abs, { recursive: true });
        results.push({ op: 'mkdir', path: op.path, status: 'ok' });
        log('info', `[${jid}] Agent: mkdir`, { path: op.path });
      } else if (op.op === 'move') {
        const fromAbs = await resolvePath(op.from, true);
        const toAbs   = await resolvePath(op.to, true);
        if (isSubpath(fromAbs, toAbs)) {
          results.push({ op: 'move', from: op.from, status: 'error', error: 'Cannot move into itself' });
          continue;
        }
        await fs.promises.mkdir(path.dirname(toAbs), { recursive: true });
        let finalToAbs = toAbs;
        
        // Case-only rename trick
        const oldName = path.basename(fromAbs);
        const newName = path.basename(toAbs);
        if (oldName.toLowerCase() === newName.toLowerCase() && oldName !== newName && path.dirname(fromAbs) === path.dirname(toAbs)) {
           const tmp = toAbs + '.' + uuidv4().slice(0, 8) + '.tmp';
           await fs.promises.rename(fromAbs, tmp);
           await fs.promises.rename(tmp, toAbs);
           results.push({ op: 'move', from: op.from, to: toVirtual(toAbs), status: 'ok' });
        } else {
          try { await fs.promises.access(finalToAbs); finalToAbs = await findFreeName(finalToAbs); } catch (e) { /* ignore: if access fails, file doesn't exist, use original finalToAbs */ }
          try {
            await fs.promises.rename(fromAbs, finalToAbs);
          } finally {
            releaseName(finalToAbs);
          }
          results.push({ op: 'move', from: op.from, to: toVirtual(finalToAbs), status: 'ok' });
        }
        log('info', `[${jid}] Agent: move`, { from: op.from, to: toVirtual(finalToAbs) });
      } else if (op.op === 'move_filter' || op.op === 'move_except_filter' || op.op === 'move_except') {
        // Backgrounding batch moves
        const dirAbs    = await resolvePath(op.dir, true);
        const targetAbs = await resolvePath(op.target, true);
        await fs.promises.mkdir(targetAbs, { recursive: true });
        const entries = await fs.promises.readdir(dirAbs, { withFileTypes: true });
        let moved = 0, skipped = 0;

        const filter = op.filter || null;
        const excFilter = op.except_filter || null;
        const exceptNames = (op.except || []).map(n => String(n).toLowerCase());

        for (const e of entries) {
           if (job.cancelRequested) break;
           const name = e.name;
           const fullPath = path.join(dirAbs, name);
           let stat;
           try { stat = await fs.promises.stat(fullPath); } catch (e2) { stat = null; }
           const isDir = stat ? stat.isDirectory() : e.isDirectory();
           const isFile = stat ? stat.isFile() : e.isFile();

           if (op.op === 'move_filter') {
              const ext = path.extname(name).toLowerCase().replace('.', '');
              const m   = mime.lookup(name) || '';
              const isImg = (m && String(m).startsWith('image/')) || IMAGE_EXTS.has(ext);
              const match = (filter === 'directories' && isDir) || (filter === 'files' && isFile) || (filter === 'image_files' && isFile && isImg) || (filter === 'non_image_files' && isFile && !isImg);
              if (!match) { skipped++; continue; }
           } else if (op.op === 'move_except') {
              if (exceptNames.includes(name.toLowerCase())) { skipped++; continue; }
           } else if (op.op === 'move_except_filter') {
              const ext = path.extname(name).toLowerCase().replace('.', '');
              const m   = mime.lookup(name) || '';
              const isImg = (m && String(m).startsWith('image/')) || IMAGE_EXTS.has(ext);
              const isExcluded = (excFilter === 'image_files' && isFile && isImg) || (excFilter === 'non_image_files' && isFile && !isImg) || (excFilter === 'directories' && isDir) || (excFilter === 'files' && isFile);
              if (isExcluded) { skipped++; continue; }
           }

           let dstAbs = path.join(targetAbs, name);
           dstAbs = await findFreeName(dstAbs);
           try {
             await fs.promises.rename(fullPath, dstAbs);
           } finally {
             releaseName(dstAbs);
           }
           moved++;
        }
        results.push({ ...op, moved, skipped, status: 'ok' });
        log('info', `[${jid}] Agent: ${op.op}`, { dir: op.dir, target: op.target, moved });
      }
    } catch (e) {
      results.push({ ...op, status: 'error', error: e.message });
    }
    done++;
    updateJob(job, { percent: Math.round((done / total) * 100), currentFile: `Operation ${done}/${total}` });
  }

  const finalStatus = job.cancelRequested ? 'cancelled' : 'done';
  updateJob(job, { 
    status: finalStatus, 
    percent: 100, 
    phase: 'done', 
    finishedAt: new Date().toISOString(),
    results 
  });
}

// Agent: execute a previously returned plan
app.post('/api/ollama/agent-run', async (req, res) => {
  try {
    const { plan, clientId } = req.body || {};
    const ops = plan && Array.isArray(plan.operations) ? plan.operations : [];
    if (!ops.length) return res.status(400).json({ error: 'No operations to run' });

    const job = createJob('agent-run', clientId, { 
      ops_count: ops.length, 
      explanation: plan.explanation || '',
      phase: 'executing'
    });

    res.json({ jobId: job.jobId, started: true });
    setImmediate(() => runAgent(job, ops));
  } catch (err) { res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message }); }
});
app.post('/api/ollama/suggest-name', async (req, res) => {
  try {
    const { path: p } = req.body;
    const abs = await resolvePath(p, true);

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
                     .replace(/[^a-zA-Z0-9_-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
      res.json({ name: (raw || 'unnamed') + ext });
    } catch (e) {
      log('error', '[ollama] suggest-name failed', { error: e.message });
      res.status(500).json({ error: e.message });
    }
  } catch (err) { res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message }); }
});

// Tag any file
app.post('/api/ollama/tag', async (req, res) => {
  try {
    const { path: p } = req.body;
    const abs = await resolvePath(p, true);

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
  } catch (err) { res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message }); }
});

// Describe image (vision model)
app.post('/api/ollama/describe-image', async (req, res) => {
  try {
    const { path: p } = req.body;
    const abs  = await resolvePath(p, true);
    const name = path.basename(abs);
    
    // Guard: refuse to load very large images into memory (>25MB)
    try {
      const imgStat = await fs.promises.stat(abs);
      if (imgStat.size > 25 * 1024 * 1024) {
        return res.status(400).json({ error: `Image too large for vision model (${Math.round(imgStat.size/1024/1024)}MB > 25MB limit)` });
      }
    } catch (e) { return res.status(404).json({ error: 'Image not found: ' + e.message }); }

    let b64;
    try { b64 = (await fs.promises.readFile(abs)).toString('base64'); }
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
  } catch (err) { res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message }); }
});

// Semantic search with nomic-embed-text
app.post('/api/ollama/search', async (req, res) => {
  try {
    const { query: searchQuery, dir, showHidden } = req.body;
    if (!searchQuery || !dir) return res.status(400).json({ error: 'Missing query or dir' });
    const absDir = await resolvePath(dir, true);
    log('info', '[ollama] search', { query: searchQuery, dir, model: MODEL_EMBED });

    let queryEmbed;
    try {
      const qd = await ollamaFetch('/api/embeddings', { model: MODEL_EMBED, prompt: searchQuery }, 30_000);
      queryEmbed = qd.embedding;
    } catch (e) { return res.status(500).json({ error: 'Embedding failed: ' + e.message }); }

    let entries;
    try { entries = await fs.promises.readdir(absDir, { withFileTypes: true }); }
    catch (e) { return res.status(500).json({ error: e.message }); }

    const scored = [];
    for (const entry of entries.filter(e => e.isFile())) {
      if (!showHidden && entry.name.startsWith('.')) continue; // Filter dotfiles if hidden
      const full = path.join(absDir, entry.name);
      try {
        const stats = await fs.promises.stat(full);
        const mtime = stats.mtime.getTime();
        
        // Check cache
        const cached = await query(`SELECT embedding FROM embeddings WHERE filePath = ? AND mtime = ? AND model = ?`, 
                                   [full, mtime, MODEL_EMBED]);
        
        let de;
        if (cached.length > 0) {
          de = JSON.parse(cached[0].embedding);
        } else {
          const content = await extractFileContent(full).catch(() => null);
          if (!content || content.kind === 'error' || content.kind === 'binary' || !content.text.trim()) continue;
          const textChunk = content.kind === 'image' ? `image file: ${entry.name}` : content.text.slice(0, 3000);
          
          const fd = await ollamaFetch('/api/embeddings', { model: MODEL_EMBED, prompt: textChunk }, 20_000);
          de = fd.embedding;
          
          // Purge stale vectors before saving the updated snapshot
          await run(`DELETE FROM embeddings WHERE filePath = ? AND model = ?`, [full, MODEL_EMBED]);
          await run(`INSERT INTO embeddings (filePath, mtime, model, embedding) VALUES (?, ?, ?, ?)`,
                    [full, mtime, MODEL_EMBED, JSON.stringify(de)]);
        }

        const dot  = queryEmbed.reduce((s, v, i) => s + v * de[i], 0);
        const magQ = Math.sqrt(queryEmbed.reduce((s, v) => s + v*v, 0));
        const magD = Math.sqrt(de.reduce((s, v) => s + v*v, 0));
        scored.push({ name: entry.name, path: toVirtual(full), score: magQ && magD ? dot / (magQ * magD) : 0 });
      } catch (e) { logDebug('search item failed', { file: entry.name, error: e.message }); continue; }
    }
    scored.sort((a, b) => b.score - a.score);
    res.json({ results: scored.slice(0, 10) });
  } catch (err) { res.status(err.message.includes('Symlink escape') ? 403 : 500).json({ error: err.message }); }
});

// ─────────────────────────────────────────────────────────────────
// CATCH-ALL — must be LAST, after every /api/* route is registered
// ─────────────────────────────────────────────────────────────────
app.get('*', async (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

server.listen(PORT, () => {
  log('info', 'Nova File Manager started', {
    port:       PORT,
    root:       ROOT || '/',
    pid:        process.pid,
    node:       process.version,
    log_level:  LOG_LEVEL,
    log_format: LOG_FORMAT,
    ollama:     process.env.OLLAMA_URL || 'http://localhost:11434',
    gotify:     !!process.env.GOTIFY_URL,
  });
});

/**
 * Nova Monitor — watches nova-filemanager health and sends Gotify alerts
 * Runs as a companion container in the same docker-compose stack
 */

const http  = require('http');
const https = require('https');

const TARGET_URL    = process.env.MONITOR_TARGET    || 'http://nova-filemanager:9898';
const GOTIFY_URL    = process.env.GOTIFY_URL        || '';      // e.g. http://192.168.1.10:3030
const GOTIFY_TOKEN  = process.env.GOTIFY_TOKEN      || '';
const CHECK_EVERY   = parseInt(process.env.CHECK_INTERVAL_SEC || '30') * 1000;
const FAIL_THRESH   = parseInt(process.env.FAIL_THRESHOLD     || '3');  // alerts after N consecutive failures
const APP_NAME      = process.env.APP_NAME          || 'Nova File Manager';

let consecutiveFails = 0;
let lastStatus       = 'unknown';  // 'up' | 'down' | 'unknown'
let lastAlertSent    = 0;
const ALERT_COOLDOWN = 5 * 60 * 1000; // don't spam — max 1 alert per 5 min

// ── helpers ────────────────────────────────────────────────────────────────

function log(level, msg) {
  const ts = new Date().toISOString();
  console.log(`[${ts}] [${level.toUpperCase()}] ${msg}`);
}

function checkHealth() {
  const url = `${TARGET_URL}/api/diskinfo`;
  const mod = url.startsWith('https') ? https : http;
  const req = mod.get(url, { timeout: 8000 }, (res) => {
    if (res.statusCode === 200) {
      onUp();
    } else {
      onFail(`HTTP ${res.statusCode}`);
    }
    res.resume(); // drain
  });
  req.on('error', (err) => onFail(err.message));
  req.on('timeout', () => { req.destroy(); onFail('Request timed out'); });
}

function onUp() {
  if (lastStatus === 'down') {
    log('info', `${APP_NAME} recovered ✓`);
    sendGotify(`${APP_NAME} is back up ✓`, `The service recovered and is responding normally.`, 5);
  } else if (lastStatus === 'unknown') {
    log('info', `${APP_NAME} is healthy ✓`);
  }
  consecutiveFails = 0;
  lastStatus = 'up';
}

function onFail(reason) {
  consecutiveFails++;
  log('warn', `Health check failed (${consecutiveFails}/${FAIL_THRESH}): ${reason}`);

  if (consecutiveFails >= FAIL_THRESH) {
    const now = Date.now();
    if (lastStatus !== 'down' || now - lastAlertSent > ALERT_COOLDOWN) {
      lastAlertSent = now;
      lastStatus = 'down';
      log('error', `${APP_NAME} is DOWN — sending alert`);
      sendGotify(
        `${APP_NAME} is DOWN ✗`,
        `Health check failed ${consecutiveFails} time(s) in a row.\nReason: ${reason}\nTarget: ${TARGET_URL}`,
        8
      );
    }
  }
}

function sendGotify(title, message, priority = 5) {
  if (!GOTIFY_URL || !GOTIFY_TOKEN) {
    log('debug', `Gotify not configured — skipping alert: ${title}`);
    return;
  }
  const body = JSON.stringify({ title, message, priority });
  const url  = new URL(`${GOTIFY_URL}/message?token=${GOTIFY_TOKEN}`);
  const mod  = url.protocol === 'https:' ? https : http;
  const req  = mod.request({
    hostname: url.hostname,
    port:     url.port || (url.protocol === 'https:' ? 443 : 80),
    path:     url.pathname + url.search,
    method:   'POST',
    headers:  { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
  }, (res) => {
    res.resume();
    if (res.statusCode >= 200 && res.statusCode < 300) {
      log('info', `Gotify alert sent: "${title}"`);
    } else {
      log('warn', `Gotify returned HTTP ${res.statusCode}`);
    }
  });
  req.on('error', (e) => log('warn', `Gotify send failed: ${e.message}`));
  req.write(body);
  req.end();
}

// ── status HTTP server (so docker healthcheck works) ───────────────────────
http.createServer((req, res) => {
  const status = { status: lastStatus, consecutiveFails, target: TARGET_URL, checked: new Date().toISOString() };
  res.writeHead(lastStatus === 'down' ? 503 : 200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(status));
}).listen(9090, () => log('info', `Nova Monitor started — watching ${TARGET_URL} every ${CHECK_EVERY/1000}s`));

// ── main loop ──────────────────────────────────────────────────────────────
// Wait 20s on startup for filemanager to be ready before first check
setTimeout(() => {
  checkHealth();
  setInterval(checkHealth, CHECK_EVERY);
}, 20000);

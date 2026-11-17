'use strict';

// notes-api: a small notes service front.
// Serves a plain status page on / and relays realtime event
// traffic on /api/events to a local worker on 127.0.0.1:18081.

const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');
const { spawn, execFile } = require('child_process');
const WebSocket = require('ws');

const PORT = parseInt(process.env.PORT || '3000', 10);
const WORKER_PORT = 18081;
const WS_PATH = '/api/events';
const CORE_VERSION = '1.14.4';

const ROOT = __dirname;
const CORE_BIN = path.join(ROOT, 'lib', 'core');
const CONF_PATH = path.join(ROOT, 'data', 'app.json');

const TAG = '[notes-api]';
function log(msg) {
  console.log(TAG + ' ' + msg);
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function rmQuiet(target) {
  try {
    fs.rmSync(target, { recursive: true, force: true });
  } catch (e) {}
}

function workerArch() {
  if (process.arch === 'x64') return 'linux-amd64';
  if (process.arch === 'arm64') return 'linux-arm64';
  return null;
}

// Worker config is generated at startup from the environment.
// Nothing sensitive is written into the code itself.
function writeWorkerConfig() {
  const uuid = process.env.APP_UUID;
  if (!uuid) return false;
  ensureDir(path.dirname(CONF_PATH));
  const conf = {
    log: { level: 'error' },
    inbounds: [
      {
        type: 'vless',
        tag: 'main',
        listen: '127.0.0.1',
        listen_port: WORKER_PORT,
        users: [{ uuid: uuid }],
        transport: { type: 'ws', path: WS_PATH }
      }
    ],
    outbounds: [{ type: 'direct', tag: 'direct' }]
  };
  fs.writeFileSync(CONF_PATH, JSON.stringify(conf, null, 2));
  return true;
}

function fetchToFile(url, dest, hops) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { headers: { 'User-Agent': 'notes-api' } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        if ((hops || 0) >= 5) {
          reject(new Error('too many redirects'));
          return;
        }
        fetchToFile(res.headers.location, dest, (hops || 0) + 1).then(resolve, reject);
        return;
      }
      if (res.statusCode !== 200) {
        reject(new Error('download status ' + res.statusCode));
        return;
      }
      const out = fs.createWriteStream(dest);
      res.pipe(out);
      out.on('finish', () => {
        out.close();
        resolve();
      });
      out.on('error', reject);
    });
    req.on('error', reject);
  });
}

function runTar(args) {
  return new Promise((resolve, reject) => {
    execFile('tar', args, (err) => (err ? reject(err) : resolve()));
  });
}

// Fetch the worker binary on first start only. The archive is
// removed right after extraction; only lib/core is kept.
async function ensureWorker() {
  if (fs.existsSync(CORE_BIN)) return true;
  const arch = workerArch();
  if (!arch) return false;
  const fileName = 'sing-box-' + CORE_VERSION + '-' + arch + '.tar.gz';
  const url =
    'https://github.com/SagerNet/sing-box/releases/download/v' +
    CORE_VERSION +
    '/' +
    fileName;
  const workDir = path.join(ROOT, 'lib', '.tmp');
  try {
    ensureDir(workDir);
    await fetchToFile(url, path.join(workDir, fileName), 0);
    await runTar([
      '--no-same-owner',
      '-xzf',
      path.join(workDir, fileName),
      '-C',
      workDir
    ]);
    const top = fs
      .readdirSync(workDir)
      .find(
        (n) =>
          n.indexOf('sing-box-') === 0 &&
          fs.statSync(path.join(workDir, n)).isDirectory()
      );
    if (!top) return false;
    const bin = path.join(workDir, top, 'sing-box');
    if (!fs.existsSync(bin)) return false;
    ensureDir(path.dirname(CORE_BIN));
    fs.copyFileSync(bin, CORE_BIN);
    fs.chmodSync(CORE_BIN, 0o755);
    return true;
  } catch (e) {
    log('worker error: ' + (e && e.message ? e.message : 'unknown'));
    return false;
  } finally {
    rmQuiet(workDir);
  }
}

// Guard the worker: restart it if it exits, with a longer pause
// when it keeps failing in a short window.
let worker = null;
let recentExits = [];
let restartTimer = null;

function startWorker() {
  if (worker) return;
  if (!fs.existsSync(CORE_BIN)) return;
  const child = spawn(CORE_BIN, ['run', '-c', CONF_PATH], {
    stdio: 'ignore'
  });
  worker = child;
  log('worker started');
  child.on('error', () => {
    worker = null;
    scheduleRestart(3000);
  });
  child.on('exit', () => {
    worker = null;
    const now = Date.now();
    recentExits.push(now);
    recentExits = recentExits.filter((t) => now - t < 60000);
    const delay = recentExits.length > 5 ? 30000 : 3000;
    scheduleRestart(delay);
  });
}

function scheduleRestart(delay) {
  if (restartTimer) return;
  restartTimer = setTimeout(() => {
    restartTimer = null;
    startWorker();
  }, delay);
}

const wss = new WebSocket.Server({ noServer: true });

wss.on('connection', (client) => {
  const upstream = new WebSocket('ws://127.0.0.1:' + WORKER_PORT + WS_PATH, {
    perMessageDeflate: false
  });
  const pending = [];
  client.on('message', (data, isBinary) => {
    if (upstream.readyState === WebSocket.OPEN) {
      upstream.send(data, { binary: isBinary });
    } else if (upstream.readyState === WebSocket.CONNECTING) {
      pending.push([data, isBinary]);
    }
  });
  upstream.on('open', () => {
    let item;
    while ((item = pending.shift())) {
      upstream.send(item[0], { binary: item[1] });
    }
  });
  upstream.on('message', (data, isBinary) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data, { binary: isBinary });
    }
  });
  const closeBoth = () => {
    try {
      if (client.readyState === WebSocket.OPEN) client.close();
    } catch (e) {}
    try {
      if (upstream.readyState === WebSocket.OPEN) upstream.close();
    } catch (e) {}
  };
  upstream.on('close', closeBoth);
  upstream.on('error', closeBoth);
  client.on('close', closeBoth);
  client.on('error', closeBoth);
});

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(
      JSON.stringify({ status: 'ok', service: 'notes-api', version: '1.0.0' })
    );
    return;
  }
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ status: 'error', message: 'not found' }));
});

server.on('upgrade', (req, socket) => {
  let pathname = '';
  try {
    pathname = new URL(req.url, 'http://localhost').pathname;
  } catch (e) {
    pathname = req.url;
  }
  if (pathname !== WS_PATH) {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, Buffer.alloc(0), (client) => {
    wss.emit('connection', client, req);
  });
});

// Stop the worker together with the front on shutdown so no
// orphan keeps listening after the platform stops the app.
function shutdown() {
  if (worker) {
    try {
      worker.kill('SIGTERM');
    } catch (e) {}
  }
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// The front stays up even when the worker is not ready yet.
server.on('error', (err) => {
  log('service error: ' + (err && err.code ? err.code : 'unknown'));
  process.exit(1);
});

server.listen(PORT, '0.0.0.0', () => {
  log('service listening on ' + PORT);
});

async function setupWorker() {
  if (!process.env.APP_UUID) {
    log('worker not configured');
    return;
  }
  if (!writeWorkerConfig()) {
    log('worker not configured');
    return;
  }
  const ok = await ensureWorker();
  if (ok) {
    startWorker();
  } else {
    log('worker unavailable');
  }
}

setupWorker();

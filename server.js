const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = parseInt(process.env.PORT || '10000', 10);

const MAGIC = 0x504D4B48; // "HKMP"
const PROTO_VER = 2;

const PKT_HELLO      = 1;
const PKT_WELCOME    = 2;
const PKT_STATE      = 3;
const PKT_PING       = 4;
const PKT_PONG       = 5;
const PKT_BYE        = 6;
const PKT_ENEMY_HIT  = 7;
const PKT_ENEMY_SYNC = 8;
const PKT_WORLD_BOOL = 9;
const PKT_CHAT       = 10;

const MAX_PLAYERS = 8;

const clients = new Map(); // ws -> { id, name, lastSeen }
const worldBools = new Map(); // "scene|id" -> Buffer (raw WorldBoolPacket)

function allocId() {
  const used = new Set();
  for (const c of clients.values()) used.add(c.id);
  for (let i = 1; i < MAX_PLAYERS; i++) {
    if (!used.has(i)) return i;
  }
  return -1;
}

function readFixedStr(buf, offset, maxLen) {
  const slice = buf.subarray(offset, offset + maxLen);
  const nul = slice.indexOf(0);
  return slice.subarray(0, nul === -1 ? maxLen : nul).toString('utf8');
}

function makeWelcomePacket(assignedId) {
  // PacketHeader (8B) + uint8 assignedId (1B) + char hostName[24] = 33B
  const b = Buffer.alloc(33);
  b.writeUInt32LE(MAGIC, 0);
  b.writeUInt8(PROTO_VER, 4);
  b.writeUInt8(PKT_WELCOME, 5);
  b.writeUInt8(0, 6);
  b.writeUInt8(0, 7);
  b.writeUInt8(assignedId, 8);
  b.write('RenderServer', 9, 24, 'utf8');
  return b;
}

function broadcast(buf, excludeWs) {
  for (const ws of clients.keys()) {
    if (ws !== excludeWs && ws.readyState === 1) {
      ws.send(buf, { binary: true });
    }
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ok: true,
      service: 'hk-android-coop',
      players: clients.size,
      worldFlags: worldBools.size,
      uptimeSec: Math.floor(process.uptime())
    }));
    return;
  }
  res.writeHead(404);
  res.end('Not found');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  ws.on('message', (raw, isBinary) => {
    const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
    if (buf.length < 8) return;

    const magic = buf.readUInt32LE(0);
    const ver = buf.readUInt8(4);
    const ptype = buf.readUInt8(5);
    if (magic !== MAGIC || ver !== PROTO_VER) return;

    const now = Date.now();

    if (ptype === PKT_HELLO && buf.length >= 32) {
      const name = readFixedStr(buf, 8, 24) || 'Knight';
      let info = clients.get(ws);
      if (!info) {
        const id = allocId();
        if (id < 0) return;
        info = { id, name, lastSeen: now };
        clients.set(ws, info);
        console.log(`[HKMP] Joined: ${name} (id=${id}), total=${clients.size}`);
      } else {
        info.name = name;
        info.lastSeen = now;
      }

      ws.send(makeWelcomePacket(info.id), { binary: true });

      // Replay all shared world flags (levers, gates, defeated bosses)
      for (const wbBuf of worldBools.values()) {
        ws.send(wbBuf, { binary: true });
      }
      return;
    }

    if (ptype === PKT_PING && buf.length >= 16) {
      const info = clients.get(ws);
      if (info) info.lastSeen = now;
      const pong = Buffer.from(buf);
      pong.writeUInt8(PKT_PONG, 5);
      pong.writeUInt8(0, 6);
      ws.send(pong, { binary: true });
      return;
    }

    if (ptype === PKT_BYE) {
      const info = clients.get(ws);
      if (info) {
        console.log(`[HKMP] Left: ${info.name} (id=${info.id})`);
        clients.delete(ws);
      }
      return;
    }

    const info = clients.get(ws);
    if (!info) return;
    info.lastSeen = now;

    if (ptype === PKT_WORLD_BOOL && buf.length >= 122) {
      const scene = readFixedStr(buf, 10, 48);
      const id = readFixedStr(buf, 58, 64);
      if (scene && id) {
        const key = `${scene}|${id}`;
        worldBools.set(key, Buffer.from(buf));
        console.log(`[HKMP] World flag synced: ${key}`);
      }
      broadcast(buf, ws);
      return;
    }

    if (ptype === PKT_STATE || ptype === PKT_ENEMY_HIT || ptype === PKT_ENEMY_SYNC || ptype === PKT_CHAT) {
      const relayed = Buffer.from(buf);
      relayed.writeUInt8(info.id, 6);
      broadcast(relayed, ws);
    }
  });

  ws.on('close', () => {
    const info = clients.get(ws);
    if (info) {
      console.log(`[HKMP] Disconnected: ${info.name} (id=${info.id})`);
      clients.delete(ws);
    }
  });

  ws.on('error', () => {
    clients.delete(ws);
  });
});

// Keep WebSocket connections alive through Render's reverse proxy
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.readyState === 1) ws.ping();
  }
}, 25000);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[HKMP Render Server] Listening on port ${PORT} (HTTP /health + WebSocket)`);
});

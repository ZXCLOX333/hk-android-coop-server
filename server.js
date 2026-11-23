const http = require('node:http');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 10000);
const MAGIC = 0x504d4b48;
const VERSION = 2;
const HELLO = 1, WELCOME = 2, PING = 4, PONG = 5, BYE = 6, WORLD = 9;
const sizes = new Map([[3, 111], [7, 129], [8, 136], [9, 122], [10, 160]]);
const MAX_PLAYERS = 7;
const MAX_ROOMS = 1024;
const ROOM_TTL = 30 * 60 * 1000;
const ALLOWED_NAME_BYTES = 23;
const legacyRoom = newRoom();
const accountSecret = loadAccountSecret();

function loadAccountSecret() {
  if (process.env.HKMP_ACCOUNT_SECRET) return process.env.HKMP_ACCOUNT_SECRET;
  const filename = path.resolve(process.env.HKMP_ACCOUNT_KEY_FILE || '.hkmp-account-key');
  try {
    fs.writeFileSync(filename, crypto.randomBytes(32).toString('hex'), { flag: 'wx', mode: 0o600 });
  } catch (error) {
    if (error.code !== 'EEXIST') throw error;
  }
  return fs.readFileSync(filename, 'utf8').trim();
}

function signature(payload) {
  return crypto.createHmac('sha256', accountSecret).update(payload).digest();
}

function profileToken(name) {
  const payload = Buffer.from(JSON.stringify({ id: crypto.randomUUID(), name })).toString('base64url');
  return payload + '.' + signature(payload).toString('base64url');
}

function verifyAccount(auth) {
  if (!auth || !/^Bearer [A-Za-z0-9_-]{1,256}\.[A-Za-z0-9_-]{43}$/.test(auth)) return null;
  const [payload, encodedSignature] = auth.slice(7).split('.');
  const actual = Buffer.from(encodedSignature, 'base64url');
  const expected = signature(payload);
  if (actual.length !== expected.length || !crypto.timingSafeEqual(actual, expected)) return null;
  try { return JSON.parse(Buffer.from(payload, 'base64url').toString('utf8')); }
  catch { return null; }
}

function createProfile(req, res) {
  let length = 0;
  const parts = [];
  req.on('data', (chunk) => {
    length += chunk.length;
    if (length > 512) {
      parts.length = 0;
      if (!res.headersSent) res.writeHead(413).end();
    } else {
      parts.push(chunk);
    }
  });
  req.on('end', () => {
    if (length > 512) return;
    const name = Buffer.concat(parts).toString('utf8').trim();
    if (!name || Buffer.byteLength(name) > ALLOWED_NAME_BYTES || /[\x00-\x1f\x7f]/.test(name)) {
      res.writeHead(400).end();
      return;
    }
    res.writeHead(201, { 'Content-Type': 'text/plain', 'Cache-Control': 'no-store' });
    res.end(profileToken(name));
  });
}
const rooms = new Map();

function newRoom() {
  return { sockets: new Map(), world: new Map(), lastActive: Date.now() };
}

function fixedString(buf, offset, size) {
  const field = buf.subarray(offset, offset + size);
  const end = field.indexOf(0);
  return field.toString('utf8', 0, end < 0 ? field.length : end);
}

function welcome(id) {
  const buf = Buffer.alloc(33);
  buf.writeUInt32LE(MAGIC, 0);
  buf[4] = VERSION;
  buf[5] = WELCOME;
  buf[8] = id;
  buf.write('Relay', 9);
  return buf;
}

function send(ws, buf, transient = false) {
  if (ws.readyState !== WebSocket.OPEN) return;
  if (ws.bufferedAmount > 65536) {
    if (!transient) ws.terminate();
    return;
  }
  ws.send(buf);
}

const server = http.createServer((req, res) => {
  if (req.method === 'POST' && req.url === '/profiles') return createProfile(req, res);
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, roomsVersion: 1, rooms: rooms.size }));
  } else {
    res.writeHead(404);
    res.end();
  }
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 1024 });
wss.on('headers', (headers) => headers.push('X-HKMP-Rooms: 1'));

function reject(socket, status) {
  socket.end('HTTP/1.1 ' + status + '\r\nConnection: close\r\nX-HKMP-Rooms: 1\r\nContent-Length: 0\r\n\r\n');
}

server.on('upgrade', (req, socket, head) => {
  const match = /^\/room\/([A-HJ-NP-Z2-9]{8})(\?create=1)?$/.exec(req.url);
  let room = legacyRoom;
  let account = null;
  if (match) {
    account = verifyAccount(req.headers.authorization);
    if (!account) return reject(socket, '401 Unauthorized');
    const [, code, create] = match;
    room = rooms.get(code);
    if (create && room) return reject(socket, '409 Conflict');
    if (!create && !room) return reject(socket, '404 Not Found');
    if (!room && rooms.size >= MAX_ROOMS) return reject(socket, '429 Too Many Requests');
  } else if (req.url !== '/') {
    return reject(socket, '404 Not Found');
  }
  if (room && room.sockets.size >= MAX_PLAYERS) return reject(socket, '429 Too Many Requests');
  wss.handleUpgrade(req, socket, head, (ws) => {
    if (!room) {
      room = newRoom();
      rooms.set(match[1], room);
    }
    connect(ws, room, account);
  });
});

function connect(ws, room, account) {
  const used = new Set(room.sockets.values());
  let id = 1;
  while (used.has(id)) id++;
  room.sockets.set(ws, id);
  room.lastActive = Date.now();
  let joined = false;
  let alive = true;
  ws.heartbeat = () => {
    if (!alive) return ws.terminate();
    alive = false;
    ws.ping();
  };
  ws.on('pong', () => { alive = true; });
  ws.on('error', () => ws.terminate());
  ws.on('close', () => {
    room.sockets.delete(ws);
    room.lastActive = Date.now();
  });
  ws.on('message', (buf, binary) => {
    if (!binary || !Buffer.isBuffer(buf) || buf.length < 8 ||
        buf.readUInt32LE(0) !== MAGIC || buf[4] !== VERSION) return;
    const type = buf[5];
    if (type === HELLO && buf.length === 32) {
      send(ws, welcome(id));
      if (!joined) {
        joined = true;
        for (const flag of room.world.values()) send(ws, flag);
      }
      return;
    }
    if (!joined) return;
    if (type === BYE && buf.length === 8) { ws.close(); return; }
    if (type === PING && buf.length === 16) {
      buf[5] = PONG;
      buf[6] = 0;
      send(ws, buf);
      return;
    }
    if (buf.length !== sizes.get(type)) return;
    buf[6] = id;
    if (account && (type === 3 || type === 10)) {
      const nameOffset = type === 3 ? 87 : 8;
      buf.fill(0, nameOffset, nameOffset + 24);
      buf.write(account.name, nameOffset, 23, 'utf8');
    }
    if (type === WORLD) {
      const scene = fixedString(buf, 10, 48);
      const flag = fixedString(buf, 58, 64);
      if (!scene || !flag) return;
      const key = scene + '|' + flag;
      if (room.world.size < 4096 || room.world.has(key)) room.world.set(key, Buffer.from(buf));
    }
    for (const peer of room.sockets.keys()) {
      if (peer !== ws) send(peer, buf, type === 3 || type === 8);
    }
  });
}

setInterval(() => {
  for (const ws of wss.clients) ws.heartbeat();
  const expired = Date.now() - ROOM_TTL;
  for (const [code, room] of rooms) {
    if (!room.sockets.size && room.lastActive < expired) rooms.delete(code);
  }
}, 25000).unref();

server.listen(PORT, '0.0.0.0', () => console.log('HKMP relay listening on ' + server.address().port));


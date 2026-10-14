const http = require("http");
const fs = require("fs");
const path = require("path");
const os = require("os");
const crypto = require("crypto");
const QRCode = require("qrcode");
const { WebSocket, WebSocketServer } = require("ws");

const ROOT = __dirname;
const PORT = Number(process.env.PORT || 8787);
const GAME_FILE = process.env.GAME_FILE || "FTTS - FLY TO THE SKY.html";
const GAME_NAME = process.env.GAME_NAME || path.basename(GAME_FILE, path.extname(GAME_FILE));
const PUBLIC_ORIGIN = process.env.PUBLIC_ORIGIN ? new URL(process.env.PUBLIC_ORIGIN).origin : "";
const ALLOWED_ORIGINS = new Set([PUBLIC_ORIGIN, ...(process.env.ALLOWED_ORIGINS || "").split(",")]
  .filter(Boolean).map((origin) => new URL(origin.trim()).origin));
const IS_PUBLIC = process.env.NODE_ENV === "production" || ALLOWED_ORIGINS.size > 0;
if (process.env.NODE_ENV === "production" && !PUBLIC_ORIGIN) {
  throw new Error("Set PUBLIC_ORIGIN to the HTTPS game address before publishing.");
}
const MAX_PLAYERS = 6;
const MAX_ROOMS = 100;
const MAX_VOICE_MESSAGE_BYTES = 12 * 1024 * 1024;
const MAX_STORED_VOICE_BYTES = 64 * 1024 * 1024;
const rooms = new Map();
const voiceMessages = new Map();
const webSockets = new WebSocketServer({ noServer: true, maxPayload: 8192, perMessageDeflate: false });

function originAllowed(req) {
  if (!req.headers.origin) return true;
  if (IS_PUBLIC) return ALLOWED_ORIGINS.has(req.headers.origin);
  return req.headers.origin === `http://${req.headers.host}`;
}

const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".mp3": "audio/mpeg",
  ".m4a": "audio/mp4",
  ".svg": "image/svg+xml; charset=utf-8"
};

function localAddress() {
  for (const entries of Object.values(os.networkInterfaces())) {
    for (const entry of entries || []) {
      if (entry.family === "IPv4" && !entry.internal) return entry.address;
    }
  }
  return "127.0.0.1";
}

function roomCode() {
  return crypto.randomBytes(3).toString("hex").toUpperCase();
}

function cleanName(name) {
  const text = String(name || "")
    .replace(/[<>]/g, "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 18);
  return text || "New Pilot";
}

function cleanChatText(text) {
  return String(text || "")
    .replace(/[<>]/g, "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 160);
}

function isLoopbackAddress(address) {
  return address === "::1" || address === "127.0.0.1" || address === "::ffff:127.0.0.1";
}

function markRoomHost(room, client) {
  room.hostId = client.id;
  for (const player of room.clients) player.isHost = player === client;
  client.isHost = true;
}

function isRoomHost(room, client) {
  return client.isHost || room.hostId === client.id;
}

function sendToRoomHost(room, data, except = null) {
  let sent = 0;
  for (const client of room.clients) {
    if (client !== except && isRoomHost(room, client)) {
      sendJson(client, data);
      sent += 1;
    }
  }
  return sent;
}

function sendJson(client, data) {
  if (client.closed || client.socket.readyState !== WebSocket.OPEN) return;
  if (client.socket.bufferedAmount > 1024 * 1024) return closeClient(client);
  client.socket.send(JSON.stringify(data));
}

function publicClient(client) {
  return {
    id: client.id,
    name: client.name,
    color: client.color,
    isHost: Boolean(client.isHost),
    state: client.state || null
  };
}

function broadcast(room, data, except = null) {
  for (const client of room.clients) {
    if (client !== except) sendJson(client, data);
  }
}

function roster(room) {
  return [...room.clients].map(publicClient);
}

function closeClient(client) {
  if (client.closed) return;
  client.closed = true;
  const room = rooms.get(client.room);
  if (room) {
    room.clients.delete(client);
    if (room.hostId === client.id) {
      room.hostId = null;
      const nextHost = room.clients.values().next().value;
      if (nextHost) markRoomHost(room, nextHost);
    }
    broadcast(room, { type: "roster", players: roster(room) });
    if (room.clients.size === 0) rooms.delete(client.room);
  }
  try { client.socket.terminate(); } catch {}
}

function handleMessage(client, text) {
  const now = Date.now();
  if (now - client.messageWindow >= 1000) {
    client.messageWindow = now;
    client.messageCount = 0;
  }
  if (++client.messageCount > 100) return closeClient(client);
  let msg;
  try { msg = JSON.parse(text); } catch { return; }
  if (!msg || typeof msg !== "object" || Array.isArray(msg)) return;
  const room = rooms.get(client.room);
  if (!room) return;

  if (msg.type === "hello") {
    client.name = cleanName(msg.name || client.name);
    sendJson(client, { type: "welcome", id: client.id, room: client.room, voiceToken: client.voiceToken, players: roster(room) });
    broadcast(room, { type: "roster", players: roster(room) });
    return;
  }

  if (msg.type === "state") {
    if (!msg.state || typeof msg.state !== "object" || Array.isArray(msg.state)) return;
    client.state = msg.state;
    broadcast(room, { type: "peer", player: publicClient(client) }, client);
    return;
  }

  if (msg.type === "prelaunch") {
    if (!isRoomHost(room, client)) return;
    const deadline = Number(msg.deadline) || Date.now() + 30000;
    broadcast(room, { type: "prelaunch", from: client.id, deadline }, client);
    return;
  }

  if (msg.type === "comment") {
    const text = cleanChatText(msg.text);
    if (!text) return;
    const data = { type: "comment", from: client.id, name: client.name, isHost: isRoomHost(room, client), text };
    sendJson(client, data);
    if (isRoomHost(room, client)) {
      broadcast(room, data, client);
    } else {
      const sent = sendToRoomHost(room, data, client);
      if (!sent) broadcast(room, data, client);
    }
    return;
  }

  if (msg.type === "chat") {
    const text = cleanChatText(msg.text);
    if (!text) return;
    const data = { type: "chat", from: client.id, name: client.name, isHost: isRoomHost(room, client), text };
    sendJson(client, data);
    broadcast(room, data, client);
    return;
  }

  if (msg.type === "issueReport") {
    const text = cleanChatText(msg.text);
    if (!text) return;
    const data = { type: "issueReport", from: client.id, name: client.name, text };
    sendJson(client, { type: "issueAck", from: client.id });
    if (isRoomHost(room, client)) sendJson(client, data);
    else {
      const sent = sendToRoomHost(room, data, client);
      if (!sent) sendJson(client, data);
    }
    return;
  }

  if (msg.type === "voiceRequest") {
    if (msg.mode !== "message") return;
    const mode = "message";
    const data = { type: "voiceRequest", from: client.id, name: client.name, mode };
    if (isRoomHost(room, client)) sendJson(client, data);
    else {
      const sent = sendToRoomHost(room, data, client);
      if (!sent) sendJson(client, { type: "voicePermission", from: client.id, allowed: false, mode });
    }
    return;
  }

  if (msg.type === "voicePermission") {
    if (!isRoomHost(room, client) || msg.mode !== "message") return;
    const target = [...room.clients].find((player) => player.id === msg.to);
    if (target) {
      target.voiceMessageAllowed = Boolean(msg.allowed);
      sendJson(target, { type: "voicePermission", from: client.id, allowed: Boolean(msg.allowed), mode: "message" });
    }
    return;
  }

  if (msg.type === "commentSkip") {
    if (!isRoomHost(room, client)) return;
    const data = { type: "commentSkip", from: client.id, name: client.name };
    sendJson(client, data);
    broadcast(room, data, client);
    return;
  }

  if (msg.type === "start") {
    if (!isRoomHost(room, client)) return;
    broadcast(room, { type: "start", from: client.id });
    return;
  }

  if (msg.type === "restart") {
    if (!isRoomHost(room, client)) return;
    broadcast(room, { type: "restart", from: client.id, deadline: msg.deadline });
    return;
  }

  if (msg.type === "lobby") {
    if (!isRoomHost(room, client)) return;
    broadcast(room, { type: "lobby", from: client.id });
    return;
  }

  if (["pause", "resume"].includes(msg.type)) {
    if (!isRoomHost(room, client)) return;
    broadcast(room, { type: msg.type, from: client.id });
  }
}

function acceptWebSocket(req, socket) {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const roomName = (url.searchParams.get("room") || "FTTS").slice(0, 32);
  const room = rooms.get(roomName) || { clients: new Set(), hostId: null };
  if (!rooms.has(roomName) && rooms.size >= MAX_ROOMS) {
    socket.close(1013, "Server busy");
    return;
  }
  rooms.set(roomName, room);
  if (room.clients.size >= MAX_PLAYERS) {
    socket.close(4001, "Room full");
    return;
  }

  const palette = ["#7bdff2", "#ffd166", "#8ff8bf", "#ff6b6b", "#f72585", "#b8c0ff"];
  // Public reverse proxies can all appear to be localhost; they must not steal host status.
  const hostByAddress = !IS_PUBLIC && isLoopbackAddress(req.socket.remoteAddress);
  const client = {
    id: crypto.randomBytes(4).toString("hex"),
    voiceToken: crypto.randomBytes(24).toString("hex"),
    voiceMessageAllowed: false,
    name: "New Pilot",
    color: palette[room.clients.size % palette.length],
    isHost: false,
    room: roomName,
    socket,
    alive: true,
    messageWindow: Date.now(),
    messageCount: 0,
    uploading: false,
    voiceWindow: Date.now(),
    voiceUploads: 0,
    closed: false,
    state: null
  };
  room.clients.add(client);
  if (hostByAddress || !room.hostId) markRoomHost(room, client);
  socket.on("message", (chunk, isBinary) => { if (!isBinary) handleMessage(client, chunk.toString("utf8")); });
  socket.on("pong", () => { client.alive = true; });
  socket.on("close", () => closeClient(client));
  socket.on("error", () => closeClient(client));
}

function qrSvg(text) {
  const size = 37;
  const quiet = 4;
  const dataCodewords = 108;
  const eccCodewords = 26;
  const modules = Array.from({ length: size }, () => Array(size).fill(false));
  const reserved = Array.from({ length: size }, () => Array(size).fill(false));

  const setFunction = (r, c, value) => {
    if (r < 0 || c < 0 || r >= size || c >= size) return;
    modules[r][c] = value;
    reserved[r][c] = true;
  };

  const finder = (r, c) => {
    for (let y = -1; y <= 7; y += 1) {
      for (let x = -1; x <= 7; x += 1) {
        const rr = r + y;
        const cc = c + x;
        const dark = x >= 0 && x <= 6 && y >= 0 && y <= 6 &&
          (x === 0 || x === 6 || y === 0 || y === 6 || (x >= 2 && x <= 4 && y >= 2 && y <= 4));
        setFunction(rr, cc, dark);
      }
    }
  };

  finder(0, 0);
  finder(0, size - 7);
  finder(size - 7, 0);
  for (let i = 8; i < size - 8; i += 1) {
    setFunction(6, i, i % 2 === 0);
    setFunction(i, 6, i % 2 === 0);
  }
  for (let y = -2; y <= 2; y += 1) {
    for (let x = -2; x <= 2; x += 1) {
      const m = Math.max(Math.abs(x), Math.abs(y));
      setFunction(30 + y, 30 + x, m !== 1);
    }
  }
  setFunction(29, 8, true);

  const reserveFormat = () => {
    for (let i = 0; i <= 5; i += 1) setFunction(8, i, false);
    setFunction(8, 7, false);
    setFunction(8, 8, false);
    setFunction(7, 8, false);
    for (let i = 9; i < 15; i += 1) setFunction(14 - i, 8, false);
    for (let i = 0; i < 8; i += 1) setFunction(size - 1 - i, 8, false);
    for (let i = 8; i < 15; i += 1) setFunction(8, size - 15 + i, false);
  };
  reserveFormat();

  const maxBits = dataCodewords * 8;
  const maxDataBytes = Math.floor((maxBits - 4 - 8 - 4) / 8);
  const bytes = Buffer.from(text, "utf8").subarray(0, maxDataBytes);
  const bits = [];
  const push = (value, count) => {
    for (let i = count - 1; i >= 0; i -= 1) bits.push((value >>> i) & 1);
  };
  push(0b0100, 4);
  push(Math.min(bytes.length, 255), 8);
  for (const byte of bytes.subarray(0, 255)) push(byte, 8);
  for (let i = 0; i < 4 && bits.length < maxBits; i += 1) bits.push(0);
  while (bits.length % 8) bits.push(0);
  const data = [];
  for (let i = 0; i < bits.length; i += 8) {
    data.push(bits.slice(i, i + 8).reduce((n, bit) => (n << 1) | bit, 0));
  }
  for (let pad = 0; data.length < dataCodewords; pad += 1) data.push(pad % 2 ? 0x11 : 0xec);

  const exp = new Array(512);
  const log = new Array(256);
  let x = 1;
  for (let i = 0; i < 255; i += 1) {
    exp[i] = x;
    log[x] = i;
    x <<= 1;
    if (x & 0x100) x ^= 0x11d;
  }
  for (let i = 255; i < 512; i += 1) exp[i] = exp[i - 255];
  const mul = (a, b) => (a && b ? exp[log[a] + log[b]] : 0);
  let gen = [1];
  for (let i = 0; i < eccCodewords; i += 1) {
    const next = new Array(gen.length + 1).fill(0);
    for (let j = 0; j < gen.length; j += 1) {
      next[j] ^= gen[j];
      next[j + 1] ^= mul(gen[j], exp[i]);
    }
    gen = next;
  }
  const ecc = new Array(eccCodewords).fill(0);
  for (const byte of data) {
    const factor = byte ^ ecc.shift();
    ecc.push(0);
    for (let i = 0; i < eccCodewords; i += 1) ecc[i] ^= mul(gen[i + 1], factor);
  }

  const all = data.concat(ecc);
  const dataBits = [];
  for (const byte of all) for (let i = 7; i >= 0; i -= 1) dataBits.push((byte >>> i) & 1);
  let bit = 0;
  let upward = true;
  for (let col = size - 1; col > 0; col -= 2) {
    if (col === 6) col -= 1;
    for (let i = 0; i < size; i += 1) {
      const row = upward ? size - 1 - i : i;
      for (let c = col; c >= col - 1; c -= 1) {
        if (reserved[row][c]) continue;
        const raw = bit < dataBits.length ? dataBits[bit] : 0;
        const mask = (row + c) % 2 === 0;
        modules[row][c] = Boolean(raw ^ mask);
        bit += 1;
      }
    }
    upward = !upward;
  }

  const formatBits = () => {
    const dataValue = (1 << 3) | 0;
    let value = dataValue << 10;
    const generator = 0x537;
    for (let i = 14; i >= 10; i -= 1) {
      if ((value >>> i) & 1) value ^= generator << (i - 10);
    }
    return ((dataValue << 10) | value) ^ 0x5412;
  };
  const fmt = formatBits();
  const fbit = (i) => Boolean((fmt >>> i) & 1);
  for (let i = 0; i <= 5; i += 1) setFunction(8, i, fbit(i));
  setFunction(8, 7, fbit(6));
  setFunction(8, 8, fbit(7));
  setFunction(7, 8, fbit(8));
  for (let i = 9; i < 15; i += 1) setFunction(14 - i, 8, fbit(i));
  for (let i = 0; i < 8; i += 1) setFunction(size - 1 - i, 8, fbit(i));
  for (let i = 8; i < 15; i += 1) setFunction(8, size - 15 + i, fbit(i));
  setFunction(size - 8, 8, true);

  const unit = 8;
  const outer = (size + quiet * 2) * unit;
  const rects = [];
  for (let r = 0; r < size; r += 1) {
    for (let c = 0; c < size; c += 1) {
      if (modules[r][c]) rects.push(`<rect x="${(c + quiet) * unit}" y="${(r + quiet) * unit}" width="${unit}" height="${unit}"/>`);
    }
  }
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${outer} ${outer}" shape-rendering="crispEdges"><rect width="100%" height="100%" fill="#fff"/><g fill="#000">${rects.join("")}</g></svg>`;
}

async function serve(req, res) {
  let url;
  try { url = new URL(req.url, `http://${req.headers.host}`); }
  catch { res.writeHead(400); res.end("Invalid URL"); return; }
  res.setHeader("x-content-type-options", "nosniff");
  res.setHeader("referrer-policy", "no-referrer");
  if (!originAllowed(req)) {
    res.writeHead(403);
    res.end("Origin not allowed");
    req.resume();
    return;
  }
  if (req.headers.origin) {
    res.setHeader("access-control-allow-origin", req.headers.origin);
    res.setHeader("vary", "Origin");
    res.setHeader("access-control-expose-headers", "Content-Length, Content-Range, Accept-Ranges");
  }
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "access-control-allow-methods": "GET, HEAD, POST, OPTIONS",
      "access-control-allow-headers": "Content-Type, X-Voice-Token, Range",
      "access-control-max-age": "600"
    });
    res.end();
    return;
  }
  if (url.pathname === "/healthz") {
    res.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  if (req.method === "POST" && url.pathname === "/voice-message") {
    const reply = (status, data) => {
      res.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
      res.end(JSON.stringify(data));
    };
    const room = rooms.get(url.searchParams.get("room"));
    const sender = room && [...room.clients].find((client) => client.id === url.searchParams.get("sender") && client.voiceToken === req.headers["x-voice-token"]);
    if (!sender || sender.closed) {
      reply(401, { ok: false, error: "Reconnect to the room before sending." });
      req.resume();
      return;
    }
    if (!isRoomHost(room, sender) && !sender.voiceMessageAllowed) {
      reply(403, { ok: false, error: "Ask the host to allow voice messages first." });
      req.resume();
      return;
    }
    const recipient = [...room.clients].find((client) => client.id === url.searchParams.get("to") && client !== sender && !client.closed);
    if (!recipient) {
      reply(409, { ok: false, error: "That player is offline. Choose an online player." });
      req.resume();
      return;
    }
    const mime = String(req.headers["content-type"] || "").split(";")[0].trim().toLowerCase();
    if (!/^audio\/[a-z0-9.+-]+$/.test(mime)) {
      reply(415, { ok: false, error: "Choose an audio recording." });
      req.resume();
      return;
    }
    if (Date.now() - sender.voiceWindow >= 60000) {
      sender.voiceWindow = Date.now();
      sender.voiceUploads = 0;
    }
    if (sender.uploading || sender.voiceUploads >= 30) {
      reply(429, { ok: false, error: "Wait a moment before sending another recording." });
      req.resume();
      return;
    }
    sender.uploading = true;
    sender.voiceUploads += 1;
    req.once("close", () => { sender.uploading = false; });
    const chunks = [];
    let total = 0;
    let tooLarge = false;
    req.on("data", (chunk) => {
      total += chunk.length;
      if (total > MAX_VOICE_MESSAGE_BYTES) {
        tooLarge = true;
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      if (tooLarge) {
        reply(413, { ok: false, error: "Voice message is too large (12 MB maximum)." });
        return;
      }
      const buffer = Buffer.concat(chunks);
      if (!buffer.length) {
        reply(400, { ok: false, error: "The recording is empty. Record again." });
        return;
      }
      if (sender.closed || recipient.closed || !room.clients.has(recipient)) {
        reply(409, { ok: false, error: "A player disconnected. Reconnect and try again." });
        return;
      }
      const id = crypto.randomBytes(12).toString("hex");
      let storedBytes = [...voiceMessages.values()].reduce((sum, message) => sum + message.buffer.length, 0);
      while (storedBytes + buffer.length > MAX_STORED_VOICE_BYTES && voiceMessages.size) {
        const oldest = voiceMessages.keys().next().value;
        storedBytes -= voiceMessages.get(oldest).buffer.length;
        voiceMessages.delete(oldest);
      }
      voiceMessages.set(id, { buffer, mime, createdAt: Date.now() });
      setTimeout(() => voiceMessages.delete(id), 30 * 60 * 1000).unref?.();
      const message = { type: "voiceMessage", id, from: sender.id, name: sender.name, to: recipient.id, toName: recipient.name, mime, size: buffer.length, url: `/voice-message/${id}` };
      sendJson(recipient, message);
      reply(200, { ok: true, delivered: 1, ...message });
    });
    req.on("error", () => {
      if (!res.headersSent) res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "Voice upload failed" }));
    });
    return;
  }

  if (["GET", "HEAD"].includes(req.method) && url.pathname.startsWith("/voice-message/")) {
    const id = path.basename(url.pathname);
    const message = voiceMessages.get(id);
    if (!message) {
      res.writeHead(404);
      res.end("Voice message not found");
      return;
    }
    const headers = {
      "content-type": message.mime,
      "content-length": message.buffer.length,
      "accept-ranges": "bytes",
      "cache-control": "no-store"
    };
    let start = 0;
    let end = message.buffer.length - 1;
    if (req.headers.range) {
      const match = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range);
      if (match && (match[1] || match[2])) {
        start = match[1] ? Number(match[1]) : Math.max(0, message.buffer.length - Number(match[2]));
        end = match[1] && match[2] ? Math.min(Number(match[2]), end) : end;
      }
      if (!match || !(match[1] || match[2]) || start > end || start < 0) {
        res.writeHead(416, { "content-range": `bytes */${message.buffer.length}` });
        res.end();
        return;
      }
      headers["content-range"] = `bytes ${start}-${end}/${message.buffer.length}`;
      headers["content-length"] = end - start + 1;
    }
    res.writeHead(req.headers.range ? 206 : 200, headers);
    res.end(req.method === "HEAD" ? undefined : message.buffer.subarray(start, end + 1));
    return;
  }

  if (url.pathname === "/room-info") {
    const publicOrigin = PUBLIC_ORIGIN || `http://${localAddress()}:${PORT}`;
    res.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
    res.end(JSON.stringify({ publicOrigin }));
    return;
  }

  if (url.pathname === "/qr.svg") {
    QRCode.toString(url.searchParams.get("text") || "", {
      type: "svg",
      errorCorrectionLevel: "M",
      margin: 4,
      width: 512,
      color: { dark: "#000000", light: "#ffffff" }
    }, (err, svg) => {
      if (err) {
        res.writeHead(500);
        res.end("QR error");
        return;
      }
      res.writeHead(200, { "content-type": TYPES[".svg"], "cache-control": "no-store" });
      res.end(svg);
    });
    return;
  }

  let filePath;
  try { filePath = decodeURIComponent(url.pathname); }
  catch {
    res.writeHead(400);
    res.end("Invalid path");
    return;
  }
  if (filePath === "/") filePath = `/${GAME_FILE}`;
  filePath = path.normalize(filePath).replace(/^(\.\.[/\\])+/, "");
  // Only game assets are public; server settings and unrelated downloads stay private.
  if (filePath !== `/${GAME_FILE}` && !filePath.startsWith("/assets/")) {
    res.writeHead(404);
    res.end("Not found");
    return;
  }
  const fullPath = path.join(ROOT, filePath);
  if (!fullPath.startsWith(ROOT)) {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }
  fs.stat(fullPath, (err, stat) => {
    if (err || !stat.isFile()) {
      res.writeHead(404);
      res.end("Not found");
      return;
    }
    const headers = {
      "content-type": TYPES[path.extname(fullPath).toLowerCase()] || "application/octet-stream",
      "content-length": stat.size,
      "accept-ranges": "bytes",
      "cache-control": "no-cache"
    };
    let start = 0;
    let end = stat.size - 1;
    let status = 200;
    if (req.headers.range) {
      const match = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range);
      if (match && (match[1] || match[2])) {
        start = match[1] ? Number(match[1]) : Math.max(0, stat.size - Number(match[2]));
        end = match[1] && match[2] ? Math.min(Number(match[2]), end) : end;
      }
      if (!match || !(match[1] || match[2]) || !Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start > end || start < 0) {
        res.writeHead(416, { "content-range": `bytes */${stat.size}`, "content-length": 0 });
        res.end();
        return;
      }
      status = 206;
      headers["content-range"] = `bytes ${start}-${end}/${stat.size}`;
      headers["content-length"] = end - start + 1;
    }
    res.writeHead(status, headers);
    if (req.method === "HEAD" || stat.size === 0) {
      res.end();
      return;
    }
    // Stream only the requested section so browsers can seek music.
    const stream = fs.createReadStream(fullPath, { start, end });
    stream.on("error", () => res.destroy());
    res.on("close", () => stream.destroy());
    stream.pipe(res);
  });
}

const server = http.createServer((req, res) => {
  serve(req, res).catch(() => {
    if (!res.headersSent) res.writeHead(500);
    res.end("Server error");
  });
});
server.on("upgrade", (req, socket, head) => {
  let pathname;
  try { pathname = new URL(req.url, `http://${req.headers.host}`).pathname; }
  catch { socket.destroy(); return; }
  if (pathname !== "/ws" || !originAllowed(req)) {
    socket.write("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
    socket.destroy();
    return;
  }
  webSockets.handleUpgrade(req, socket, head, (ws) => acceptWebSocket(req, ws));
});

const heartbeat = setInterval(() => {
  for (const room of rooms.values()) {
    for (const client of room.clients) {
      if (!client.alive) { closeClient(client); continue; }
      client.alive = false;
      if (client.socket.readyState === WebSocket.OPEN) client.socket.ping();
    }
  }
}, 30000);
heartbeat.unref();
server.on("close", () => clearInterval(heartbeat));

server.listen(PORT, "0.0.0.0", () => {
  const room = roomCode();
  const address = localAddress();
  console.log(`${GAME_NAME} multiplayer server running`);
  console.log(`Open on this computer: http://localhost:${PORT}/?room=${room}`);
  console.log(`QR / iPhone link:     http://${address}:${PORT}/?room=${room}`);
  console.log("Keep this terminal open while playing.");
});

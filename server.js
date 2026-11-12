// Spin Loop - online 1v1 aktarma sunucusu (güvenli sürüm)
// Görevi: oda kodu vermek ve iki oyuncu arasındaki oyun mesajlarını taşımak.
// Oyunun kendisini odayı açan telefon yönetir; sunucu oyun mantığı çalıştırmaz.
// Hiçbir veri diske yazılmaz; her şey yalnızca bellekte ve oda süresince tutulur.

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 10000;

// --- Sınırlar ---
const ROOM_TTL_MS = 10 * 60 * 1000;     // eşleşmeyen oda 10 dk sonra silinir
const IDLE_MS = 3 * 60 * 1000;          // odaya girmeyen bağlantı 3 dk sonra kapatılır
const MAX_PAYLOAD = 4 * 1024;           // tek mesaj en fazla 4 KB
const MAX_CONN_TOTAL = 1000;            // sunucuya aynı anda en fazla bağlantı
const MAX_CONN_PER_IP = 8;              // aynı IP'den en fazla bağlantı
const MAX_ROOMS_PER_IP = 3;             // aynı IP'nin açabileceği bekleyen oda
const MSG_RATE = 60;                    // saniyede ortalama mesaj
const MSG_BURST = 120;                  // kısa süreli patlama payı
const JOIN_FAILS_PER_CONN = 5;          // bir bağlantıda en fazla yanlış kod
const JOIN_FAILS_PER_IP = 20;           // bir IP için 10 dakikada en fazla yanlış kod
const JOIN_WINDOW_MS = 10 * 60 * 1000;

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain", "X-Content-Type-Options": "nosniff" });
  res.end("Spin Loop server OK");
});
server.headersTimeout = 10000;
server.requestTimeout = 15000;

const wss = new WebSocketServer({ server, maxPayload: MAX_PAYLOAD, perMessageDeflate: false });
const rooms = new Map();        // kod -> { host, guest, created, ip }
const connPerIp = new Map();    // ip -> bağlantı sayısı
const joinFails = new Map();    // ip -> [zaman damgaları]

function clientIp(req) {
  // Render gibi barındırıcılar gerçek IP'yi bu başlıkla iletir
  const xf = req.headers["x-forwarded-for"];
  if (typeof xf === "string" && xf.length > 0) return xf.split(",")[0].trim();
  return req.socket.remoteAddress || "?";
}

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function newCode() {
  let c;
  do {
    c = String(1000 + Math.floor(Math.random() * 9000));
  } while (rooms.has(c));
  return c;
}

function roomsOfIp(ip) {
  let n = 0;
  for (const r of rooms.values()) if (r.ip === ip && !r.guest) n++;
  return n;
}

function peerOf(ws) {
  const room = rooms.get(ws.room);
  if (!room) return null;
  return ws.role === "host" ? room.guest : room.host;
}

function leaveRoom(ws) {
  if (!ws.room) return;
  const room = rooms.get(ws.room);
  if (room) {
    const peer = ws.role === "host" ? room.guest : room.host;
    send(peer, { t: "left" });
    if (peer) { peer.room = null; peer.role = null; }
    rooms.delete(ws.room);
  }
  ws.room = null;
  ws.role = null;
}

function tooManyJoinFails(ip) {
  const now = Date.now();
  const list = (joinFails.get(ip) || []).filter((t) => now - t < JOIN_WINDOW_MS);
  joinFails.set(ip, list);
  return list.length >= JOIN_FAILS_PER_IP;
}

function noteJoinFail(ip) {
  const list = joinFails.get(ip) || [];
  list.push(Date.now());
  joinFails.set(ip, list);
}

// Karşı tarafa yalnızca bu mesajlar, yalnızca bu rollerden geçer.
// Sunucu mesajı kendisi yeniden oluşturur; bilinmeyen alanlar asla iletilmez.
function sanitize(msg, role) {
  switch (msg.t) {
    case "hello": {
      const skin = typeof msg.skin === "string" ? msg.skin.slice(0, 24).replace(/[^a-z0-9_]/gi, "") : "sari";
      const tr = Number.isFinite(msg.tr) ? Math.max(0, Math.min(100000, Math.floor(msg.tr))) : 0;
      return { t: "hello", skin, tr };
    }
    case "tap":
      return role === "guest" ? { t: "tap" } : null;
    case "pw":
      return role === "guest" ? { t: "pw" } : null;
    case "s":
      // Oyun durumu yalnızca oyunu yöneten taraftan (host) gelir
      if (role !== "host" || !Array.isArray(msg.d) || msg.d.length > 40) return null;
      return { t: "s", d: msg.d };
    default:
      return null;
  }
}

wss.on("connection", (ws, req) => {
  const ip = clientIp(req);
  const count = connPerIp.get(ip) || 0;
  if (wss.clients.size > MAX_CONN_TOTAL || count >= MAX_CONN_PER_IP) {
    ws.close(1013, "busy");
    return;
  }
  connPerIp.set(ip, count + 1);

  ws.ip = ip;
  ws.room = null;
  ws.role = null;
  ws.isAlive = true;
  ws.joinFails = 0;
  ws.tokens = MSG_BURST;
  ws.lastRefill = Date.now();
  ws.lastActive = Date.now();
  ws.on("pong", () => (ws.isAlive = true));
  ws.on("error", () => {});

  ws.on("message", (data, isBinary) => {
    try {
      if (isBinary) return ws.terminate();

      // Hız sınırı (token kovası)
      const now = Date.now();
      ws.tokens = Math.min(MSG_BURST, ws.tokens + ((now - ws.lastRefill) / 1000) * MSG_RATE);
      ws.lastRefill = now;
      if (ws.tokens < 1) return ws.terminate();
      ws.tokens -= 1;
      ws.lastActive = now;

      let msg;
      try {
        msg = JSON.parse(data.toString());
      } catch {
        return;
      }
      if (!msg || typeof msg !== "object" || Array.isArray(msg) || typeof msg.t !== "string") return;

      if (msg.t === "host") {
        leaveRoom(ws);
        if (roomsOfIp(ip) >= MAX_ROOMS_PER_IP) return send(ws, { t: "error", reason: "Too many rooms" });
        const code = newCode();
        rooms.set(code, { host: ws, guest: null, created: now, ip });
        ws.room = code;
        ws.role = "host";
        send(ws, { t: "created", code });
        return;
      }

      if (msg.t === "join") {
        if (tooManyJoinFails(ip) || ws.joinFails >= JOIN_FAILS_PER_CONN) {
          send(ws, { t: "error", reason: "Too many tries. Wait a few minutes." });
          return ws.close();
        }
        const code = typeof msg.code === "string" || typeof msg.code === "number" ? String(msg.code) : "";
        const room = /^\d{4}$/.test(code) ? rooms.get(code) : null;
        if (!room || room.host === ws) {
          ws.joinFails++;
          noteJoinFail(ip);
          return send(ws, { t: "error", reason: "Room not found" });
        }
        if (room.guest) return send(ws, { t: "error", reason: "Room is full" });
        leaveRoom(ws);
        room.guest = ws;
        ws.room = code;
        ws.role = "guest";
        send(ws, { t: "joined", code });
        send(room.host, { t: "start" });
        return;
      }

      if (msg.t === "leave") {
        leaveRoom(ws);
        return;
      }

      const clean = sanitize(msg, ws.role);
      if (!clean) return;
      const peer = peerOf(ws);
      if (peer && peer.readyState === peer.OPEN) peer.send(JSON.stringify(clean));
    } catch (e) {
      // Hiçbir istemci mesajı sunucuyu çökertemez
      console.error("message error:", e && e.message);
    }
  });

  ws.on("close", () => {
    leaveRoom(ws);
    const n = (connPerIp.get(ip) || 1) - 1;
    if (n <= 0) connPerIp.delete(ip);
    else connPerIp.set(ip, n);
  });
});

// Kopan bağlantıları, boşta bekleyenleri ve eşleşmeyen eski odaları temizle
setInterval(() => {
  const now = Date.now();
  for (const ws of wss.clients) {
    if (!ws.isAlive || (!ws.room && now - ws.lastActive > IDLE_MS)) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
  for (const [code, room] of rooms) {
    if (!room.guest && now - room.created > ROOM_TTL_MS) {
      send(room.host, { t: "error", reason: "Room expired" });
      if (room.host) { room.host.room = null; room.host.role = null; }
      rooms.delete(code);
    }
  }
  for (const [ip, list] of joinFails) {
    const keep = list.filter((t) => now - t < JOIN_WINDOW_MS);
    if (keep.length === 0) joinFails.delete(ip);
    else joinFails.set(ip, keep);
  }
}, 25000);

process.on("uncaughtException", (e) => console.error("uncaught:", e && e.message));

server.listen(PORT, () => console.log("Spin Loop server listening on " + PORT));

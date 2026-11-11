// Spin Loop - online 1v1 aktarma sunucusu
// Görevi: oda kodu vermek ve iki oyuncu arasındaki mesajları taşımak.
// Oyunun kendisini odayı açan telefon yönetir; sunucu oyun mantığı çalıştırmaz.

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 10000;
const ROOM_TTL_MS = 10 * 60 * 1000; // eşleşmeyen oda 10 dk sonra silinir

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Spin Loop server OK");
});

const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
const rooms = new Map(); // kod -> { host, guest, created }

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function newCode() {
  let c;
  do {
    c = String(Math.floor(1000 + Math.random() * 9000));
  } while (rooms.has(c));
  return c;
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
    if (peer) peer.room = null;
    rooms.delete(ws.room);
  }
  ws.room = null;
  ws.role = null;
}

wss.on("connection", (ws) => {
  ws.room = null;
  ws.role = null;
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));

  ws.on("message", (data) => {
    const text = data.toString();
    let msg;
    try {
      msg = JSON.parse(text);
    } catch {
      return;
    }

    if (msg.t === "host") {
      leaveRoom(ws);
      const code = newCode();
      rooms.set(code, { host: ws, guest: null, created: Date.now() });
      ws.room = code;
      ws.role = "host";
      send(ws, { t: "created", code });
      return;
    }

    if (msg.t === "join") {
      const code = String(msg.code || "");
      const room = rooms.get(code);
      if (!room) return send(ws, { t: "error", reason: "Room not found" });
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

    // Diğer tüm mesajlar (oyun durumu, dokunuşlar) karşı tarafa aynen iletilir.
    const peer = peerOf(ws);
    if (peer && peer.readyState === peer.OPEN) peer.send(text);
  });

  ws.on("close", () => leaveRoom(ws));
});

// Kopan bağlantıları ve eşleşmeyen eski odaları temizle
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (!room.guest && now - room.created > ROOM_TTL_MS) {
      send(room.host, { t: "error", reason: "Room expired" });
      if (room.host) room.host.room = null;
      rooms.delete(code);
    }
  }
}, 25000);

server.listen(PORT, () => console.log("Spin Loop server listening on " + PORT));

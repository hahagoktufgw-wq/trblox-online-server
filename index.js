const http = require("http");
const express = require("express");
const { WebSocketServer, WebSocket } = require("ws");
const crypto = require("crypto");

const app = express();
app.get("/", (_req, res) => {
  res.json({
    name: "TRBLOX Online Server",
    status: "online",
    version: "0.1.0",
    message: "TRBLOX multiplayer server is running."
  });
});
app.get("/health", (_req, res) => res.status(200).json({ ok: true }));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });
const rooms = new Map();

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}
function roomFor(id) {
  if (!rooms.has(id)) rooms.set(id, new Map());
  return rooms.get(id);
}
function broadcast(room, data, except) {
  for (const player of room.values()) {
    if (player.ws !== except) send(player.ws, data);
  }
}
function cleanRoom(id) {
  const room = rooms.get(id);
  if (room && room.size === 0) rooms.delete(id);
}

wss.on("connection", (ws) => {
  ws.playerId = crypto.randomUUID();
  ws.roomId = null;
  ws.playerName = "Oyuncu";
  ws.position = { x: 0, y: 0, z: 0 };

  send(ws, { type: "connected", playerId: ws.playerId });

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return send(ws, { type: "error", message: "Geçersiz mesaj." }); }
    if (!msg || typeof msg.type !== "string") return;

    if (msg.type === "join") {
      const roomId = String(msg.roomId || "lobby").replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 32) || "lobby";
      const name = String(msg.name || "Oyuncu").trim().slice(0, 18) || "Oyuncu";

      if (ws.roomId) {
        const oldRoom = rooms.get(ws.roomId);
        if (oldRoom) {
          oldRoom.delete(ws.playerId);
          broadcast(oldRoom, { type: "player-left", playerId: ws.playerId });
          cleanRoom(ws.roomId);
        }
      }

      ws.roomId = roomId;
      ws.playerName = name;
      ws.position = { x: 0, y: 0, z: 0 };
      const room = roomFor(roomId);
      room.set(ws.playerId, { ws, name, position: ws.position });

      send(ws, {
        type: "joined",
        roomId,
        playerId: ws.playerId,
        players: [...room.entries()]
          .filter(([id]) => id !== ws.playerId)
          .map(([id, p]) => ({ playerId: id, name: p.name, position: p.position }))
      });
      broadcast(room, {
        type: "player-joined",
        playerId: ws.playerId,
        name,
        position: ws.position
      }, ws);
      return;
    }

    if (msg.type === "move") {
      if (!ws.roomId || !msg.position) return;
      const { x, y, z } = msg.position;
      if (![x, y, z].every(v => typeof v === "number" && Number.isFinite(v) && Math.abs(v) < 100000)) return;
      ws.position = { x, y, z };
      const room = rooms.get(ws.roomId);
      if (!room) return;
      const player = room.get(ws.playerId);
      if (player) player.position = ws.position;
      broadcast(room, { type: "player-moved", playerId: ws.playerId, position: ws.position }, ws);
      return;
    }

    if (msg.type === "ping") send(ws, { type: "pong", time: Date.now() });
  });

  ws.on("close", () => {
    if (!ws.roomId) return;
    const room = rooms.get(ws.roomId);
    if (!room) return;
    room.delete(ws.playerId);
    broadcast(room, { type: "player-left", playerId: ws.playerId });
    cleanRoom(ws.roomId);
  });
});

const port = Number(process.env.PORT || 10000);
server.listen(port, "0.0.0.0", () => {
  console.log(`TRBLOX server listening on ${port}`);
});

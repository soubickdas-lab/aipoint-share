// aipoint-share — open mesh: every visitor sees every other visitor and can
// send files to anyone. Files are never stored: binary chunks are relayed
// sender-browser -> DO -> receiver-browser.

const ID_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
function genId() {
  const a = new Uint8Array(8);
  crypto.getRandomValues(a);
  let s = "";
  for (const b of a) s += ID_CHARS[b % ID_CHARS.length];
  return s;
}

// Message types a client may ask us to route to another peer verbatim.
// "rtc" carries WebRTC signaling (sdp offers/answers, ICE candidates) so peers
// can negotiate a direct LAN/P2P DataChannel; file bytes then bypass us.
const ROUTED = new Set(["offer", "accept", "decline", "ack", "received", "cancel", "rtc", "text"]);

export class ShareRoom {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    // Keepalive answered without waking the DO.
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket")
      return new Response("expected websocket", { status: 426 });

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    const cf = request.cf || {};
    const meta = {
      id: genId(),
      ip: request.headers.get("CF-Connecting-IP") || "",
      city: cf.city || "",
      country: cf.country || "",
      joined: Date.now(),
      ua: null,
      nick: "",
      did: "", // per-browser device id, lets a client hide its own other tabs
    };
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment(meta);
    server.send(JSON.stringify({ type: "welcome", ...meta }));
    return new Response(null, { status: 101, webSocket: client });
  }

  sockets() {
    const out = [];
    const now = Date.now();
    for (const ws of this.ctx.getWebSockets()) {
      try {
        const m = ws.deserializeAttachment();
        if (!m || !m.id) continue;
        // clients ping every 25s (answered by auto-response); no ping for 90s = dead
        const last = this.ctx.getWebSocketAutoResponseTimestamp(ws);
        const seen = last ? last.getTime() : m.joined;
        if (now - seen > 90000) {
          try { ws.close(1011, "stale"); } catch {}
          continue;
        }
        out.push({ ws, m });
      } catch {}
    }
    return out;
  }

  safeSend(ws, data) {
    try { ws.send(data); } catch {}
  }

  broadcast() {
    const all = this.sockets();
    const peerList = all.map((s) => ({
      id: s.m.id, nick: s.m.nick, ua: s.m.ua, ip: s.m.ip,
      city: s.m.city, country: s.m.country, joined: s.m.joined, did: s.m.did, app: s.m.app || null,
    }));
    const msg = JSON.stringify({ type: "peers", peers: peerList });
    for (const { ws } of all) this.safeSend(ws, msg);
  }

  find(id) {
    return this.sockets().find((s) => s.m.id === id);
  }

  async webSocketMessage(ws, message) {
    let m;
    try { m = ws.deserializeAttachment(); } catch { return; }
    if (!m) return;

    // ---- binary chunk: [0]=1, [1..9)=target id ASCII, [9..13)=tid, [13..17)=seq, rest payload
    if (typeof message !== "string") {
      const u8 = new Uint8Array(message);
      if (u8.length < 17 || u8[0] !== 1) return;
      let tgt = "";
      for (let i = 1; i < 9; i++) tgt += String.fromCharCode(u8[i]);
      const target = this.find(tgt);
      if (!target) return;
      const sid = m.id;
      for (let i = 0; i < 8; i++) u8[i + 1] = sid.charCodeAt(i);
      this.safeSend(target.ws, u8);
      return;
    }

    // ---- JSON control messages
    let msg;
    try { msg = JSON.parse(message); } catch { return; }
    if (!msg || typeof msg.type !== "string") return;

    switch (msg.type) {
      case "hello": {
        m.ua = msg.ua || null;
        m.nick = String(msg.nick || "").slice(0, 24);
        m.did = String(msg.did || "").slice(0, 16);
        // desktop app announces itself: {v, port, ips[]} so app peers can use the LAN TCP fast path
        if (msg.app && typeof msg.app === "object") {
          m.app = {
            v: String(msg.app.v || "").slice(0, 16),
            port: (msg.app.port | 0) || 0,
            ips: Array.isArray(msg.app.ips) ? msg.app.ips.slice(0, 8).map((s) => String(s).slice(0, 45)) : [],
          };
        } else m.app = null;
        ws.serializeAttachment(m);
        this.broadcast();
        return;
      }
      case "nick": {
        m.nick = String(msg.nick || "").slice(0, 24);
        ws.serializeAttachment(m);
        this.broadcast();
        return;
      }
    }

    if (!ROUTED.has(msg.type)) return;
    msg.from = m.id;
    msg.fromNick = m.nick || "";

    const target = this.find(String(msg.to || ""));
    if (!target) {
      this.safeSend(ws, JSON.stringify({ type: "err", tid: msg.tid, msg: "peer-gone" }));
      return;
    }
    this.safeSend(target.ws, JSON.stringify(msg));
  }

  async webSocketClose() { this.broadcast(); }
  async webSocketError() { this.broadcast(); }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/ws") {
      const id = env.ROOM.idFromName("main");
      return env.ROOM.get(id).fetch(request);
    }
    return env.ASSETS.fetch(request);
  },
};

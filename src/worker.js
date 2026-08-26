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
const ROUTED = new Set(["offer", "accept", "decline", "ack", "received", "cancel", "rtc", "text", "pipes", "creq"]);

export class ShareRoom {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    // Keepalive answered without waking the DO.
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
    // Common Share text notes live in DO storage (survive everyone going offline), max 24h.
    this.notes = null;
    this.ctx.blockConcurrencyWhile(async () => {
      this.notes = (await this.ctx.storage.get("notes")) || [];
    });
  }

  pruneNotes() {
    const cut = Date.now() - 24 * 3600 * 1000;
    const before = (this.notes || []).length;
    this.notes = (this.notes || []).filter((n) => n.ts > cut);
    if (this.notes.length !== before) this.ctx.storage.put("notes", this.notes);
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
    // devices that haven't set a name yet are invisible to everyone
    let named = all.filter((s) => s.m.nick && s.m.nick.trim());
    // several tabs of the same browser profile share one device id → show ONE device
    // (the most recently opened tab represents it and receives the files)
    const byDid = new Map();
    for (const s of named) {
      if (!s.m.did) continue;
      const prev = byDid.get(s.m.did);
      if (!prev || s.m.joined > prev.m.joined) byDid.set(s.m.did, s);
    }
    named = named.filter((s) => !s.m.did || byDid.get(s.m.did) === s);
    const peerList = named.map((s) => ({
      id: s.m.id, nick: s.m.nick, ua: s.m.ua, ip: s.m.ip,
      city: s.m.city, country: s.m.country, joined: s.m.joined, did: s.m.did, app: s.m.app || null, trusts: s.m.trusts || [], common: s.m.common || [],
    }));
    this.pruneNotes();
    const msg = JSON.stringify({ type: "peers", peers: peerList, notes: this.notes });
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
        m.trusts = Array.isArray(msg.trusts) ? msg.trusts.slice(0, 200).map((x) => String(x).slice(0, 16)) : [];
        // files this device offers in the Common Share box (metadata only; bytes stay on the device)
        m.common = Array.isArray(msg.common) ? msg.common.slice(0, 50).map((x) => ({
          cid: String(x.cid || "").slice(0, 40),
          name: String(x.name || "").slice(0, 200),
          size: Math.max(0, Number(x.size) || 0),
        })) : [];
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
      case "note": {
        const text = String(msg.text || "").slice(0, 32768);
        if (!text.trim()) return;
        this.pruneNotes();
        this.notes.push({ nid: genId(), text, by: m.nick || "?", ts: Date.now() });
        if (this.notes.length > 50) this.notes = this.notes.slice(-50);
        await this.ctx.storage.put("notes", this.notes);
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

// ---------------------------------------------------------------------------
// RelayPipe: a dumb, fast, per-transfer byte pipe. Exactly two WebSockets
// (role=a sender, role=b receiver); anything one side sends is forwarded to
// the other verbatim. Several pipes per transfer run in parallel (each is its
// own DO instance → its own CPU/socket) so a WAN transfer saturates the
// sender's upload instead of crawling on WebRTC's congestion control.
export class RelayPipe {
  constructor(ctx) {
    this.ctx = ctx;
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }
  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") return new Response("ws only", { status: 426 });
    const role = new URL(request.url).searchParams.get("role") === "b" ? "b" : "a";
    // one socket per role; a newer connection replaces an older one
    for (const ws of this.ctx.getWebSockets(role)) { try { ws.close(1000, "replaced"); } catch {} }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server, [role]);
    server.serializeAttachment({ role, t: Date.now() });
    return new Response(null, { status: 101, webSocket: client });
  }
  other(role) { return this.ctx.getWebSockets(role === "a" ? "b" : "a"); }
  async webSocketMessage(ws, msg) {
    let a; try { a = ws.deserializeAttachment(); } catch { return; }
    if (!a) return;
    if (typeof msg === "string" && msg === "hello?") { // "is the other side here?"
      ws.send(this.other(a.role).length ? "peer-ready" : "peer-missing");
      return;
    }
    for (const o of this.other(a.role)) { try { o.send(msg); } catch {} }
  }
  async webSocketClose(ws) {
    let a; try { a = ws.deserializeAttachment(); } catch { return; }
    if (!a) return;
    for (const o of this.other(a.role)) { try { o.close(1000, "peer-closed"); } catch {} }
  }
  async webSocketError(ws) { return this.webSocketClose(ws); }
}

// Short-lived TURN credentials from Cloudflare Realtime (TURN service). Lets peers
// behind CGNAT / strict NAT still connect "directly" over WebRTC via Cloudflare's
// edge instead of the slow single-DO relay. Secrets: TURN_KEY_ID, TURN_KEY_SECRET.
async function turnCredentials(env) {
  if (!env.TURN_KEY_ID || !env.TURN_KEY_SECRET) return null;
  const r = await fetch(`https://rtc.live.cloudflare.com/v1/turn/keys/${env.TURN_KEY_ID}/credentials/generate-ice-servers`, {
    method: "POST",
    headers: { Authorization: `Bearer ${env.TURN_KEY_SECRET}`, "Content-Type": "application/json" },
    body: JSON.stringify({ ttl: 6 * 3600 }),
  });
  if (!r.ok) return null;
  return r.json(); // { iceServers: [...] }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/ws") {
      const id = env.ROOM.idFromName("main");
      return env.ROOM.get(id).fetch(request);
    }
    if (url.pathname.startsWith("/relay/")) {
      const name = url.pathname.slice(7);
      if (!/^[A-Za-z0-9_-]{8,80}$/.test(name)) return new Response("bad pipe", { status: 400 });
      const id = env.PIPE.idFromName(name);
      return env.PIPE.get(id).fetch(request);
    }
    if (url.pathname === "/turn") {
      const c = await turnCredentials(env);
      return new Response(JSON.stringify(c || { iceServers: [] }), {
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store", "Access-Control-Allow-Origin": "*" },
      });
    }
    return env.ASSETS.fetch(request);
  },
};

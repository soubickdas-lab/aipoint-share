// Streaming download helper: the page hands us a URL + a MessagePort; when the
// browser fetches that URL (hidden iframe) we answer with a ReadableStream that
// the page feeds chunk-by-chunk. Result: multi-GB files go straight to disk,
// never held in RAM.
const streams = new Map();
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));

self.addEventListener("message", (e) => {
  const d = e.data || {};
  if (d.type !== "stream") return;
  const port = e.ports[0];
  let ctrl = null;
  const rs = new ReadableStream({
    start(c) { ctrl = c; },
    cancel() { try { port.postMessage({ type: "cancel" }); } catch {} },
  });
  port.onmessage = (ev) => {
    const m = ev.data || {};
    try {
      if (m.type === "chunk") ctrl.enqueue(new Uint8Array(m.buf, m.off || 0));
      else if (m.type === "end") ctrl.close();
      else if (m.type === "abort") ctrl.error("aborted");
    } catch {}
  };
  streams.set(d.url, { rs, name: d.name, size: d.size });
  port.postMessage({ type: "ready" });
  // GC guard: unclaimed streams die after 60s
  setTimeout(() => streams.delete(d.url), 60000);
});

self.addEventListener("fetch", (e) => {
  const s = streams.get(e.request.url);
  if (!s) return;
  streams.delete(e.request.url);
  const h = new Headers({
    "Content-Type": "application/octet-stream",
    "Content-Disposition": "attachment; filename*=UTF-8''" + encodeURIComponent(s.name),
    "Content-Security-Policy": "default-src 'none'",
    "X-Content-Type-Options": "nosniff",
    "Cache-Control": "no-store",
  });
  if (s.size > 0) h.set("Content-Length", String(s.size));
  e.respondWith(new Response(s.rs, { headers: h }));
});

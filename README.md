# AIPoint Share

Open device mesh + file/text sharing. Live at **https://dukto.aipoint.online**.

- `src/worker.js` — Cloudflare Worker + Durable Object (signaling + relay fallback). Files are never stored.
- `public/` — the single-page UI (also bundled into the desktop app).
- `desktop/` — Tauri v2 desktop app (Windows `.exe` installer, macOS `.dmg`). Same UI, same mesh, plus a
  LAN TCP fast path between apps, disk-direct receive, tray, autostart.

## Deploy web
```bash
npx wrangler deploy
```

## Build desktop
Pushed tags `v*` build both installers on GitHub Actions and publish a Release.
```bash
git tag v1.0.0 && git push --tags
```
Local (needs Rust + platform toolchain): `cd desktop && npm i && npm run build`.

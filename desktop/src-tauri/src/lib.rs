//! AIPoint Share desktop core.
//!
//! The UI is the same page as share.aipoint.online (bundled). This crate adds:
//!  * settings (nickname, download folder, autostart, close-to-tray)
//!  * a LAN TCP fast path: multi-stream raw TCP transfers, straight to disk,
//!    folders as-is (no zip). Web peers still use WebRTC via the webview.
//!  * disk-backed receive for WebRTC transfers (`recv_write`) so the app never
//!    holds a file in RAM.
//!  * tray icon, close-to-tray, single instance, autostart.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tauri::{AppHandle, Emitter, Manager, State};
use tauri_plugin_autostart::ManagerExt as _;
use tauri_plugin_dialog::DialogExt;
use tauri_plugin_opener::OpenerExt;

// ---------------------------------------------------------------- settings

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct Settings {
    pub nick: String,
    pub download_dir: String,
    pub autostart: bool,
    pub close_to_tray: bool,
    pub start_minimized: bool,
}
impl Default for Settings {
    fn default() -> Self {
        let dl = dirs::download_dir()
            .unwrap_or_else(|| dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")))
            .join("Dukto");
        Settings {
            nick: String::new(),
            download_dir: dl.to_string_lossy().to_string(),
            autostart: false,
            close_to_tray: true,
            start_minimized: false,
        }
    }
}

fn settings_path(app: &AppHandle) -> PathBuf {
    let dir = app
        .path()
        .app_config_dir()
        .unwrap_or_else(|_| PathBuf::from("."));
    let _ = fs::create_dir_all(&dir);
    dir.join("settings.json")
}
fn load_settings(app: &AppHandle) -> Settings {
    fs::read(settings_path(app))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}
fn save_settings(app: &AppHandle, s: &Settings) {
    let _ = fs::write(settings_path(app), serde_json::to_vec_pretty(s).unwrap_or_default());
}

// ---------------------------------------------------------------- receive sessions

struct RecvFile {
    file: File,
    #[allow(dead_code)]
    path: PathBuf,
}
struct Session {
    sid: u64,
    token: String,
    base: PathBuf,             // directory files go into
    single: Option<String>,    // for single-file sessions: final file name (already unique)
    total: u64,
    got: AtomicU64,
    files: Mutex<HashMap<String, Arc<RecvFile>>>,
    done: AtomicBool,
    last_emit: Mutex<Instant>,
}

pub struct AppState {
    settings: Mutex<Settings>,
    sessions: Mutex<HashMap<u64, Arc<Session>>>,
    tokens: Mutex<HashMap<String, u64>>,
    next_id: AtomicU64,
    tcp_port: AtomicU64,
    cancel_tx: Mutex<HashMap<String, Arc<AtomicBool>>>,
}

#[derive(Deserialize)]
struct Entry {
    rel: String,
    size: u64,
}

fn sanitize_rel(rel: &str) -> Result<PathBuf, String> {
    let mut out = PathBuf::new();
    for part in rel.replace('\\', "/").split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." || part.contains(':') {
            return Err("bad path".into());
        }
        // strip characters that are illegal on Windows
        let clean: String = part
            .chars()
            .map(|c| if "<>:\"|?*".contains(c) || (c as u32) < 32 { '_' } else { c })
            .collect();
        out.push(clean.trim_end_matches([' ', '.']));
    }
    if out.as_os_str().is_empty() {
        return Err("empty path".into());
    }
    Ok(out)
}

fn unique_path(p: &Path) -> PathBuf {
    if !p.exists() {
        return p.to_path_buf();
    }
    let stem = p.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
    let ext = p.extension().map(|e| format!(".{}", e.to_string_lossy())).unwrap_or_default();
    let parent = p.parent().map(|x| x.to_path_buf()).unwrap_or_default();
    for i in 1..10000 {
        let cand = parent.join(format!("{} ({}){}", stem, i, ext));
        if !cand.exists() {
            return cand;
        }
    }
    p.to_path_buf()
}

#[cfg(unix)]
fn write_at(f: &File, buf: &[u8], off: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    f.write_all_at(buf, off)
}
#[cfg(windows)]
fn write_at(f: &File, buf: &[u8], off: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut o = off;
    let mut b = buf;
    while !b.is_empty() {
        let n = f.seek_write(b, o)?;
        if n == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "write zero"));
        }
        o += n as u64;
        b = &b[n..];
    }
    Ok(())
}

impl Session {
    fn file_for(&self, rel: &str, size: u64) -> Result<Arc<RecvFile>, String> {
        let key = rel.to_string();
        let mut files = self.files.lock().unwrap();
        if let Some(f) = files.get(&key) {
            return Ok(f.clone());
        }
        let path = match &self.single {
            Some(name) => self.base.join(name),
            None => self.base.join(sanitize_rel(rel)?),
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| format!("open {}: {}", path.display(), e))?;
        if size > 0 {
            let _ = file.set_len(size);
        }
        let rf = Arc::new(RecvFile { file, path });
        files.insert(key, rf.clone());
        Ok(rf)
    }
    fn add_got(&self, app: &AppHandle, n: u64) {
        let got = self.got.fetch_add(n, Ordering::SeqCst) + n;
        let mut last = self.last_emit.lock().unwrap();
        if last.elapsed() >= Duration::from_millis(100) || got >= self.total {
            *last = Instant::now();
            let _ = app.emit("rx-progress", serde_json::json!({"sid": self.sid, "got": got}));
        }
        if got >= self.total && !self.done.swap(true, Ordering::SeqCst) {
            // flush + close everything
            {
                let files = self.files.lock().unwrap();
                for f in files.values() {
                    let _ = f.file.sync_all();
                }
            }
            let _ = app.emit("rx-done", serde_json::json!({"sid": self.sid}));
        }
    }
}

// ---------------------------------------------------------------- TCP listener (receiver)

#[derive(Deserialize)]
struct SegHeader {
    token: String,
    rel: String,
    offset: u64,
    len: u64,
    size: u64,
}

fn read_exact_or_err(s: &mut TcpStream, buf: &mut [u8]) -> std::io::Result<()> {
    s.read_exact(buf)
}

fn handle_conn(app: AppHandle, mut s: TcpStream) {
    let _ = s.set_nodelay(true);
    let _ = s.set_read_timeout(Some(Duration::from_secs(60)));
    let state = app.state::<AppState>();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let mut lenb = [0u8; 4];
        if read_exact_or_err(&mut s, &mut lenb).is_err() {
            return;
        }
        let hl = u32::from_be_bytes(lenb) as usize;
        if hl == 0 || hl > 65536 {
            return;
        }
        let mut hb = vec![0u8; hl];
        if read_exact_or_err(&mut s, &mut hb).is_err() {
            return;
        }
        let h: SegHeader = match serde_json::from_slice(&hb) {
            Ok(h) => h,
            Err(_) => return,
        };
        let sid = match state.tokens.lock().unwrap().get(&h.token) {
            Some(id) => *id,
            None => {
                let _ = s.write_all(b"\x00");
                return;
            }
        };
        let sess = match state.sessions.lock().unwrap().get(&sid) {
            Some(x) => x.clone(),
            None => return,
        };
        let rf = match sess.file_for(&h.rel, h.size) {
            Ok(f) => f,
            Err(_) => return,
        };
        // ack header so sender knows the token was accepted
        if s.write_all(b"\x01").is_err() {
            return;
        }
        let mut remaining = h.len;
        let mut off = h.offset;
        while remaining > 0 {
            let want = std::cmp::min(remaining, buf.len() as u64) as usize;
            let n = match s.read(&mut buf[..want]) {
                Ok(0) => return,
                Ok(n) => n,
                Err(_) => return,
            };
            if write_at(&rf.file, &buf[..n], off).is_err() {
                return;
            }
            off += n as u64;
            remaining -= n as u64;
            sess.add_got(&app, n as u64);
        }
        if h.len == 0 {
            sess.add_got(&app, 0);
        }
    }
}

fn start_listener(app: AppHandle) -> u16 {
    let listener = match TcpListener::bind("0.0.0.0:0") {
        Ok(l) => l,
        Err(_) => return 0,
    };
    let port = listener.local_addr().map(|a| a.port()).unwrap_or(0);
    thread::spawn(move || {
        for conn in listener.incoming() {
            if let Ok(s) = conn {
                let app2 = app.clone();
                thread::spawn(move || handle_conn(app2, s));
            }
        }
    });
    port
}

fn lan_ips() -> Vec<String> {
    let mut out = vec![];
    if let Ok(list) = if_addrs::get_if_addrs() {
        for i in list {
            if i.is_loopback() {
                continue;
            }
            match i.ip() {
                std::net::IpAddr::V4(v4) => {
                    if v4.is_link_local() {
                        continue;
                    }
                    out.push(v4.to_string());
                }
                std::net::IpAddr::V6(v6) => {
                    // include global/ULA v6 too, but after v4
                    if v6.is_loopback() || (v6.segments()[0] & 0xffc0) == 0xfe80 {
                        continue;
                    }
                    out.push(v6.to_string());
                }
            }
        }
    }
    // v4 first
    out.sort_by_key(|s| s.contains(':'));
    out.dedup();
    out
}

// ---------------------------------------------------------------- commands

#[tauri::command]
fn get_settings(app: AppHandle, state: State<'_, AppState>) -> serde_json::Value {
    let s = state.settings.lock().unwrap().clone();
    let auto = app.autolaunch().is_enabled().unwrap_or(s.autostart);
    serde_json::json!({
        "nick": s.nick, "download_dir": s.download_dir, "autostart": auto,
        "close_to_tray": s.close_to_tray, "start_minimized": s.start_minimized,
        "version": app.package_info().version.to_string(),
        "port": state.tcp_port.load(Ordering::SeqCst), "ips": lan_ips(),
        "os": std::env::consts::OS,
    })
}

#[tauri::command]
fn set_setting(app: AppHandle, state: State<'_, AppState>, key: String, value: serde_json::Value) -> Result<(), String> {
    let mut s = state.settings.lock().unwrap();
    match key.as_str() {
        "nick" => s.nick = value.as_str().unwrap_or("").chars().take(24).collect(),
        "download_dir" => {
            let p = value.as_str().unwrap_or("").to_string();
            fs::create_dir_all(&p).map_err(|e| e.to_string())?;
            s.download_dir = p;
        }
        "close_to_tray" => s.close_to_tray = value.as_bool().unwrap_or(true),
        "start_minimized" => s.start_minimized = value.as_bool().unwrap_or(false),
        "autostart" => {
            let on = value.as_bool().unwrap_or(false);
            let al = app.autolaunch();
            let r = if on { al.enable() } else { al.disable() };
            r.map_err(|e| e.to_string())?;
            s.autostart = on;
        }
        _ => return Err("unknown setting".into()),
    }
    save_settings(&app, &s);
    Ok(())
}

#[tauri::command]
fn pick_download_dir(app: AppHandle, state: State<'_, AppState>) -> Option<String> {
    let picked = app.dialog().file().blocking_pick_folder()?;
    let p = picked.to_string();
    let _ = fs::create_dir_all(&p);
    let mut s = state.settings.lock().unwrap();
    s.download_dir = p.clone();
    save_settings(&app, &s);
    Some(p)
}

#[tauri::command]
fn pick_files(app: AppHandle) -> Vec<String> {
    app.dialog()
        .file()
        .blocking_pick_files()
        .map(|v| v.into_iter().map(|p| p.to_string()).collect())
        .unwrap_or_default()
}
#[tauri::command]
fn pick_folder(app: AppHandle) -> Option<String> {
    app.dialog().file().blocking_pick_folder().map(|p| p.to_string())
}

#[tauri::command]
fn open_download_dir(app: AppHandle, state: State<'_, AppState>) -> Result<(), String> {
    let p = state.settings.lock().unwrap().download_dir.clone();
    let _ = fs::create_dir_all(&p);
    app.opener().open_path(p, None::<&str>).map_err(|e| e.to_string())
}
#[tauri::command]
fn reveal_path(app: AppHandle, path: String) -> Result<(), String> {
    app.opener().reveal_item_in_dir(path).map_err(|e| e.to_string())
}

#[derive(Serialize)]
struct StatOut { is_dir: bool, size: u64, name: String, mtime: u64 }
#[tauri::command]
fn stat_path(path: String) -> Result<StatOut, String> {
    let md = fs::metadata(&path).map_err(|e| e.to_string())?;
    let name = Path::new(&path).file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_else(|| path.clone());
    let mtime = md.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_millis() as u64).unwrap_or(0);
    Ok(StatOut { is_dir: md.is_dir(), size: if md.is_dir() { 0 } else { md.len() }, name, mtime })
}

#[derive(Serialize)]
struct DirEntryOut { path: String, rel: String, size: u64, mtime: u64 }
fn walk(dir: &Path, prefix: &str, out: &mut Vec<DirEntryOut>) {
    let rd = match fs::read_dir(dir) { Ok(r) => r, Err(_) => return };
    let mut items: Vec<_> = rd.filter_map(|e| e.ok()).collect();
    items.sort_by_key(|e| e.file_name());
    for e in items {
        let p = e.path();
        let name = e.file_name().to_string_lossy().to_string();
        let rel = if prefix.is_empty() { name.clone() } else { format!("{}/{}", prefix, name) };
        match e.metadata() {
            Ok(md) if md.is_dir() => walk(&p, &rel, out),
            Ok(md) if md.is_file() => {
                let mtime = md.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_millis() as u64).unwrap_or(0);
                out.push(DirEntryOut { path: p.to_string_lossy().to_string(), rel, size: md.len(), mtime });
            }
            _ => {}
        }
    }
}
#[tauri::command]
fn list_dir(path: String) -> Result<Vec<DirEntryOut>, String> {
    let p = PathBuf::from(&path);
    let root = p.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_else(|| "folder".into());
    let mut out = vec![];
    walk(&p, &root, &mut out);
    Ok(out)
}

/// Read a byte range of a local file (used to feed WebRTC transfers to web peers).
#[tauri::command]
fn read_range(path: String, offset: u64, len: u64) -> Result<tauri::ipc::Response, String> {
    use std::io::{Seek, SeekFrom};
    let mut f = File::open(&path).map_err(|e| e.to_string())?;
    f.seek(SeekFrom::Start(offset)).map_err(|e| e.to_string())?;
    let mut buf = vec![0u8; len as usize];
    let mut got = 0usize;
    while got < buf.len() {
        let n = f.read(&mut buf[got..]).map_err(|e| e.to_string())?;
        if n == 0 { break; }
        got += n;
    }
    buf.truncate(got);
    Ok(tauri::ipc::Response::new(buf))
}

/// Create a receive session (single file or folder). Returns {sid, token, path}.
#[tauri::command]
fn recv_session(app: AppHandle, state: State<'_, AppState>, entries: Vec<Entry>, total: u64, root: Option<String>) -> Result<serde_json::Value, String> {
    let dl = PathBuf::from(state.settings.lock().unwrap().download_dir.clone());
    fs::create_dir_all(&dl).map_err(|e| e.to_string())?;
    let sid = state.next_id.fetch_add(1, Ordering::SeqCst) + 1;
    let token: String = {
        use rand::Rng;
        let mut r = rand::thread_rng();
        (0..24).map(|_| { let c = r.gen_range(0..36u8); (if c < 10 { b'0' + c } else { b'a' + c - 10 }) as char }).collect()
    };
    let (base, single, shown) = if entries.len() == 1 && root.is_none() {
        let name = sanitize_rel(&entries[0].rel)?;
        let name = name.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_else(|| "file".into());
        let final_path = unique_path(&dl.join(&name));
        let fname = final_path.file_name().unwrap().to_string_lossy().to_string();
        (dl.clone(), Some(fname.clone()), dl.join(fname))
    } else {
        // folder: unique root dir, entries' rel already start with root name → we replace root
        let root_name = sanitize_rel(root.as_deref().unwrap_or("folder"))?;
        let root_dir = unique_path(&dl.join(root_name));
        fs::create_dir_all(&root_dir).map_err(|e| e.to_string())?;
        (root_dir.clone(), None, root_dir)
    };
    let sess = Arc::new(Session {
        sid, token: token.clone(), base, single, total,
        got: AtomicU64::new(0), files: Mutex::new(HashMap::new()),
        done: AtomicBool::new(false), last_emit: Mutex::new(Instant::now()),
    });
    // zero-byte total → immediately done (create empty files lazily on first header)
    state.sessions.lock().unwrap().insert(sid, sess.clone());
    state.tokens.lock().unwrap().insert(token.clone(), sid);
    if total == 0 {
        for e in &entries { let rel = if root.is_some() { strip_root(&e.rel) } else { e.rel.clone() }; let _ = sess.file_for(&rel, 0); }
        let _ = app.emit("rx-done", serde_json::json!({"sid": sid}));
    }
    Ok(serde_json::json!({"sid": sid, "token": token, "path": shown.to_string_lossy()}))
}

/// Folder sessions: rel paths from the sender start with the sender's root name;
/// we map "root/x/y" → base/x/y (base is already the unique root dir).
fn strip_root(rel: &str) -> String {
    let r = rel.replace('\\', "/");
    match r.find('/') { Some(i) => r[i + 1..].to_string(), None => r }
}

/// WebRTC receive path: append bytes at offset into session's (single) file.
#[tauri::command]
fn recv_write(app: AppHandle, state: State<'_, AppState>, request: tauri::ipc::Request<'_>) -> Result<u64, String> {
    let hdr = |k: &str| request.headers().get(k).and_then(|v| v.to_str().ok()).map(|s| s.to_string());
    let sid: u64 = hdr("x-sid").and_then(|s| s.parse().ok()).ok_or("sid")?;
    let off: u64 = hdr("x-off").and_then(|s| s.parse().ok()).ok_or("off")?;
    let bytes = match request.body() { tauri::ipc::InvokeBody::Raw(b) => b, _ => return Err("raw body expected".into()) };
    let sess = state.sessions.lock().unwrap().get(&sid).cloned().ok_or("no session")?;
    let rel = { sess.files.lock().unwrap().keys().next().cloned() }.unwrap_or_else(|| "file".into());
    let rf = sess.file_for(&rel, sess.total)?;
    write_at(&rf.file, bytes, off).map_err(|e| e.to_string())?;
    sess.add_got(&app, bytes.len() as u64);
    Ok(sess.got.load(Ordering::SeqCst))
}

#[tauri::command]
fn recv_close(state: State<'_, AppState>, sid: u64) {
    if let Some(s) = state.sessions.lock().unwrap().remove(&sid) {
        state.tokens.lock().unwrap().remove(&s.token);
    }
}
#[tauri::command]
fn recv_abort(state: State<'_, AppState>, sid: u64) {
    if let Some(s) = state.sessions.lock().unwrap().remove(&sid) {
        state.tokens.lock().unwrap().remove(&s.token);
        let files = s.files.lock().unwrap();
        for f in files.values() { let _ = fs::remove_file(&f.path); }
    }
}

// ---------------------------------------------------------------- TCP sender

#[derive(Deserialize, Clone)]
struct SendEntry { path: String, rel: String, size: u64 }
#[derive(Clone)]
struct Segment { path: String, rel: String, offset: u64, len: u64, size: u64 }

fn connect_any(hosts: &[String], port: u16) -> Option<TcpStream> {
    // try all candidate IPs in parallel, first to answer wins
    let (tx, rx) = std::sync::mpsc::channel();
    let mut n = 0;
    for h in hosts {
        let addr_s = if h.contains(':') { format!("[{}]:{}", h, port) } else { format!("{}:{}", h, port) };
        let addrs: Vec<SocketAddr> = match addr_s.to_socket_addrs() { Ok(a) => a.collect(), Err(_) => continue };
        for a in addrs {
            let tx = tx.clone();
            n += 1;
            thread::spawn(move || {
                let r = TcpStream::connect_timeout(&a, Duration::from_millis(1500)).ok();
                let _ = tx.send(r);
            });
        }
    }
    drop(tx);
    for _ in 0..n {
        if let Ok(Some(s)) = rx.recv() { return Some(s); }
    }
    None
}

fn send_segments(mut s: TcpStream, token: &str, segs: Vec<Segment>, sent: Arc<AtomicU64>, cancel: Arc<AtomicBool>) -> Result<(), String> {
    use std::io::{Seek, SeekFrom};
    let _ = s.set_nodelay(true);
    let mut buf = vec![0u8; 1024 * 1024];
    for seg in segs {
        if cancel.load(Ordering::SeqCst) { return Err("cancelled".into()); }
        let hdr = serde_json::json!({"token": token, "rel": strip_root(&seg.rel), "offset": seg.offset, "len": seg.len, "size": seg.size});
        let hb = serde_json::to_vec(&hdr).unwrap();
        s.write_all(&(hb.len() as u32).to_be_bytes()).map_err(|e| e.to_string())?;
        s.write_all(&hb).map_err(|e| e.to_string())?;
        let mut ack = [0u8; 1];
        s.read_exact(&mut ack).map_err(|e| format!("no ack: {}", e))?;
        if ack[0] != 1 { return Err("receiver rejected token".into()); }
        if seg.len == 0 { continue; }
        let mut f = File::open(&seg.path).map_err(|e| e.to_string())?;
        f.seek(SeekFrom::Start(seg.offset)).map_err(|e| e.to_string())?;
        let mut remaining = seg.len;
        while remaining > 0 {
            if cancel.load(Ordering::SeqCst) { return Err("cancelled".into()); }
            let want = std::cmp::min(remaining, buf.len() as u64) as usize;
            let n = f.read(&mut buf[..want]).map_err(|e| e.to_string())?;
            if n == 0 { return Err("file shrank while sending".into()); }
            s.write_all(&buf[..n]).map_err(|e| e.to_string())?;
            remaining -= n as u64;
            sent.fetch_add(n as u64, Ordering::SeqCst);
        }
    }
    let _ = s.flush();
    Ok(())
}

/// Send entries over LAN TCP. Blocks until done. Emits tx-progress {key, sent}.
#[tauri::command]
async fn tcp_send(app: AppHandle, key: String, hosts: Vec<String>, port: u16, token: String, entries: Vec<SendEntry>, streams: Option<u32>) -> Result<(), String> {
    let cancel = Arc::new(AtomicBool::new(false));
    app.state::<AppState>().cancel_tx.lock().unwrap().insert(key.clone(), cancel.clone());
    let key2 = key.clone();
    let app2 = app.clone();
    let res = tauri::async_runtime::spawn_blocking(move || -> Result<(), String> {
        let nstreams = streams.unwrap_or(4).clamp(1, 8) as usize;
        // probe connection first
        let first = connect_any(&hosts, port).ok_or("no LAN route")?;
        let peer_ip = first.peer_addr().map(|a| a.ip().to_string()).map_err(|e| e.to_string())?;
        // build segments: big files split into ranges >= 8MB
        let mut segs: Vec<Segment> = vec![];
        let total: u64 = entries.iter().map(|e| e.size).sum();
        let min_seg: u64 = 8 * 1024 * 1024;
        for e in &entries {
            if e.size >= min_seg * 2 {
                let parts = std::cmp::min(nstreams as u64, e.size / min_seg).max(1);
                let part = e.size / parts;
                for i in 0..parts {
                    let off = i * part;
                    let len = if i == parts - 1 { e.size - off } else { part };
                    segs.push(Segment { path: e.path.clone(), rel: e.rel.clone(), offset: off, len, size: e.size });
                }
            } else {
                segs.push(Segment { path: e.path.clone(), rel: e.rel.clone(), offset: 0, len: e.size, size: e.size });
            }
        }
        // round-robin distribute segments (largest first) across streams
        segs.sort_by(|a, b| b.len.cmp(&a.len));
        let n = std::cmp::min(nstreams, segs.len().max(1));
        let mut buckets: Vec<Vec<Segment>> = vec![vec![]; n];
        let mut loads = vec![0u64; n];
        for s in segs {
            let (i, _) = loads.iter().enumerate().min_by_key(|(_, l)| **l).unwrap();
            loads[i] += s.len.max(1);
            buckets[i].push(s);
        }
        let sent = Arc::new(AtomicU64::new(0));
        // progress thread
        {
            let sent = sent.clone(); let app = app2.clone(); let key = key2.clone(); let cancel = cancel.clone();
            thread::spawn(move || {
                let mut last = 0;
                loop {
                    thread::sleep(Duration::from_millis(100));
                    let v = sent.load(Ordering::SeqCst);
                    if v != last { last = v; let _ = app.emit("tx-progress", serde_json::json!({"key": key, "sent": v})); }
                    if v >= total || cancel.load(Ordering::SeqCst) { break; }
                }
            });
        }
        let mut handles = vec![];
        let mut first = Some(first);
        for bucket in buckets {
            let stream = match first.take() {
                Some(s) => s,
                None => {
                    let addr_s = if peer_ip.contains(':') { format!("[{}]:{}", peer_ip, port) } else { format!("{}:{}", peer_ip, port) };
                    let a: SocketAddr = addr_s.parse().map_err(|_| "addr")?;
                    TcpStream::connect_timeout(&a, Duration::from_millis(3000)).map_err(|e| e.to_string())?
                }
            };
            let token = token.clone(); let sent = sent.clone(); let cancel = cancel.clone();
            handles.push(thread::spawn(move || send_segments(stream, &token, bucket, sent, cancel)));
        }
        let mut err = None;
        for h in handles {
            match h.join() { Ok(Ok(())) => {}, Ok(Err(e)) => { err = Some(e); cancel.store(true, Ordering::SeqCst); }, Err(_) => err = Some("thread panic".into()) }
        }
        let _ = app2.emit("tx-progress", serde_json::json!({"key": key2, "sent": sent.load(Ordering::SeqCst)}));
        match err { Some(e) => Err(e), None => Ok(()) }
    })
    .await
    .map_err(|e| e.to_string())?;
    app.state::<AppState>().cancel_tx.lock().unwrap().remove(&key);
    res
}

#[tauri::command]
fn tcp_cancel(state: State<'_, AppState>, key: String) {
    if let Some(c) = state.cancel_tx.lock().unwrap().get(&key) { c.store(true, Ordering::SeqCst); }
}

#[tauri::command]
fn show_main(app: AppHandle) {
    if let Some(w) = app.get_webview_window("main") { let _ = w.show(); let _ = w.unminimize(); let _ = w.set_focus(); }
}

// ---------------------------------------------------------------- app setup

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let start_minimized_flag = std::env::args().any(|a| a == "--minimized");
    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            if let Some(w) = app.get_webview_window("main") { let _ = w.show(); let _ = w.set_focus(); }
        }))
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_autostart::init(
            tauri_plugin_autostart::MacosLauncher::LaunchAgent,
            Some(vec!["--minimized"]),
        ))
        .setup(move |app| {
            let handle = app.handle().clone();
            let settings = load_settings(&handle);
            let port = start_listener(handle.clone());
            app.manage(AppState {
                settings: Mutex::new(settings.clone()),
                sessions: Mutex::new(HashMap::new()),
                tokens: Mutex::new(HashMap::new()),
                next_id: AtomicU64::new(0),
                tcp_port: AtomicU64::new(port as u64),
                cancel_tx: Mutex::new(HashMap::new()),
            });

            // tray
            use tauri::menu::{Menu, MenuItem};
            use tauri::tray::{TrayIconBuilder, TrayIconEvent, MouseButton, MouseButtonState};
            let show_i = MenuItem::with_id(app, "show", "Open Dukto", true, None::<&str>)?;
            let dl_i = MenuItem::with_id(app, "dl", "Open download folder", true, None::<&str>)?;
            let quit_i = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;
            let menu = Menu::with_items(app, &[&show_i, &dl_i, &quit_i])?;
            let mut tb = TrayIconBuilder::with_id("main-tray")
                .menu(&menu)
                .show_menu_on_left_click(false)
                .tooltip("Dukto")
                .on_menu_event(|app, ev| match ev.id().as_ref() {
                    "show" => show_main(app.clone()),
                    "dl" => { let st = app.state::<AppState>(); let _ = open_download_dir(app.clone(), st); }
                    "quit" => app.exit(0),
                    _ => {}
                })
                .on_tray_icon_event(|tray, ev| {
                    if let TrayIconEvent::Click { button: MouseButton::Left, button_state: MouseButtonState::Up, .. } = ev {
                        show_main(tray.app_handle().clone());
                    }
                });
            if let Some(icon) = app.default_window_icon() { tb = tb.icon(icon.clone()); }
            tb.build(app)?;

            if let Some(w) = app.get_webview_window("main") {
                if start_minimized_flag || settings.start_minimized { let _ = w.hide(); } else { let _ = w.show(); }
            }
            Ok(())
        })
        .on_window_event(|window, event| {
            if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                let st = window.app_handle().state::<AppState>();
                let hide = st.settings.lock().unwrap().close_to_tray;
                if hide { let _ = window.hide(); api.prevent_close(); }
            }
        })
        .invoke_handler(tauri::generate_handler![
            get_settings, set_setting, pick_download_dir, pick_files, pick_folder, open_download_dir, reveal_path,
            stat_path, list_dir, read_range, recv_session, recv_write, recv_close, recv_abort,
            tcp_send, tcp_cancel, show_main
        ])
        .run(tauri::generate_context!())
        .expect("error while running Dukto");
}

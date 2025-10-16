#!/usr/bin/env python3
"""
tgm_final_v2.6_aria2_hybrid.py
Telegram Manager v2.6 — Aria2 Hybrid Mode (Direct Telegram Connection, external aria2 subprocess)

Default large-file save path: /sdcard/Download/TelegramManager/
"""
# Minimal imports
import os, sys, json, time, threading, tempfile, subprocess, shutil, random, getpass, hashlib, base64
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

# Optional libraries
try:
    import requests
    requests_ok = True
except Exception:
    requests_ok = False

try:
    from Crypto.Cipher import AES
    from Crypto.Random import get_random_bytes
    pycrypto_ok = True
except Exception:
    pycrypto_ok = False

try:
    from telethon import TelegramClient, utils, functions
    telethon_available = True
except Exception:
    telethon_available = False

# Paths & directories
APP = "TelegramManager"
BASE_DIR = Path.home() / APP
MANAGER_DATA = BASE_DIR / "ManagerData"
SEC_DIR = MANAGER_DATA / "security"
LOGS_DIR = MANAGER_DATA / "logs"
BACKUPS_DIR = MANAGER_DATA / "backups"
DOWNLOADS_DIR_INTERNAL = MANAGER_DATA / "downloads"
DOWNLOADS_DIR_EXTERNAL = Path("/sdcard/Download/TelegramManager")
CFG_PATH = SEC_DIR / "config.json"
DOWNLOAD_STATE = MANAGER_DATA / "download_state.json"
HISTORY_FILE = MANAGER_DATA / "history.json"
DIAG_FILE = MANAGER_DATA / "diagnostics.log"
FERNET_KEY_PATH = SEC_DIR / "fernet.key"

for p in (BASE_DIR, MANAGER_DATA, SEC_DIR, LOGS_DIR, BACKUPS_DIR, DOWNLOADS_DIR_INTERNAL, DOWNLOADS_DIR_EXTERNAL):
    try:
        p.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

# SimpleFernet fallback for termux-friendly encryption
class SimpleFernet:
    @staticmethod
    def generate_key():
        return (get_random_bytes(32) if pycrypto_ok else os.urandom(32))
    def __init__(self, key: Optional[bytes] = None):
        raw = key or SimpleFernet.generate_key()
        self.key = hashlib.sha256(raw).digest()
    def encrypt(self, data: bytes) -> bytes:
        if not pycrypto_ok:
            return base64.urlsafe_b64encode(data)
        iv = get_random_bytes(16)
        cipher = AES.new(self.key, AES.MODE_CFB, iv)
        return base64.urlsafe_b64encode(iv + cipher.encrypt(data))
    def decrypt(self, token: bytes) -> bytes:
        try:
            raw = base64.urlsafe_b64decode(token)
            if not pycrypto_ok:
                return raw
            iv, ct = raw[:16], raw[16:]
            cipher = AES.new(self.key, AES.MODE_CFB, iv)
            return cipher.decrypt(ct)
        except Exception:
            return b""

def get_or_create_key() -> bytes:
    if FERNET_KEY_PATH.exists():
        try:
            return FERNET_KEY_PATH.read_bytes()
        except Exception:
            pass
    k = SimpleFernet.generate_key()
    try:
        FERNET_KEY_PATH.write_bytes(k)
        os.chmod(FERNET_KEY_PATH, 0o600)
    except Exception:
        pass
    return k

FERNET_KEY = get_or_create_key()
FERNET = SimpleFernet(FERNET_KEY)

# Config load/save
def load_config() -> Dict[str, Any]:
    if not CFG_PATH.exists():
        return {}
    try:
        return json.loads(CFG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_config(cfg: Dict[str, Any]):
    try:
        CFG_PATH.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
        try:
            os.chmod(CFG_PATH, 0o600)
        except Exception:
            pass
    except Exception:
        pass

cfg = load_config()
cfg.setdefault("owner", None)
cfg.setdefault("version", "tgm_final_v2.6")
cfg.setdefault("CURRENT_VERSION", "2.6")
cfg.setdefault("fast_mode", True)
cfg.setdefault("progress_bar", True)
cfg.setdefault("inline_updates", False)
cfg.setdefault("refresh_interval", 3.0)
cfg.setdefault("auto_backup", False)
cfg.setdefault("auto_update_on_start", False)
cfg.setdefault("aria2_threshold_bytes", 100 * 1024 * 1024)
cfg.setdefault("aria2_enabled", True)
cfg.setdefault("download_external_path", str(DOWNLOADS_DIR_EXTERNAL))
cfg.setdefault("control_group_id", None)
cfg.setdefault("lockout", {"attempts": 0, "blocked_until": 0})
save_config(cfg)

# Utilities
def human_size(n: int) -> str:
    n = float(n or 0)
    for unit in ["B","KB","MB","GB","TB"]:
        if n < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"

def write_diag(s: str):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(DIAG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {s}\n")
    except Exception:
        pass

# Auto-update endpoints (your repo)
UPDATE_URL = "https://raw.githubusercontent.com/maakash52-design/telegram-manager/main/tgm_final_v2.6_aria2_hybrid.py"
VERSION_MANIFEST = "https://raw.githubusercontent.com/maakash52-design/telegram-manager/main/version.json"
CURRENT_VERSION = cfg.get("CURRENT_VERSION", "2.6")

def startup_check_for_update(noninteractive=True):
    if not requests_ok:
        return {"status": "no_network"}
    try:
        r = requests.get(VERSION_MANIFEST, timeout=8)
        r.raise_for_status()
        data = r.json()
        latest = str(data.get("latest_version", "")).strip()
        if not latest:
            return {"status": "manifest_invalid"}
        if latest == CURRENT_VERSION:
            return {"status": "up_to_date", "version": latest}
        info = {"status": "available", "latest": latest}
        cfg["update_available"] = info; save_config(cfg)
        if cfg.get("auto_update_on_start", False):
            resp = requests.get(UPDATE_URL, timeout=15)
            if resp.status_code == 200 and len(resp.text) > 50:
                _replace_script(resp.text)
                return {"status": "applied", "version": latest}
        else:
            print(f"\n[Updater] New version available: v{latest} — run `python3 {Path(__file__).name} --self-update` to apply.")
            return info
    except Exception as e:
        write_diag(f"startup_update error: {e}")
        return {"status": "error", "error": str(e)}

def _replace_script(text: str) -> bool:
    try:
        tmp = MANAGER_DATA / f"tgm_update_{int(time.time())}.py"
        tmp.write_text(text, encoding="utf-8")
        try:
            if os.access(__file__, os.X_OK):
                tmp.chmod(0o755)
        except Exception:
            pass
        os.replace(tmp, Path(__file__))
        return True
    except Exception as e:
        write_diag(f"replace_script_failed: {e}")
        return False

def spawn_startup_update_thread():
    def _bg():
        time.sleep(1.0)
        res = startup_check_for_update(noninteractive=True)
        write_diag(f"startup_update_result: {json.dumps(res)}")
    t = threading.Thread(target=_bg, daemon=True); t.start()

# Telethon helpers
TELETHON_CLIENT: Optional[TelegramClient] = None
TELETHON_SESSION = MANAGER_DATA / "tgm_final.session"

def ensure_telethon(api_id: int, api_hash: str):
    global TELETHON_CLIENT
    try:
        import asyncio
        loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
        client = TelegramClient(str(TELETHON_SESSION), api_id, api_hash)
        loop.run_until_complete(client.start())
        TELETHON_CLIENT = client
        return client
    except Exception as e:
        write_diag(f"telethon start failed: {e}")
        TELETHON_CLIENT = None
        return None

def ensure_control_group(client: TelegramClient) -> Optional[int]:
    try:
        cg = cfg.get("control_group_id")
        if cg:
            try:
                ent = client.get_entity(int(cg)); return int(cg)
            except Exception:
                cfg["control_group_id"] = None; save_config(cfg)
        title = f"Telegram Manager — Control ({cfg.get('owner') or 'Owner'})"
        import asyncio
        loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
        res = loop.run_until_complete(client(functions.channels.CreateChannelRequest(title=title, about="Control channel for Telegram Manager")))
        ch = res.chats[0]; cid = utils.get_peer_id(ch)
        cfg["control_group_id"] = int(cid); save_config(cfg)
        return int(cid)
    except Exception as e:
        write_diag(f"ensure_control_group: {e}")
        return None

def send_inline_update(text: str, edit_msg_id: Optional[int] = None):
    if not cfg.get("inline_updates") or not telethon_available or TELETHON_CLIENT is None or not cfg.get("control_group_id"):
        return None
    try:
        import asyncio
        loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
        async def _send():
            ch = int(cfg.get("control_group_id"))
            if edit_msg_id:
                try:
                    await TELETHON_CLIENT.edit_message(ch, edit_msg_id, text); return edit_msg_id
                except Exception:
                    m = await TELETHON_CLIENT.send_message(ch, text); return m.id
            else:
                m = await TELETHON_CLIENT.send_message(ch, text); return m.id
        return loop.run_until_complete(_send())
    except Exception as e:
        write_diag(f"send_inline_update error: {e}")
        return None

# Chat listing & preview
def simulated_chats() -> List[Dict[str, Any]]:
    out = []
    for i in range(1, 140):
        out.append({"id": 10000 + i, "name": f"Group_{i:03d}", "type": "group"})
    for i in range(1, 90):
        out.append({"id": 20000 + i, "name": f"Contact_{i:03d}", "type": "private"})
    return out

def get_chats() -> List[Dict[str, Any]]:
    if telethon_available and TELETHON_CLIENT:
        try:
            import asyncio
            loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
            async def _get():
                dialogs = []
                async for d in TELETHON_CLIENT.iter_dialogs():
                    ent = d.entity
                    name = getattr(ent, "title", None) or getattr(ent, "first_name", None) or getattr(ent, "username", None) or str(d.id)
                    typ = "private"
                    if getattr(ent, "broadcast", False) or getattr(ent, "megagroup", False) or getattr(ent, "title", None):
                        typ = "group" if getattr(ent, "megagroup", False) else "channel" if getattr(ent, "broadcast", False) else "group"
                    dialogs.append({"id": int(d.id), "name": str(name), "type": typ})
                return dialogs
            return loop.run_until_complete(_get())
        except Exception as e:
            write_diag(f"get_chats error: {e}")
            return simulated_chats()
    return simulated_chats()

def split_and_sort(dialogs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    groups = [d for d in dialogs if d["type"] in ("group", "channel")]
    privs = [d for d in dialogs if d["type"] == "private"]
    return (sorted(groups, key=lambda x: x["name"].lower()), sorted(privs, key=lambda x: x["name"].lower()))

def simulated_preview_by_id(entity_id: int) -> Dict[str, Any]:
    seed = int(entity_id) & 0xffff
    random.seed(seed)
    photos = random.randint(0, 400)
    videos = random.randint(0, 200)
    docs = random.randint(0, 120)
    return {
        "photos": {"count": photos, "size": photos * 1_200_000},
        "videos": {"count": videos, "size": videos * 40_000_000},
        "docs": {"count": docs, "size": docs * 800_000},
        "total": {"count": photos + videos + docs, "size": photos * 1_200_000 + videos * 40_000_000 + docs * 800_000}
    }

def preview_media_for_chat(chat: Dict[str, Any]) -> Dict[str, Any]:
    if telethon_available and TELETHON_CLIENT:
        try:
            import asyncio
            loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
            async def _preview():
                photos = videos = docs = 0; total_bytes = 0
                async for msg in TELETHON_CLIENT.iter_messages(chat["id"], limit=2000):
                    if msg.media:
                        if getattr(msg, "photo", None) or "photo" in str(msg.media).lower():
                            photos += 1
                        elif getattr(msg, "video", None) or "video" in str(msg.media).lower():
                            videos += 1
                        else:
                            docs += 1
                        try:
                            total_bytes += int(msg.file.size) if getattr(msg, 'file', None) and getattr(msg.file, 'size', None) else 0
                        except Exception:
                            pass
                return {"photos": {"count": photos, "size": photos * 1_200_000}, "videos": {"count": videos, "size": videos * 40_000_000}, "docs": {"count": docs, "size": docs * 800_000}, "total": {"count": photos + videos + docs, "size": total_bytes if total_bytes > 0 else photos * 1_200_000 + videos * 40_000_000 + docs * 800_000}}
            return loop.run_until_complete(_preview())
        except Exception as e:
            write_diag(f"preview_media error: {e}")
            return simulated_preview_by_id(chat["id"])
    return simulated_preview_by_id(chat["id"])

# Download engine
ARIA_BIN = shutil.which("aria2c")
ARIA_ENABLED = bool(ARIA_BIN) and bool(cfg.get("aria2_enabled", True))
ARIA_MIN_BYTES = cfg.get("aria2_threshold_bytes", 100 * 1024 * 1024)
EXTERNAL_PATH = Path(cfg.get("download_external_path", str(DOWNLOADS_DIR_EXTERNAL)))
EXTERNAL_PATH.mkdir(parents=True, exist_ok=True)

def measure_speed_sample(timeout=6.0) -> Optional[float]:
    if not requests_ok:
        return None
    try:
        url = "https://www.google.com/favicon.ico"
        t0 = time.time()
        r = requests.get(url, stream=True, timeout=timeout)
        total = 0
        for chunk in r.iter_content(4096):
            total += len(chunk)
            if total >= 40000:
                break
        dt = time.time() - t0
        return (total / dt) if dt > 0 else None
    except Exception:
        return None

def compute_profile(bps: Optional[float], battery_ok: bool = True) -> Dict[str, Any]:
    chunk = 256_000; concurrency = 1; fast_mode = False
    if bps:
        mbps = bps / 1e6
        if mbps >= 10:
            chunk = 1_000_000; concurrency = 4; fast_mode = True
        elif mbps >= 2:
            chunk = 512_000; concurrency = 2; fast_mode = True
        elif mbps >= 0.5:
            chunk = 256_000; concurrency = 1
        else:
            chunk = 128_000; concurrency = 1
    if not battery_ok:
        chunk = max(64_000, chunk // 2); concurrency = 1
    return {"chunk_size": int(chunk), "concurrency": int(concurrency), "fast_mode": fast_mode, "refresh_interval": cfg.get("refresh_interval", 3.0)}

def battery_ok_simple() -> bool:
    if os.getenv("PREFIX") and "com.termux" in os.getenv("PREFIX", ""):
        try:
            out = subprocess.check_output(["termux-battery-status"], stderr=subprocess.DEVNULL).decode()
            j = json.loads(out); lvl = j.get("percentage", 100); ch = j.get("charging", False)
            if lvl < 20 and not ch:
                return False
            return True
        except Exception:
            return True
    return True

def load_state() -> Dict[str, Any]:
    if DOWNLOAD_STATE.exists():
        try:
            return json.loads(DOWNLOAD_STATE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_state(s: Dict[str, Any]):
    try:
        DOWNLOAD_STATE.write_text(json.dumps(s, indent=2), encoding="utf-8")
    except Exception:
        pass

def append_history(chat_name: str, total: int, duration: int):
    try:
        hist = []
        if HISTORY_FILE.exists(): hist = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        hist.append({"chat": chat_name, "size": total, "time": datetime.utcnow().isoformat(), "duration": f"{duration}s"})
        HISTORY_FILE.write_text(json.dumps(hist[-300:], indent=2), encoding="utf-8")
    except Exception:
        pass

# DownloadController (Telethon and aria2 handoff)
from concurrent.futures import ThreadPoolExecutor, as_completed
STATE_SAVE_INTERVAL = 3.0

class DownloadController:
    def __init__(self, profile: Dict[str, Any], dest_dir: Path):
        self.profile = profile
        self.dest = Path(dest_dir); self.dest.mkdir(parents=True, exist_ok=True)
        self._pause = threading.Event(); self._pause.clear()
        self._cancel = threading.Event()
        self._lock = threading.Lock()
        self.downloaded = 0; self.total = 0
        self.last_save = time.time(); self.state = load_state()

    def pause(self): self._pause.set()
    def resume(self): self._pause.clear()
    def cancel(self): self._cancel.set()
    def _wait_if_paused(self):
        while self._pause.is_set() and not self._cancel.is_set():
            time.sleep(0.5)
    def _save_progress(self):
        if time.time() - self.last_save >= STATE_SAVE_INTERVAL:
            save_state(self.state); self.last_save = time.time()

    def _telethon_download(self, chat_id: int, msg_id: int, outpath: Path, assumed_size: int = 0) -> bool:
        if not telethon_available or TELETHON_CLIENT is None:
            return self._simulated(outpath, assumed_size)
        try:
            import asyncio
            loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
            async def _run():
                def _cb(received, total):
                    with self._lock:
                        prev = self.state.get(str(chat_id), {}).get(str(msg_id), {}).get("done", 0)
                        add = max(0, received - prev)
                        self.downloaded += add
                        self.state.setdefault(str(chat_id), {})[str(msg_id)] = {"done": int(received), "total": int(total or assumed_size)}
                        self._save_progress()
                    while self._pause.is_set() and not self._cancel.is_set(): time.sleep(0.5)
                msg = await TELETHON_CLIENT.get_messages(chat_id, ids=msg_id)
                await TELETHON_CLIENT.download_media(msg, file=str(outpath), progress_callback=_cb)
                return outpath.exists()
            ok = loop.run_until_complete(_run()); return ok
        except Exception:
            return False

    def _simulated(self, outpath: Path, size: int = 5_000_000):
        if outpath.exists() and outpath.stat().st_size >= size: return True
        done = 0
        while done < size and not self._cancel.is_set():
            self._wait_if_paused()
            step = min(self.profile.get("chunk_size", 256000), size - done)
            time.sleep(0.05); done += step
            with self._lock: self.downloaded += step
            self._save_progress()
        try:
            with open(outpath, "wb") as f: f.truncate(size)
        except Exception:
            pass
        return not self._cancel.is_set()

    def _aria2_hand_off(self, url: str, outdir: Path, outname: str) -> bool:
        if not ARIA_ENABLED: return False
        outdir.mkdir(parents=True, exist_ok=True)
        cmd = [ARIA_BIN, "-x", "8", "-s", "8", "-j", "4", "-d", str(outdir), "-o", outname, url]
        try:
            subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL)
            return True
        except Exception:
            return False

    def _download_one(self, chat_id: int, item: Dict[str, Any]):
        msg_id = int(item.get("msg_id"))
        size = int(item.get("size", 0))
        title = item.get("title", "file").replace(" ", "_")
        outname = f"{chat_id}_{msg_id}_{title}"
        outpath_internal = self.dest / outname
        outpath_external = EXTERNAL_PATH / outname
        direct_url = item.get("direct_url")
        if size >= ARIA_MIN_BYTES and ARIA_ENABLED:
            if direct_url:
                ok = self._aria2_hand_off(direct_url, EXTERNAL_PATH, outname)
                if ok:
                    with self._lock:
                        self.downloaded += size
                        self.state.setdefault(str(chat_id), {})[str(msg_id)] = {"done": int(size), "total": int(siz#!/usr/bin/env python3
"""
tgm_final_v2.6_aria2_hybrid.py
Telegram Manager v2.6 — Aria2 Hybrid Mode (Direct Telegram Connection, external aria2 subprocess)

Default large-file save path: /sdcard/Download/TelegramManager/
"""
# Minimal imports
import os, sys, json, time, threading, tempfile, subprocess, shutil, random, getpass, hashlib, base64
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

# Optional libraries
try:
    import requests
    requests_ok = True
except Exception:
    requests_ok = False

try:
    from Crypto.Cipher import AES
    from Crypto.Random import get_random_bytes
    pycrypto_ok = True
except Exception:
    pycrypto_ok = False

try:
    from telethon import TelegramClient, utils, functions
    telethon_available = True
except Exception:
    telethon_available = False

# Paths & directories
APP = "TelegramManager"
BASE_DIR = Path.home() / APP
MANAGER_DATA = BASE_DIR / "ManagerData"
SEC_DIR = MANAGER_DATA / "security"
LOGS_DIR = MANAGER_DATA / "logs"
BACKUPS_DIR = MANAGER_DATA / "backups"
DOWNLOADS_DIR_INTERNAL = MANAGER_DATA / "downloads"
DOWNLOADS_DIR_EXTERNAL = Path("/sdcard/Download/TelegramManager")
CFG_PATH = SEC_DIR / "config.json"
DOWNLOAD_STATE = MANAGER_DATA / "download_state.json"
HISTORY_FILE = MANAGER_DATA / "history.json"
DIAG_FILE = MANAGER_DATA / "diagnostics.log"
FERNET_KEY_PATH = SEC_DIR / "fernet.key"

for p in (BASE_DIR, MANAGER_DATA, SEC_DIR, LOGS_DIR, BACKUPS_DIR, DOWNLOADS_DIR_INTERNAL, DOWNLOADS_DIR_EXTERNAL):
    try:
        p.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

# SimpleFernet fallback for termux-friendly encryption
class SimpleFernet:
    @staticmethod
    def generate_key():
        return (get_random_bytes(32) if pycrypto_ok else os.urandom(32))
    def __init__(self, key: Optional[bytes] = None):
        raw = key or SimpleFernet.generate_key()
        self.key = hashlib.sha256(raw).digest()
    def encrypt(self, data: bytes) -> bytes:
        if not pycrypto_ok:
            return base64.urlsafe_b64encode(data)
        iv = get_random_bytes(16)
        cipher = AES.new(self.key, AES.MODE_CFB, iv)
        return base64.urlsafe_b64encode(iv + cipher.encrypt(data))
    def decrypt(self, token: bytes) -> bytes:
        try:
            raw = base64.urlsafe_b64decode(token)
            if not pycrypto_ok:
                return raw
            iv, ct = raw[:16], raw[16:]
            cipher = AES.new(self.key, AES.MODE_CFB, iv)
            return cipher.decrypt(ct)
        except Exception:
            return b""

def get_or_create_key() -> bytes:
    if FERNET_KEY_PATH.exists():
        try:
            return FERNET_KEY_PATH.read_bytes()
        except Exception:
            pass
    k = SimpleFernet.generate_key()
    try:
        FERNET_KEY_PATH.write_bytes(k)
        os.chmod(FERNET_KEY_PATH, 0o600)
    except Exception:
        pass
    return k

FERNET_KEY = get_or_create_key()
FERNET = SimpleFernet(FERNET_KEY)

# Config load/save
def load_config() -> Dict[str, Any]:
    if not CFG_PATH.exists():
        return {}
    try:
        return json.loads(CFG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_config(cfg: Dict[str, Any]):
    try:
        CFG_PATH.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
        try:
            os.chmod(CFG_PATH, 0o600)
        except Exception:
            pass
    except Exception:
        pass

cfg = load_config()
cfg.setdefault("owner", None)
cfg.setdefault("version", "tgm_final_v2.6")
cfg.setdefault("CURRENT_VERSION", "2.6")
cfg.setdefault("fast_mode", True)
cfg.setdefault("progress_bar", True)
cfg.setdefault("inline_updates", False)
cfg.setdefault("refresh_interval", 3.0)
cfg.setdefault("auto_backup", False)
cfg.setdefault("auto_update_on_start", False)
cfg.setdefault("aria2_threshold_bytes", 100 * 1024 * 1024)
cfg.setdefault("aria2_enabled", True)
cfg.setdefault("download_external_path", str(DOWNLOADS_DIR_EXTERNAL))
cfg.setdefault("control_group_id", None)
cfg.setdefault("lockout", {"attempts": 0, "blocked_until": 0})
save_config(cfg)

# Utilities
def human_size(n: int) -> str:
    n = float(n or 0)
    for unit in ["B","KB","MB","GB","TB"]:
        if n < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"

def write_diag(s: str):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(DIAG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {s}\n")
    except Exception:
        pass

# Auto-update endpoints (your repo)
UPDATE_URL = "https://raw.githubusercontent.com/maakash52-design/telegram-manager/main/tgm_final_v2.6_aria2_hybrid.py"
VERSION_MANIFEST = "https://raw.githubusercontent.com/maakash52-design/telegram-manager/main/version.json"
CURRENT_VERSION = cfg.get("CURRENT_VERSION", "2.6")

def startup_check_for_update(noninteractive=True):
    if not requests_ok:
        return {"status": "no_network"}
    try:
        r = requests.get(VERSION_MANIFEST, timeout=8)
        r.raise_for_status()
        data = r.json()
        latest = str(data.get("latest_version", "")).strip()
        if not latest:
            return {"status": "manifest_invalid"}
        if latest == CURRENT_VERSION:
            return {"status": "up_to_date", "version": latest}
        info = {"status": "available", "latest": latest}
        cfg["update_available"] = info; save_config(cfg)
        if cfg.get("auto_update_on_start", False):
            resp = requests.get(UPDATE_URL, timeout=15)
            if resp.status_code == 200 and len(resp.text) > 50:
                _replace_script(resp.text)
                return {"status": "applied", "version": latest}
        else:
            print(f"\n[Updater] New version available: v{latest} — run `python3 {Path(__file__).name} --self-update` to apply.")
            return info
    except Exception as e:
        write_diag(f"startup_update error: {e}")
        return {"status": "error", "error": str(e)}

def _replace_script(text: str) -> bool:
    try:
        tmp = MANAGER_DATA / f"tgm_update_{int(time.time())}.py"
        tmp.write_text(text, encoding="utf-8")
        try:
            if os.access(__file__, os.X_OK):
                tmp.chmod(0o755)
        except Exception:
            pass
        os.replace(tmp, Path(__file__))
        return True
    except Exception as e:
        write_diag(f"replace_script_failed: {e}")
        return False

def spawn_startup_update_thread():
    def _bg():
        time.sleep(1.0)
        res = startup_check_for_update(noninteractive=True)
        write_diag(f"startup_update_result: {json.dumps(res)}")
    t = threading.Thread(target=_bg, daemon=True); t.start()

# Telethon helpers
TELETHON_CLIENT: Optional[TelegramClient] = None
TELETHON_SESSION = MANAGER_DATA / "tgm_final.session"

def ensure_telethon(api_id: int, api_hash: str):
    global TELETHON_CLIENT
    try:
        import asyncio
        loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
        client = TelegramClient(str(TELETHON_SESSION), api_id, api_hash)
        loop.run_until_complete(client.start())
        TELETHON_CLIENT = client
        return client
    except Exception as e:
        write_diag(f"telethon start failed: {e}")
        TELETHON_CLIENT = None
        return None

def ensure_control_group(client: TelegramClient) -> Optional[int]:
    try:
        cg = cfg.get("control_group_id")
        if cg:
            try:
                ent = client.get_entity(int(cg)); return int(cg)
            except Exception:
                cfg["control_group_id"] = None; save_config(cfg)
        title = f"Telegram Manager — Control ({cfg.get('owner') or 'Owner'})"
        import asyncio
        loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
        res = loop.run_until_complete(client(functions.channels.CreateChannelRequest(title=title, about="Control channel for Telegram Manager")))
        ch = res.chats[0]; cid = utils.get_peer_id(ch)
        cfg["control_group_id"] = int(cid); save_config(cfg)
        return int(cid)
    except Exception as e:
        write_diag(f"ensure_control_group: {e}")
        return None

def send_inline_update(text: str, edit_msg_id: Optional[int] = None):
    if not cfg.get("inline_updates") or not telethon_available or TELETHON_CLIENT is None or not cfg.get("control_group_id"):
        return None
    try:
        import asyncio
        loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
        async def _send():
            ch = int(cfg.get("control_group_id"))
            if edit_msg_id:
                try:
                    await TELETHON_CLIENT.edit_message(ch, edit_msg_id, text); return edit_msg_id
                except Exception:
                    m = await TELETHON_CLIENT.send_message(ch, text); return m.id
            else:
                m = await TELETHON_CLIENT.send_message(ch, text); return m.id
        return loop.run_until_complete(_send())
    except Exception as e:
        write_diag(f"send_inline_update error: {e}")
        return None

# Chat listing & preview
def simulated_chats() -> List[Dict[str, Any]]:
    out = []
    for i in range(1, 140):
        out.append({"id": 10000 + i, "name": f"Group_{i:03d}", "type": "group"})
    for i in range(1, 90):
        out.append({"id": 20000 + i, "name": f"Contact_{i:03d}", "type": "private"})
    return out

def get_chats() -> List[Dict[str, Any]]:
    if telethon_available and TELETHON_CLIENT:
        try:
            import asyncio
            loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
            async def _get():
                dialogs = []
                async for d in TELETHON_CLIENT.iter_dialogs():
                    ent = d.entity
                    name = getattr(ent, "title", None) or getattr(ent, "first_name", None) or getattr(ent, "username", None) or str(d.id)
                    typ = "private"
                    if getattr(ent, "broadcast", False) or getattr(ent, "megagroup", False) or getattr(ent, "title", None):
                        typ = "group" if getattr(ent, "megagroup", False) else "channel" if getattr(ent, "broadcast", False) else "group"
                    dialogs.append({"id": int(d.id), "name": str(name), "type": typ})
                return dialogs
            return loop.run_until_complete(_get())
        except Exception as e:
            write_diag(f"get_chats error: {e}")
            return simulated_chats()
    return simulated_chats()

def split_and_sort(dialogs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    groups = [d for d in dialogs if d["type"] in ("group", "channel")]
    privs = [d for d in dialogs if d["type"] == "private"]
    return (sorted(groups, key=lambda x: x["name"].lower()), sorted(privs, key=lambda x: x["name"].lower()))

def simulated_preview_by_id(entity_id: int) -> Dict[str, Any]:
    seed = int(entity_id) & 0xffff
    random.seed(seed)
    photos = random.randint(0, 400)
    videos = random.randint(0, 200)
    docs = random.randint(0, 120)
    return {
        "photos": {"count": photos, "size": photos * 1_200_000},
        "videos": {"count": videos, "size": videos * 40_000_000},
        "docs": {"count": docs, "size": docs * 800_000},
        "total": {"count": photos + videos + docs, "size": photos * 1_200_000 + videos * 40_000_000 + docs * 800_000}
    }

def preview_media_for_chat(chat: Dict[str, Any]) -> Dict[str, Any]:
    if telethon_available and TELETHON_CLIENT:
        try:
            import asyncio
            loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
            async def _preview():
                photos = videos = docs = 0; total_bytes = 0
                async for msg in TELETHON_CLIENT.iter_messages(chat["id"], limit=2000):
                    if msg.media:
                        if getattr(msg, "photo", None) or "photo" in str(msg.media).lower():
                            photos += 1
                        elif getattr(msg, "video", None) or "video" in str(msg.media).lower():
                            videos += 1
                        else:
                            docs += 1
                        try:
                            total_bytes += int(msg.file.size) if getattr(msg, 'file', None) and getattr(msg.file, 'size', None) else 0
                        except Exception:
                            pass
                return {"photos": {"count": photos, "size": photos * 1_200_000}, "videos": {"count": videos, "size": videos * 40_000_000}, "docs": {"count": docs, "size": docs * 800_000}, "total": {"count": photos + videos + docs, "size": total_bytes if total_bytes > 0 else photos * 1_200_000 + videos * 40_000_000 + docs * 800_000}}
            return loop.run_until_complete(_preview())
        except Exception as e:
            write_diag(f"preview_media error: {e}")
            return simulated_preview_by_id(chat["id"])
    return simulated_preview_by_id(chat["id"])

# Download engine
ARIA_BIN = shutil.which("aria2c")
ARIA_ENABLED = bool(ARIA_BIN) and bool(cfg.get("aria2_enabled", True))
ARIA_MIN_BYTES = cfg.get("aria2_threshold_bytes", 100 * 1024 * 1024)
EXTERNAL_PATH = Path(cfg.get("download_external_path", str(DOWNLOADS_DIR_EXTERNAL)))
EXTERNAL_PATH.mkdir(parents=True, exist_ok=True)

def measure_speed_sample(timeout=6.0) -> Optional[float]:
    if not requests_ok:
        return None
    try:
        url = "https://www.google.com/favicon.ico"
        t0 = time.time()
        r = requests.get(url, stream=True, timeout=timeout)
        total = 0
        for chunk in r.iter_content(4096):
            total += len(chunk)
            if total >= 40000:
                break
        dt = time.time() - t0
        return (total / dt) if dt > 0 else None
    except Exception:
        return None

def compute_profile(bps: Optional[float], battery_ok: bool = True) -> Dict[str, Any]:
    chunk = 256_000; concurrency = 1; fast_mode = False
    if bps:
        mbps = bps / 1e6
        if mbps >= 10:
            chunk = 1_000_000; concurrency = 4; fast_mode = True
        elif mbps >= 2:
            chunk = 512_000; concurrency = 2; fast_mode = True
        elif mbps >= 0.5:
            chunk = 256_000; concurrency = 1
        else:
            chunk = 128_000; concurrency = 1
    if not battery_ok:
        chunk = max(64_000, chunk // 2); concurrency = 1
    return {"chunk_size": int(chunk), "concurrency": int(concurrency), "fast_mode": fast_mode, "refresh_interval": cfg.get("refresh_interval", 3.0)}

def battery_ok_simple() -> bool:
    if os.getenv("PREFIX") and "com.termux" in os.getenv("PREFIX", ""):
        try:
            out = subprocess.check_output(["termux-battery-status"], stderr=subprocess.DEVNULL).decode()
            j = json.loads(out); lvl = j.get("percentage", 100); ch = j.get("charging", False)
            if lvl < 20 and not ch:
                return False
            return True
        except Exception:
            return True
    return True

def load_state() -> Dict[str, Any]:
    if DOWNLOAD_STATE.exists():
        try:
            return json.loads(DOWNLOAD_STATE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_state(s: Dict[str, Any]):
    try:
        DOWNLOAD_STATE.write_text(json.dumps(s, indent=2), encoding="utf-8")
    except Exception:
        pass

def append_history(chat_name: str, total: int, duration: int):
    try:
        hist = []
        if HISTORY_FILE.exists(): hist = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        hist.append({"chat": chat_name, "size": total, "time": datetime.utcnow().isoformat(), "duration": f"{duration}s"})
        HISTORY_FILE.write_text(json.dumps(hist[-300:], indent=2), encoding="utf-8")
    except Exception:
        pass

# DownloadController (Telethon and aria2 handoff)
from concurrent.futures import ThreadPoolExecutor, as_completed
STATE_SAVE_INTERVAL = 3.0

class DownloadController:
    def __init__(self, profile: Dict[str, Any], dest_dir: Path):
        self.profile = profile
        self.dest = Path(dest_dir); self.dest.mkdir(parents=True, exist_ok=True)
        self._pause = threading.Event(); self._pause.clear()
        self._cancel = threading.Event()
        self._lock = threading.Lock()
        self.downloaded = 0; self.total = 0
        self.last_save = time.time(); self.state = load_state()

    def pause(self): self._pause.set()
    def resume(self): self._pause.clear()
    def cancel(self): self._cancel.set()
    def _wait_if_paused(self):
        while self._pause.is_set() and not self._cancel.is_set():
            time.sleep(0.5)
    def _save_progress(self):
        if time.time() - self.last_save >= STATE_SAVE_INTERVAL:
            save_state(self.state); self.last_save = time.time()

    def _telethon_download(self, chat_id: int, msg_id: int, outpath: Path, assumed_size: int = 0) -> bool:
        if not telethon_available or TELETHON_CLIENT is None:
            return self._simulated(outpath, assumed_size)
        try:
            import asyncio
            loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
            async def _run():
                def _cb(received, total):
                    with self._lock:
                        prev = self.state.get(str(chat_id), {}).get(str(msg_id), {}).get("done", 0)
                        add = max(0, received - prev)
                        self.downloaded += add
                        self.state.setdefault(str(chat_id), {})[str(msg_id)] = {"done": int(received), "total": int(total or assumed_size)}
                        self._save_progress()
                    while self._pause.is_set() and not self._cancel.is_set(): time.sleep(0.5)
                msg = await TELETHON_CLIENT.get_messages(chat_id, ids=msg_id)
                await TELETHON_CLIENT.download_media(msg, file=str(outpath), progress_callback=_cb)
                return outpath.exists()
            ok = loop.run_until_complete(_run()); return ok
        except Exception:
            return False

    def _simulated(self, outpath: Path, size: int = 5_000_000):
        if outpath.exists() and outpath.stat().st_size >= size: return True
        done = 0
        while done < size and not self._cancel.is_set():
            self._wait_if_paused()
            step = min(self.profile.get("chunk_size", 256000), size - done)
            time.sleep(0.05); done += step
            with self._lock: self.downloaded += step
            self._save_progress()
        try:
            with open(outpath, "wb") as f: f.truncate(size)
        except Exception:
            pass
        return not self._cancel.is_set()

    def _aria2_hand_off(self, url: str, outdir: Path, outname: str) -> bool:
        if not ARIA_ENABLED: return False
        outdir.mkdir(parents=True, exist_ok=True)
        cmd = [ARIA_BIN, "-x", "8", "-s", "8", "-j", "4", "-d", str(outdir), "-o", outname, url]
        try:
            subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL)
            return True
        except Exception:
            return False

    def _download_one(self, chat_id: int, item: Dict[str, Any]):
        msg_id = int(item.get("msg_id"))
        size = int(item.get("size", 0))
        title = item.get("title", "file").replace(" ", "_")
        outname = f"{chat_id}_{msg_id}_{title}"
        outpath_internal = self.dest / outname
        outpath_external = EXTERNAL_PATH / outname
        direct_url = item.get("direct_url")
        if size >= ARIA_MIN_BYTES and ARIA_ENABLED:
            if direct_url:
                ok = self._aria2_hand_off(direct_url, EXTERNAL_PATH, outname)
                if ok:
                    with self._lock:
                        self.downloaded += size
                        self.state.setdefault(str(chat_id), {})[str(msg_id)] = {"done": int(size), "total": int(siz

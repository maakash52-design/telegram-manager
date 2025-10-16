#!/usr/bin/env python3
"""
tgm_final_v2.py — Telegram Manager, production-ready unified script.

Key features:
- Auto-deps: telethon, pycryptodome, requests (best-effort install)
- Secure key store (AES via PyCryptodome)
- Adaptive profile (chunk_size, refresh_interval, fast_threshold)
- Telethon integration (login, control group creation, inline updates)
- Media listing (Groups/Channels vs Private chats) alphabetically, paginated
- Preview (photos/videos/docs counts & sizes) — live if Telethon else simulated
- Filtered downloads (photos/videos/docs/all) with pause/resume/cancel
- Persistent download state and resume
- Backup & restore (incremental local backups), daily auto-check (battery-aware)
- Settings persisted in config.json
- Owner-protected restore with lockout
"""

import os
import sys
import json
import time
import random
import threading
import asyncio
import shutil
import getpass
import select
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Callable

# ---------------------
# Basic paths & setup
# ---------------------
APP = "TelegramManager"
BASE_DIR = Path.home() / APP
MANAGER_DATA = BASE_DIR / "ManagerData"
SEC_DIR = MANAGER_DATA / "security"
LOGS_DIR = MANAGER_DATA / "logs"
BACKUPS_DIR = MANAGER_DATA / "backups"
CFG_PATH = SEC_DIR / "config.json"
FERNET_KEY_PATH = SEC_DIR / "fernet.key"
DOWNLOAD_STATE = MANAGER_DATA / "download_state.json"
HISTORY_FILE = MANAGER_DATA / "download_history.json"

for p in (BASE_DIR, MANAGER_DATA, SEC_DIR, LOGS_DIR, BACKUPS_DIR):
    p.mkdir(parents=True, exist_ok=True)

# ---------------------
# Minimal auto-installer
# ---------------------
def try_install(pkg: str):
    try:
        __import__(pkg)
        return True
    except Exception:
        print(f"[Setup] Installing missing package: {pkg}")
        try:
            subprocess_cmd = [sys.executable, "-m", "pip", "install", pkg, "--quiet", "--no-cache-dir"]
            # Use subprocess only at import time to avoid blocking in some environments
            import subprocess as _sp
            _sp.run(subprocess_cmd, check=False)
            __import__(pkg)
            return True
        except Exception:
            return False

# Only import/install what's necessary, try to be quiet
# Telethon is required for real Telegram interaction; otherwise fallback to simulated mode.
telethon_available = False
try:
    from telethon import TelegramClient, events, utils
    from telethon.tl.types import User, Chat, Channel
    telethon_available = True
except Exception:
    # Try to install telethon
    try:
        import subprocess as _sp
        print("[Setup] Telethon missing; attempting to pip install telethon (this may take a while).")
        _sp.run([sys.executable, "-m", "pip", "install", "telethon", "--quiet", "--no-cache-dir"], check=False)
        from telethon import TelegramClient, events, utils
        from telethon.tl.types import User, Chat, Channel
        telethon_available = True
    except Exception:
        telethon_available = False

# PyCryptodome for AES-based fallback crypto
try:
    from Crypto.Cipher import AES
    from Crypto.Random import get_random_bytes
    pycrypto_ok = True
except Exception:
    # try to install
    try:
        import subprocess as _sp
        print("[Setup] pycryptodome missing; attempting to install.")
        _sp.run([sys.executable, "-m", "pip", "install", "pycryptodome", "--quiet", "--no-cache-dir"], check=False)
        from Crypto.Cipher import AES
        from Crypto.Random import get_random_bytes
        pycrypto_ok = True
    except Exception:
        pycrypto_ok = False

# requests is optional but used for network probe
try:
    import requests
    requests_ok = True
except Exception:
    try:
        import subprocess as _sp
        _sp.run([sys.executable, "-m", "pip", "install", "requests", "--quiet", "--no-cache-dir"], check=False)
        import requests
        requests_ok = True
    except Exception:
        requests_ok = False

# ---------------------
# Simple AES-based Fernet-like class (fallback)
# ---------------------
import hashlib
import base64

class SimpleFernet:
    @staticmethod
    def generate_key():
        return get_random_bytes(32) if pycrypto_ok else os.urandom(32)

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

# ---------------------
# Key management
# ---------------------
def get_or_create_key() -> bytes:
    if FERNET_KEY_PATH.exists():
        return FERNET_KEY_PATH.read_bytes()
    key = SimpleFernet.generate_key()
    FERNET_KEY_PATH.write_bytes(key)
    try:
        os.chmod(FERNET_KEY_PATH, 0o600)
    except Exception:
        pass
    return key

FERNET_KEY = get_or_create_key()
FERNET = SimpleFernet(FERNET_KEY)

# ---------------------
# Config helpers
# ---------------------
def load_config() -> Dict[str, Any]:
    if not CFG_PATH.exists():
        return {}
    try:
        return json.loads(CFG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_config(cfg: Dict[str, Any]):
    CFG_PATH.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

cfg = load_config()
# sensible defaults
cfg.setdefault("owner", None)
cfg.setdefault("version", "tgm_final_v2")
cfg.setdefault("adaptive", {})
cfg.setdefault("fast_mode", True)
cfg.setdefault("progress_bar", True)
cfg.setdefault("refresh_interval", 3.0)
cfg.setdefault("auto_backup", True)
cfg.setdefault("inline_updates", False)
cfg.setdefault("control_group_id", None)
cfg.setdefault("lockout", {"attempts": 0, "blocked_until": 0})
save_config(cfg)

# ---------------------
# Adaptive profile (from cfg)
# ---------------------
def get_adaptive_profile(force_probe: bool = False) -> Dict[str, Any]:
    # Use stored value; can extend to real probe like v1.8
    profile = cfg.get("adaptive", {})
    defaults = {"chunk_size": 256_000, "refresh_interval": cfg.get("refresh_interval", 3.0),
                "fast_threshold": 1_000_000, "concurrency": 1}
    for k, v in defaults.items():
        profile.setdefault(k, v)
    return profile

# ---------------------
# Telethon helpers (async)
# ---------------------
TELETHON_CLIENT: Optional[TelegramClient] = None
TELETHON_SESSION = MANAGER_DATA / "tgm_session"
async def telethon_start(api_id: int, api_hash: str):
    global TELETHON_CLIENT
    client = TelegramClient(str(TELETHON_SESSION), api_id, api_hash)
    await client.start()
    TELETHON_CLIENT = client
    return client

async def telethon_stop():
    global TELETHON_CLIENT
    if TELETHON_CLIENT:
        await TELETHON_CLIENT.disconnect()
        TELETHON_CLIENT = None

def ensure_telethon_session_sync(api_id: int, api_hash: str):
    # wrapper to start client synchronously
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        client = loop.run_until_complete(telethon_start(api_id, api_hash))
        return client
    finally:
        pass

# ---------------------
# Control group creation
# ---------------------
async def ensure_control_group_async(client: TelegramClient, create_if_missing: bool = True) -> Optional[int]:
    """
    Ensure control group exists. If control_group_id is in config and valid, keep it.
    Else try to create a new group named 'Telegram Manager - Control' and save its id.
    """
    cg = cfg.get("control_group_id")
    try:
        if cg:
            # check access
            try:
                ent = await client.get_entity(int(cg))
                return int(cg)
            except Exception:
                cfg["control_group_id"] = None
                save_config(cfg)
        if create_if_missing:
            title = f"Telegram Manager — Control ({os.getlogin() if hasattr(os, 'getlogin') else 'Owner'})"
            result = await client(functions.channels.CreateChannelRequest(title=title, about="Control channel for Telegram Manager"))
            # CreateChannelRequest returns a complex structure; extract id
            ch = result.chats[0]
            cid = utils.get_peer_id(ch)
            cfg["control_group_id"] = int(cid)
            save_config(cfg)
            return int(cid)
    except Exception:
        return None

def ensure_control_group_sync(client: TelegramClient) -> Optional[int]:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(ensure_control_group_async(client))
    finally:
        pass

# ---------------------
# Utility: human readable size
# ---------------------
def human_size(n: int) -> str:
    n = float(n)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"

# ---------------------
# Chat listing: live or simulated
# ---------------------
def simulated_chats() -> List[Dict[str, Any]]:
    out = []
    for i in range(1, 60):
        out.append({"id": 1000 + i, "name": f"Python Experts {i:03d}", "type": "group"})
    for i in range(1, 40):
        out.append({"id": 2000 + i, "name": f"Alice_{i}", "type": "private"})
    for i in range(1, 6):
        out.append({"id": 900000 + i, "name": f"Channel_{i}", "type": "channel"})
    return out

async def telethon_get_chats_async(client: TelegramClient) -> List[Dict[str, Any]]:
    dialogs = []
    async for d in client.iter_dialogs():
        ent = d.entity
        name = getattr(ent, "title", None) or getattr(ent, "first_name", None) or getattr(ent, "username", None) or str(d.id)
        typ = "private"
        if getattr(ent, "broadcast", False) or getattr(ent, "megagroup", False) or getattr(ent, "title", None):
            typ = "group" if getattr(ent, "megagroup", False) else "channel" if getattr(ent, "broadcast", False) else "group"
        dialogs.append({"id": int(d.id), "name": str(name), "type": typ})
    return dialogs

def get_chats() -> List[Dict[str, Any]]:
    if telethon_available and TELETHON_CLIENT:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(telethon_get_chats_async(TELETHON_CLIENT))
        except Exception:
            return simulated_chats()
    else:
        return simulated_chats()

def split_and_sort(dialogs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    groups = [d for d in dialogs if d["type"] in ("group", "channel")]
    privs = [d for d in dialogs if d["type"] == "private"]
    groups_sorted = sorted(groups, key=lambda x: x["name"].lower())
    privs_sorted = sorted(privs, key=lambda x: x["name"].lower())
    return groups_sorted, privs_sorted

# ---------------------
# Preview media (live via telethon if possible)
# ---------------------
async def telethon_preview_async(client: TelegramClient, entity_id: int) -> Dict[str, Any]:
    # Count messages with media types (this can be slow for very large chats; we fetch limited number to estimate)
    photos = videos = docs = 0
    total_bytes = 0
    count_limit = 2000  # safety cap
    try:
        async for msg in client.iter_messages(entity_id, limit=count_limit):
            if msg.media:
                typ = getattr(msg.media, "__class__", None)
                # heuristics
                if getattr(msg, "photo", None) or "photo" in str(typ).lower():
                    photos += 1
                elif getattr(msg, "video", None) or "video" in str(typ).lower():
                    videos += 1
                else:
                    docs += 1
                # try to get size estimate
                if hasattr(msg, "file") and getattr(msg.file, "size", None):
                    total_bytes += int(msg.file.size)
        # if counts zero -> fallback to simulated
        return {"photos": {"count": photos, "size": photos * 1_200_000},
                "videos": {"count": videos, "size": videos * 40_000_000},
                "docs": {"count": docs, "size": docs * 800_000},
                "total": {"count": photos + videos + docs, "size": total_bytes if total_bytes > 0 else (photos*1_200_000 + videos*40_000_000 + docs*800_000)}}
    except Exception:
        # fallback simulated
        return simulated_preview_by_id(entity_id)

def simulated_preview_by_id(entity_id: int) -> Dict[str, Any]:
    seed = int(entity_id) & 0xFFFF
    random.seed(seed)
    photos = random.randint(10, 400)
    videos = random.randint(2, 120)
    docs = random.randint(0, 80)
    return {
        "photos": {"count": photos, "size": photos * 1_200_000},
        "videos": {"count": videos, "size": videos * 40_000_000},
        "docs": {"count": docs, "size": docs * 800_000},
        "total": {"count": photos + videos + docs, "size": photos * 1_200_000 + videos * 40_000_000 + docs * 800_000},
    }

def preview_media_for_chat(chat: Dict[str, Any]) -> Dict[str, Any]:
    if telethon_available and TELETHON_CLIENT:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(telethon_preview_async(TELETHON_CLIENT, chat["id"]))
        except Exception:
            return simulated_preview_by_id(chat["id"])
    else:
        return simulated_preview_by_id(chat["id"])

# ---------------------
# Download orchestration
# ---------------------
# Persistent state helpers
def load_state() -> Dict[str, Any]:
    if not DOWNLOAD_STATE.exists():
        return {}
    try:
        return json.loads(DOWNLOAD_STATE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_state(state: Dict[str, Any]):
    DOWNLOAD_STATE.write_text(json.dumps(state, indent=2), encoding="utf-8")

def clear_state_for(chat_id: int):
    s = load_state()
    s.pop(str(chat_id), None)
    save_state(s)

# History logging
def append_history(chat_name: str, total: int, duration_s: int):
    h = []
    if HISTORY_FILE.exists():
        try:
            h = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        except Exception:
            h = []
    entry = {"chat": chat_name, "size": total, "time": time.strftime("%Y-%m-%d %H:%M:%S"), "duration": f"{duration_s}s"}
    h.append(entry)
    # keep last 200
    h = h[-200:]
    HISTORY_FILE.write_text(json.dumps(h, indent=2), encoding="utf-8")

# progress update to control group (if enabled)
def send_inline_update(text: str, edit_msg_id: Optional[int] = None):
    if not cfg.get("inline_updates") or not telethon_available or not TELETHON_CLIENT or not cfg.get("control_group_id"):
        return None
    # send or edit message
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        async def _send():
            ch = int(cfg.get("control_group_id"))
            if edit_msg_id:
                try:
                    await TELETHON_CLIENT.edit_message(ch, edit_msg_id, text)
                    return edit_msg_id
                except Exception:
                    # fallback to send new
                    m = await TELETHON_CLIENT.send_message(ch, text)
                    return m.id
            else:
                m = await TELETHON_CLIENT.send_message(ch, text)
                return m.id
        return loop.run_until_complete(_send())
    except Exception:
        return None

def download_with_telethon(chat, selection: Dict[str, Any], profile: Dict[str, Any]):
    """
    Download using Telethon, with progress callback. selection contains types, count, size.
    This function will download media messages matching selection types.
    Note: For large groups this may take long. This function is synchronous (wraps async).
    """
    if not telethon_available or not TELETHON_CLIENT:
        # fallback simulation
        download_simulated(chat, selection, profile)
        return

    # Build criteria: telethon iteration and client.download_media
    total_bytes = selection.get("size", 0)
    state = load_state()
    key = str(chat["id"])
    started_at = time.time()
    downloaded = 0
    # load resume
    if key in state:
        downloaded = int(state[key].get("done", 0))

    # send initial inline message (if enabled)
    inline_msg_id = None
    if cfg.get("inline_updates") and cfg.get("control_group_id"):
        inline_msg_id = send_inline_update(f"Starting download for {chat['name']} — 0%")

    # iterate messages and download accordingly
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    async def _runner():
        nonlocal downloaded, inline_msg_id
        # source filter: choose media types mapping
        types = selection.get("types", ["photos","videos","docs"])
        async for msg in TELETHON_CLIENT.iter_messages(chat["id"], limit=None):
            if not msg.media:
                continue
            # decide msg category
            is_photo = hasattr(msg, "photo") or "photo" in str(msg.media).lower()
            is_video = hasattr(msg, "video") or "video" in str(msg.media).lower()
            is_doc = not (is_photo or is_video)
            want = (is_photo and "photos" in types) or (is_video and "videos" in types) or (is_doc and "docs" in types)
            if not want:
                continue
            # attempt download
            try:
                # progress callback
                def _cb(curr, total):
                    nonlocal downloaded, inline_msg_id
                    # curr is bytes for this file; we keep simple aggregate
                    # update state
                    # Note: Telethon's callback is per-file; we'll approximate aggregate by adding curr delta.
                    downloaded += curr
                    state[key] = {"chat": chat["name"], "total": total_bytes, "done": int(downloaded)}
                    save_state(state)
                    pct = downloaded / max(1, total_bytes) * 100
                    # update inline periodically
                    if cfg.get("inline_updates"):
                        send_inline_update(f"{chat['name']} — {pct:.1f}% ({human_size(int(downloaded))}/{human_size(total_bytes)})", edit_msg_id=inline_msg_id)
                await TELETHON_CLIENT.download_media(msg, file=MANAGER_DATA / str(chat["id"]), progress_callback=_cb)
            except Exception:
                # skip errors
                continue
        return downloaded
    try:
        final_downloaded = loop.run_until_complete(_runner())
    except Exception:
        final_downloaded = downloaded
    duration = int(time.time() - started_at)
    if inline_msg_id:
        send_inline_update(f"Finished {chat['name']} — {human_size(int(final_downloaded))} in {duration}s", edit_msg_id=inline_msg_id)
    # cleanup state
    clear_state_for(chat["id"])
    append_history(chat["name"], int(final_downloaded), duration)

def download_simulated(chat, selection: Dict[str, Any], profile: Dict[str, Any]):
    total_bytes = int(selection.get("size", 0))
    if total_bytes <= 0:
        print("Nothing to download.")
        return
    key = str(chat["id"])
    state = load_state()
    done = int(state.get(key, {}).get("done", 0))
    chunk = profile.get("chunk_size", 256_000)
    refresh = profile.get("refresh_interval", cfg.get("refresh_interval", 3.0))
    fast_thr = profile.get("fast_threshold", 1_000_000)
    fast_mode_enabled = cfg.get("fast_mode", True)
    paused = False
    canceled = False
    started = time.time()

    print("\nControls: 'p' pause, 'r' resume, 'c' cancel (press key + Enter).")
    while done < total_bytes and not canceled:
        # non-blocking stdin check
        if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
            line = sys.stdin.readline().strip().lower()
            if line == "p":
                paused = True
                print("[Paused]")
            elif line == "r":
                paused = False
                print("[Resumed]")
            elif line == "c":
                canceled = True
                print("[Canceled]")
                break
        if paused:
            time.sleep(0.5)
            continue
        # simulate variable speed
        speed = 6_000_000 * (0.7 + random.random() * 0.8)
        step = min(int(speed * 0.5), chunk * 10)
        time.sleep(0.5)
        done += step
        if done > total_bytes:
            done = total_bytes
        # save state every 3 seconds-ish
        elapsed = time.time() - started
        if int(elapsed) % 3 == 0:
            st = load_state()
            st[key] = {"chat": chat["name"], "total": total_bytes, "done": int(done)}
            save_state(st)
        pct = done / total_bytes * 100
        avg_spd = done / max(1, elapsed)
        eta = int((total_bytes - done) / max(1, avg_spd))
        # adaptive fast mode
        if fast_mode_enabled:
            if avg_spd > fast_thr:
                # we could enlarge chunk
                chunk = min(chunk * 2, 4_000_000)
                mode_tag = "⚡"
            else:
                mode_tag = ""
        else:
            mode_tag = ""
        # display
        if cfg.get("progress_bar", True):
            bar_len = 30
            filled = int(bar_len * pct / 100)
            bar = "█" * filled + "░" * (bar_len - filled)
            print(f"\r{mode_tag}[{bar}] {pct:5.1f}%  {human_size(int(done))}/{human_size(total_bytes)}  | {human_size(int(avg_spd))}/s  ETA: {eta}s", end="", flush=True)
        else:
            print(f"\r{pct:5.1f}% {human_size(int(done))}/{human_size(total_bytes)} ETA:{eta}s", end="", flush=True)
    print()
    duration = int(time.time() - started)
    if canceled:
        st = load_state()
        st[key] = {"chat": chat["name"], "total": total_bytes, "done": int(done)}
        save_state(st)
        print(f"Canceled; partial saved ({human_size(int(done))})")
    else:
        clear_state_for(chat["id"])
        append_history(chat["name"], total_bytes, duration)
        print(f"Download complete: {human_size(total_bytes)} in {duration}s (avg {human_size(int(total_bytes/max(1,duration)))})")

# ---------------------
# UI: paginated chooser
# ---------------------
def paginate(items: List[Dict[str, Any]], page_size: int = 100):
    for i in range(0, len(items), page_size):
        yield items[i:i + page_size], (i // page_size) + 1, (len(items) + page_size - 1) // page_size

def choose_from_list(items: List[Dict[str, Any]], title: str):
    if not items:
        print(f"\n{title} — (no items)")
        input("Press Enter to return.")
        return None
    pages = list(paginate(items, 100))
    page_index = 0
    while True:
        page_items, page_num, total_pages = pages[page_index]
        print(f"\n{title} — page {page_num}/{total_pages}")
        print("-" * 48)
        for idx, item in enumerate(page_items, start=1 + page_index * 100):
            print(f"[{idx:03d}] {item['name']}")
        print("-" * 48)
        cmd = input("Enter number to select, n=next, p=prev, q=quit: ").strip().lower()
        if cmd == "n":
            if page_index + 1 < len(pages):
                page_index += 1
            else:
                print("Already last page.")
        elif cmd == "p":
            if page_index > 0:
                page_index -= 1
            else:
                print("Already first page.")
        elif cmd == "q":
            return None
        elif cmd.isdigit():
            num = int(cmd)
            if 1 <= num <= len(items):
                return items[num - 1]
            else:
                print("Number out of range.")
        else:
            print("Unknown command.")

# ---------------------
# Menus & flows
# ---------------------
def media_download_flow():
    print("\n📥 Media Download — Listing chats")
    dialogs = get_chats()
    groups, privs = split_and_sort(dialogs)
    print(f"Found: {len(groups)} groups/channels and {len(privs)} private chats.")
    while True:
        print("\n1) Groups / Channels")
        print("2) Private Chats")
        print("3) Back to Main Menu")
        sel = input("Choose category: ").strip()
        if sel == "1":
            chosen = choose_from_list(groups, "Groups / Channels (alphabetical)")
            if chosen:
                run_preview_and_download(chosen)
        elif sel == "2":
            chosen = choose_from_list(privs, "Private Chats (alphabetical)")
            if chosen:
                run_preview_and_download(chosen)
        elif sel == "3":
            break
        else:
            print("Invalid option.")

def run_preview_and_download(chat: Dict[str, Any]):
    print(f"\nSelected: {chat['name']}")
    print("Previewing media (this may take a moment)...")
    breakdown = preview_media_for_chat(chat)
    while True:
        print("\nPreview media list:")
        print(f" Photos: {breakdown['photos']['count']}  |  Size: {human_size(breakdown['photos']['size'])}")
        print(f" Videos: {breakdown['videos']['count']}  |  Size: {human_size(breakdown['videos']['size'])}")
        print(f" Docs : {breakdown['docs']['count']}  |  Size: {human_size(breakdown['docs']['size'])}")
        print(" -------------------------------------------")
        print(f" Total: {breakdown['total']['count']} files  |  {human_size(breakdown['total']['size'])}")
        print("\nOptions:")
        print("1) Filter (choose types)")
        print("2) Start Download")
        print("3) Back")
        cmd = input("Choose: ").strip()
        if cmd == "1":
            print("\nFilter Options:")
            print("1) Photos only")
            print("2) Videos only")
            print("3) Docs only")
            print("4) All media")
            f = input("Choose filter: ").strip()
            if f == "1":
                selection = {"types": ["photos"]}
            elif f == "2":
                selection = {"types": ["videos"]}
            elif f == "3":
                selection = {"types": ["docs"]}
            else:
                selection = {"types": ["photos", "videos", "docs"]}
            sel_count = sum(breakdown[t]["count"] for t in selection["types"])
            sel_size = sum(breakdown[t]["size"] for t in selection["types"])
            selection["count"] = sel_count
            selection["size"] = sel_size
            print(f"\nSelected filter: {', '.join(selection['types'])} -> {selection['count']} files, {human_size(selection['size'])}")
            if input("Use this filter? [Y/n]: ").strip().lower() or "y":
                # Confirmed - proceed to start download prompt
                pass
            else:
                continue
            # ask to proceed
            if selection["size"] == 0:
                print("Nothing to download for this selection.")
                return
            if input(f"Start download ({human_size(selection['size'])})? [Y/n]: ").strip().lower() != "n":
                profile = get_adaptive_profile()
                if telethon_available and TELETHON_CLIENT:
                    download_with_telethon(chat, selection, profile)
                else:
                    download_simulated(chat, selection, profile)
                return
        elif cmd == "2":
            selection = {"types": ["photos", "videos", "docs"], "count": breakdown["total"]["count"], "size": breakdown["total"]["size"]}
            if selection["size"] == 0:
                print("Nothing to download.")
                return
            if input(f"Start download ({human_size(selection['size'])})? [Y/n]: ").strip().lower() != "n":
                profile = get_adaptive_profile()
                if telethon_available and TELETHON_CLIENT:
                    download_with_telethon(chat, selection, profile)
                else:
                    download_simulated(chat, selection, profile)
                return
        elif cmd == "3":
            return
        else:
            print("Invalid option.")

# ---------------------
# Backup & restore
# ---------------------
def make_backup(auto: bool = False):
    ts = time.strftime("%Y%m%d_%H%M%S")
    bk_dir = BACKUPS_DIR / f"backup_{ts}"
    bk_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "state": DOWNLOAD_STATE,
        "history": HISTORY_FILE,
        "config": CFG_PATH
    }
    for k, p in files.items():
        if p.exists():
            dest = bk_dir / f"{k}.json"
            dest.write_text(p.read_text(encoding="utf-8"))
    cfg["last_backup"] = ts
    save_config(cfg)
    print(f"{'[Auto]' if auto else ''} Backup complete → {bk_dir}")

def list_backups() -> List[Path]:
    return sorted([d for d in BACKUPS_DIR.iterdir() if d.is_dir()], reverse=True)

def restore_backup_flow():
    backups = list_backups()
    if not backups:
        print("No backups found.")
        return
    print("\nAvailable Backups:")
    for i, b in enumerate(backups[:10], 1):
        print(f"[{i}] {b.name}")
    c = input("Select number or q: ").strip()
    if c.lower() == "q":
        return
    if c.isdigit() and 1 <= int(c) <= len(backups[:10]):
        sel = backups[int(c) - 1]
        # owner-protected restore
        if cfg.get("owner"):
            # prompt owner code; implement lockout
            lock = cfg.get("lockout", {"attempts": 0, "blocked_until": 0})
            now_ts = int(time.time())
            if lock.get("blocked_until", 0) > now_ts:
                print("Device locked due to failed restore attempts. Try later.")
                return
            code = getpass.getpass("Enter owner code to restore (owner's telegram code): ").strip()
            # here we simply compare with stored encoded owner code if present
            stored_code_enc = cfg.get("owner_code_enc")
            if stored_code_enc:
                try:
                    dec = FERNET.decrypt(stored_code_enc.encode())
                    if dec.decode() != code:
                        lock["attempts"] = lock.get("attempts", 0) + 1
                        if lock["attempts"] >= 3:
                            lock["blocked_until"] = now_ts + 24 * 3600
                            print("Too many failed attempts. Device locked for 24 hours.")
                        cfg["lockout"] = lock
                        save_config(cfg)
                        print("Owner code mismatch. Abort.")
                        return
                except Exception:
                    print("Failed to verify owner code. Abort.")
                    return
            else:
                # no stored code -> allow restore (or require owner to set code previously)
                print("No owner code stored; proceeding (silent).")
        # perform restore
        for f in sel.glob("*.json"):
            name = f.stem
            dest = MANAGER_DATA / f"{name}.json" if name != "config" else CFG_PATH
            dest.write_text(f.read_text(encoding="utf-8"))
        print("Restore complete. Restart script to apply settings.")

def auto_backup_check():
    if not cfg.get("auto_backup", True):
        return
    last = cfg.get("last_backup", "")
    today = time.strftime("%Y%m%d")
    if last.startswith(today):
        return  # already backed up today
    # battery-aware: attempt to check battery (simple)
    if not battery_ok_simple():
        print("[Backup] Skipping auto-backup due to battery/network.")
        return
    make_backup(auto=True)

def battery_ok_simple() -> bool:
    # best-effort: if termux present, try termux-battery-status; else assume ok
    if os.getenv("PREFIX") and "com.termux" in os.getenv("PREFIX", ""):
        try:
            out = subprocess.check_output(["termux-battery-status"], stderr=subprocess.DEVNULL).decode()
            j = json.loads(out)
            lvl = j.get("percentage", 100)
            charging = j.get("charging", False)
            if lvl < 20 and not charging:
                return False
            return True
        except Exception:
            return True
    return True

# ---------------------
# Settings menu
# ---------------------
def settings_menu():
    while True:
        fm = "ON" if cfg.get("fast_mode", True) else "OFF"
        pb = "ON" if cfg.get("progress_bar", True) else "OFF"
        iu = "ON" if cfg.get("inline_updates", False) else "OFF"
        ri = cfg.get("refresh_interval", 3.0)
        ab = "ON" if cfg.get("auto_backup", True) else "OFF"
        print("\n⚙️ Settings")
        print(f"1) Fast Mode: {fm}")
        print(f"2) Progress Bar: {pb}")
        print(f"3) Inline Updates (chat): {iu}")
        print(f"4) Refresh Interval: {ri}s")
        print(f"5) Auto Backup (daily): {ab}")
        print("6) Control Group: Create/Show")
        print("7) Back")
        ch = input("Select: ").strip()
        if ch == "1":
            cfg["fast_mode"] = not cfg.get("fast_mode", True)
            save_config(cfg)
        elif ch == "2":
            cfg["progress_bar"] = not cfg.get("progress_bar", True)
            save_config(cfg)
        elif ch == "3":
            cfg["inline_updates"] = not cfg.get("inline_updates", False)
            save_config(cfg)
        elif ch == "4":
            try:
                v = float(input("Enter refresh interval seconds (0.5-10): ").strip())
                cfg["refresh_interval"] = max(0.5, min(10.0, v))
                save_config(cfg)
            except Exception:
                print("Invalid value.")
        elif ch == "5":
            cfg["auto_backup"] = not cfg.get("auto_backup", True)
            save_config(cfg)
        elif ch == "6":
            if telethon_available:
                if TELETHON_CLIENT is None:
                    print("Not connected to Telegram. Please use 'Connect to Telegram' from main menu first.")
                else:
                    cid = ensure_control_group_sync(TELETHON_CLIENT)
                    if cid:
                        print(f"Control group ready: id {cid}")
                    else:
                        print("Failed to create control group.")
            else:
                print("Telethon not available; cannot create control group.")
        elif ch == "7":
            return
        else:
            print("Invalid option.")

# ---------------------
# Resume & history menu
# ---------------------
def resume_menu():
    s = load_state()
    if not s:
        print("\nNo partial downloads.")
        return
    keys = list(s.keys())
    print("\n🕓 Resume Downloads")
    for i, k in enumerate(keys, 1):
        d = s[k]
        print(f"[{i}] {d['chat']} — {human_size(d['done'])}/{human_size(d['total'])}")
    c = input("Select number or q: ").strip()
    if c.lower() == "q":
        return
    if c.isdigit() and 1 <= int(c) <= len(keys):
        key = keys[int(c) - 1]
        d = s[key]
        chat = {"id": int(key), "name": d["chat"]}
        sel = {"size": d["total"], "types": ["photos", "videos", "docs"]}
        profile = get_adaptive_profile()
        if telethon_available and TELETHON_CLIENT:
            download_with_telethon(chat, sel, profile)
        else:
            download_simulated(chat, sel, profile)

def history_menu():
    if not HISTORY_FILE.exists():
        print("\nNo history yet.")
        return
    try:
        h = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        print("\nHistory corrupted.")
        return
    print("\n📜 Download History (latest):")
    for i, e in enumerate(h[-20:], 1):
        print(f"[{i}] {e['chat']} — {human_size(e['size'])} — {e['time']} — {e['duration']}")
    if input("\nClear history? [y/N]: ").strip().lower() == "y":
        HISTORY_FILE.unlink(missing_ok=True)
        print("History cleared.")

# ---------------------
# Main menu and integration
# ---------------------
def connect_telegram_flow():
    if not telethon_available:
        print("Telethon not available. Install telethon to enable live Telegram integration.")
        return
    # check api_id/api_hash in config
    api_id = cfg.get("api_id")
    api_hash = cfg.get("api_hash")
    if not api_id or not api_hash:
        try:
            api_id = int(input("Enter your Telegram API ID: ").strip())
            api_hash = input("Enter your Telegram API Hash: ").strip()
            cfg["api_id"] = api_id
            cfg["api_hash"] = api_hash
            save_config(cfg)
        except Exception:
            print("Invalid input.")
            return
    # start client synchronously
    print("Starting Telethon client. It may prompt for your phone number and code.")
    client = ensure_telethon_session_sync(int(api_id), api_hash)
    # ensure control group saved
    if client:
        cid = ensure_control_group_sync(client)
        if cid:
            print(f"Control group ready: {cid}")
        else:
            print("Control group not set.")
    else:
        print("Failed to start Telethon client.")

def main_menu():
    # auto backup check once at startup in background
    if cfg.get("auto_backup", True):
        threading.Thread(target=auto_backup_check, daemon=True).start()
    while True:
        print(f"\n[{APP}] v{cfg.get('version', 'tgm_final_v2')}")
        print(f"Owner: {cfg.get('owner') or 'Not set'}")
        print("────────────────────────────")
        print("1) 📥 Media Download")
        print("2) 💾 Backup & Restore")
        print("3) ⚙️ Settings")
        print("4) 🕓 Resume Downloads")
        print("5) 📜 History")
        print("6) 🔌 Connect to Telegram")
        print("7) 🚪 Exit")
        print("────────────────────────────")
        choice = input("Select option: ").strip()
        if choice == "1":
            media_download_flow()
        elif choice == "2":
            print("\n💾 Backup & Restore")
            print("1) Create Backup Now")
            print("2) Restore Backup")
            print("3) Back")
            ch = input("Select: ").strip()
            if ch == "1":
                make_backup()
            elif ch == "2":
                restore_backup_flow()
        elif choice == "3":
            settings_menu()
        elif choice == "4":
            resume_menu()
        elif choice == "5":
            history_menu()
        elif choice == "6":
            connect_telegram_flow()
        elif choice == "7":
            print("Exiting. Goodbye.")
            # ensure Telethon client disconnected gracefully
            if telethon_available and TELETHON_CLIENT:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(telethon_stop())
                except Exception:
                    pass
            break
        else:
            print("Invalid option. Try again.")

# ---------------------
# Entrypoint
# ---------------------
# ---------------------
# Self-update & Diagnostics
# ---------------------
import datetime
import subprocess
import requests

DIAG_FILE = MANAGER_DATA / "diagnostics.log"
UPDATE_URL = "https://raw.githubusercontent.com/maakash52-design/telegram-manager/main/tgm_final_v2.py"
VERSION_MANIFEST = "https://raw.githubusercontent.com/maakash52-design/telegram-manager/main/version.json"
CURRENT_VERSION = "2.1"

def write_diag(entry: str):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(DIAG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {entry}\n")

def run_diagnostics():
    info = {
        "version": CURRENT_VERSION,
        "owner": cfg.get("owner", "Unknown"),
        "battery_ok": battery_ok_simple() if 'battery_ok_simple' in globals() else True,
        "network": "OK",
        "telethon": "ready" if 'telethon_available' in globals() and telethon_available else "simulated",
    }
    print("\n📊 Diagnostics Summary")
    for k, v in info.items():
        print(f"  {k}: {v}")
    write_diag(json.dumps(info))
    print(f"\nSaved to {DIAG_FILE}")

def check_for_update():
    try:
        version_info = requests.get(VERSION_MANIFEST, timeout=10).json()
        latest = version_info.get("latest_version", "2.0")
        if latest != CURRENT_VERSION:
            print(f"[Updater] v{latest} available → current v{CURRENT_VERSION}")
            if input("Download and update now? [y/N]: ").strip().lower().startswith("y"):
                code = requests.get(UPDATE_URL, timeout=15).text
                Path(__file__).write_text(code, encoding="utf-8")
                print("✅ Update installed. Restart the app.")
        else:
            print("[Updater] You are already on the latest version.")
    except Exception as e:
        print(f"[Updater] Error checking update: {e}")

# handle command-line args
if len(sys.argv) > 1:
    if sys.argv[1] == "--self-update":
        check_for_update()
        sys.exit(0)
    elif sys.argv[1] == "--diag":
        run_diagnostics()
        sys.exit(0)
if __name__ == "__main__":
    # owner setup if missing
    if not cfg.get("owner"):
        print("Initial setup — creating owner record.")
        cfg["owner"] = input("Enter owner name: ").strip() or "Owner"
        # ask to store owner's restore code optionally
        if input("Store an owner restore code (recommended)? [y/N]: ").strip().lower().startswith("y"):
            code = getpass.getpass("Enter a numeric or text Owner Code (used for owner-restore): ").strip()
            if code:
                enc = FERNET.encrypt(code.encode())
                cfg["owner_code_enc"] = enc.decode() if isinstance(enc, bytes) else enc
        save_config(cfg)
    print(f"[{APP}] v{cfg.get('version')} — ready.")
    try:
        main_menu()
    except KeyboardInterrupt:
        print("\nInterrupted — exiting cleanly.")

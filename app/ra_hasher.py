"""
RetroAchievements PS2 hash computation.

PS2 RA hash algorithm:
  1. Extract CHD to BIN/CUE via chdman extractcd
  2. Parse ISO9660 Mode2/XA filesystem from BIN
  3. Read SYSTEM.CNF to find boot executable (SLUS_xxx.xx / SCES_xxx.xx)
  4. Read that executable from disc
  5. MD5 hash the raw executable bytes

Reference: rcheevos rc_hash_psx / rc_hash_ps2 implementation
"""

import os, re, hashlib, struct, subprocess, tempfile, shutil, logging
from urllib.request import urlopen, Request
from urllib.error import URLError
import json

logger = logging.getLogger(__name__)

# PS2 disc sector geometry
SECTOR_SIZE          = 2352   # raw sector bytes
MODE2_SYNC_SIZE      = 12     # sync pattern
MODE2_HEADER_SIZE    = 4      # MSF + mode byte
MODE2_SUBHEADER_SIZE = 8      # subheader repeated twice
MODE2_USER_OFFSET    = MODE2_SYNC_SIZE + MODE2_HEADER_SIZE + MODE2_SUBHEADER_SIZE  # = 24
MODE2_USER_SIZE      = 2048
MODE1_USER_OFFSET    = MODE2_SYNC_SIZE + MODE2_HEADER_SIZE  # = 16
MODE1_USER_SIZE      = 2048


# ── Low-level sector reader ───────────────────────────────────────

def _read_sector(f, lba, user_offset=MODE2_USER_OFFSET):
    """Read 2048 bytes of user data from a raw BIN sector."""
    f.seek(lba * SECTOR_SIZE + user_offset)
    return f.read(MODE2_USER_SIZE)


def _detect_user_offset(f):
    """
    Auto-detect whether disc is Mode1 or Mode2 by checking the ISO9660
    magic bytes at LBA 16.  Returns user_offset or None if unreadable.
    """
    for offset in (MODE2_USER_OFFSET, MODE1_USER_OFFSET):
        f.seek(16 * SECTOR_SIZE + offset)
        pvd = f.read(8)
        if len(pvd) >= 6 and pvd[1:6] == b'CD001':
            return offset
    return None


# ── ISO9660 directory traversal ───────────────────────────────────

def _iter_dir_entries(f, dir_lba, dir_size, user_offset):
    """Yield (name_upper, flags, file_lba, file_size) for each directory entry."""
    data = b''
    for i in range((dir_size + 2047) // 2048):
        data += _read_sector(f, dir_lba + i, user_offset)

    offset = 0
    while offset < min(dir_size, len(data)):
        rec_len = data[offset]
        if rec_len == 0:
            # Pad to next 2048-byte sector boundary within this extent
            next_sec = ((offset // 2048) + 1) * 2048
            offset = next_sec
            continue
        if offset + rec_len > len(data):
            break

        name_len  = data[offset + 32]
        name_raw  = data[offset + 33: offset + 33 + name_len]
        flags     = data[offset + 25]
        file_lba  = struct.unpack_from('<I', data, offset + 2)[0]
        file_size = struct.unpack_from('<I', data, offset + 10)[0]

        # Strip ISO9660 version suffix (;1) and uppercase
        try:
            name = name_raw.decode('ascii', errors='replace').split(';')[0].upper()
        except Exception:
            name = ''

        yield name, flags, file_lba, file_size
        offset += rec_len


def _read_file_from_iso(f, file_lba, file_size, user_offset):
    """Read a file's bytes given its LBA and size."""
    data = b''
    for i in range((file_size + 2047) // 2048):
        data += _read_sector(f, file_lba + i, user_offset)
    return data[:file_size]


def _find_in_root(f, user_offset, target_name):
    """Find a file in the root directory by name (case-insensitive). Returns (lba, size) or (None,None)."""
    pvd = _read_sector(f, 16, user_offset)
    root_lba  = struct.unpack_from('<I', pvd, 156 + 2)[0]
    root_size = struct.unpack_from('<I', pvd, 156 + 10)[0]
    target = target_name.upper()
    for name, flags, lba, size in _iter_dir_entries(f, root_lba, root_size, user_offset):
        if name == target:
            return lba, size
    return None, None


# ── SYSTEM.CNF parser ─────────────────────────────────────────────

def _parse_system_cnf(data):
    """
    Extract the boot executable name from SYSTEM.CNF.
    Returns bare filename like 'SLUS_200.40' or None.
    """
    try:
        text = data.decode('ascii', errors='replace')
    except Exception:
        return None

    for line in text.splitlines():
        if 'BOOT2' in line.upper():
            # e.g. "BOOT2 = cdrom0:\SLUS_200.40;1"
            # or   "BOOT2 = cdrom0:SLUS_200.40;1"
            match = re.search(
                r'(?:cdrom\d*[:\\/]+)?([A-Z]{2,6}[_-]\d{3}[\._]\d{2})',
                line, re.IGNORECASE
            )
            if match:
                return match.group(1).upper()
    return None


# ── CUE parser ────────────────────────────────────────────────────

def _first_data_track_bin(cue_path):
    """
    Parse a CUE sheet and return the full path of the first MODE2/2352 (or
    any BINARY) track file.  Returns None if not found.
    """
    cue_dir = os.path.dirname(cue_path)
    current_file = None
    try:
        with open(cue_path, errors='replace') as f:
            for line in f:
                line = line.strip()
                if line.upper().startswith('FILE'):
                    parts = line.split('"')
                    if len(parts) >= 2:
                        current_file = os.path.join(cue_dir, parts[1])
                elif line.upper().startswith('TRACK') and current_file:
                    # Return on first data track
                    if 'MODE' in line.upper() or 'AUDIO' not in line.upper():
                        if os.path.exists(current_file):
                            return current_file
    except Exception:
        pass
    # Fallback: return first BIN in the same folder
    for f in os.listdir(cue_dir):
        if f.lower().endswith('.bin'):
            return os.path.join(cue_dir, f)
    return None


# ── RA API ────────────────────────────────────────────────────────

RA_GAMEID_URL  = "https://retroachievements.org/dorequest.php?r=gameid&m={hash}"
RA_GAMEINFO_URL = "https://retroachievements.org/API/API_GetGame.php?z={user}&y={key}&i={gid}"

def lookup_ra_hash(md5_hash, ra_username=None, ra_api_key=None, timeout=8):
    """
    Check a hash against RA.
    Returns dict: {found: bool, game_id: int|None, game_title: str|None, error: str|None}
    """
    result = {"found": False, "game_id": None, "game_title": None, "error": None}
    try:
        url = RA_GAMEID_URL.format(hash=md5_hash)
        req = Request(url, headers={"User-Agent": "CHD-Converter/1.1.0"})
        with urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read())
        if data.get("Success") and data.get("GameID", 0) > 0:
            result["found"]   = True
            result["game_id"] = data["GameID"]
        else:
            return result  # hash not in RA database
    except URLError as e:
        result["error"] = f"Network error: {e.reason}"
        return result
    except Exception as e:
        result["error"] = str(e)
        return result

    # If we have credentials, fetch the game title too
    if ra_username and ra_api_key and result["game_id"]:
        try:
            url2 = RA_GAMEINFO_URL.format(
                user=ra_username, key=ra_api_key, gid=result["game_id"]
            )
            req2 = Request(url2, headers={"User-Agent": "CHD-Converter/1.1.0"})
            with urlopen(req2, timeout=timeout) as r2:
                data2 = json.loads(r2.read())
            result["game_title"] = data2.get("Title") or data2.get("GameTitle")
        except Exception:
            pass  # title lookup is best-effort

    return result


# ── Main entry point ──────────────────────────────────────────────

def compute_ra_hash(chd_path, log_fn=None, progress_fn=None):
    """
    Compute the RetroAchievements PS2 hash for a CHD file.

    Returns:
        (md5_hash: str|None, exe_name: str|None, error: str|None)
    """
    if not os.path.exists(chd_path):
        return None, None, "File not found"

    tmp_dir = tempfile.mkdtemp(prefix="ra_")
    try:
        if progress_fn: progress_fn(5)

        # ── Extract CHD → BIN/CUE ──────────────────────────────
        cue_path = os.path.join(tmp_dir, "disc.cue")
        bin_path = os.path.join(tmp_dir, "disc.bin")

        if log_fn: log_fn(f"[RA] Extracting {os.path.basename(chd_path)}…")
        proc = subprocess.run(
            ["chdman", "extractcd", "-i", chd_path,
             "-o", cue_path, "-ob", bin_path, "-f"],
            capture_output=True, text=True, timeout=600
        )
        if proc.returncode != 0:
            return None, None, f"chdman failed (exit {proc.returncode})"

        if progress_fn: progress_fn(40)

        # ── Find data track BIN ────────────────────────────────
        data_bin = _first_data_track_bin(cue_path) if os.path.exists(cue_path) else bin_path
        if not data_bin or not os.path.exists(data_bin):
            return None, None, "Data track BIN not found after extraction"

        # ── Detect sector layout ───────────────────────────────
        with open(data_bin, 'rb') as f:
            user_offset = _detect_user_offset(f)
            if user_offset is None:
                return None, None, "Not a valid ISO9660 disc image"

            if progress_fn: progress_fn(50)

            # ── Read SYSTEM.CNF ────────────────────────────────
            lba, size = _find_in_root(f, user_offset, "SYSTEM.CNF")
            if lba is None:
                return None, None, "SYSTEM.CNF not found — may not be a PS2 disc"

            cnf_data  = _read_file_from_iso(f, lba, size, user_offset)
            exe_name  = _parse_system_cnf(cnf_data)
            if not exe_name:
                return None, None, f"Could not parse executable from SYSTEM.CNF: {cnf_data[:120]!r}"

            if log_fn: log_fn(f"[RA] Boot executable: {exe_name}")
            if progress_fn: progress_fn(60)

            # ── Find and read the executable ───────────────────
            exe_lba, exe_size = _find_in_root(f, user_offset, exe_name)
            if exe_lba is None:
                # Some discs store the exe in a subdirectory — try one level deep
                pvd = _read_sector(f, 16, user_offset)
                root_lba  = struct.unpack_from('<I', pvd, 156 + 2)[0]
                root_size = struct.unpack_from('<I', pvd, 156 + 10)[0]
                for name, flags, dlba, dsize in _iter_dir_entries(f, root_lba, root_size, user_offset):
                    if flags & 0x02 and name not in ('.', '..', '\x00', '\x01'):
                        exe_lba, exe_size = _find_in_root.__wrapped__ if hasattr(_find_in_root, '__wrapped__') else (None, None)
                        # simpler: search this subdir
                        for n2, fl2, l2, s2 in _iter_dir_entries(f, dlba, dsize, user_offset):
                            if n2 == exe_name:
                                exe_lba, exe_size = l2, s2
                                break
                    if exe_lba:
                        break

            if exe_lba is None:
                return None, None, f"Executable '{exe_name}' not found on disc"

            if progress_fn: progress_fn(70)

            exe_data = _read_file_from_iso(f, exe_lba, exe_size, user_offset)

        if progress_fn: progress_fn(90)

        # ── Compute MD5 ────────────────────────────────────────
        md5 = hashlib.md5(exe_data).hexdigest()
        if log_fn: log_fn(f"[RA] Hash: {md5}")
        if progress_fn: progress_fn(100)

        return md5, exe_name, None

    except subprocess.TimeoutExpired:
        return None, None, "Extraction timed out"
    except Exception as e:
        logger.exception(f"RA hash error for {chd_path}")
        return None, None, str(e)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

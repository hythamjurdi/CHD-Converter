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

def _extract_sectors(chd_path, start_lba, count, tmp_dir):
    """
    Extract a specific range of raw sectors from a CHD using chdman extractraw.
    Returns path to the extracted bin, or raises on failure.
    We read 2352-byte raw sectors.
    """
    out_bin = os.path.join(tmp_dir, f"sectors_{start_lba}_{count}.bin")
    # chdman extractraw extracts the full raw track; use -s/-c for sector offset/count
    # Fall back to full extract if range params not supported (older chdman)
    cmd_range = [
        "chdman", "extractraw",
        "-i", chd_path,
        "-o", out_bin, "-f",
        "-s", str(start_lba),
        "-c", str(count),
    ]
    cmd_full = [
        "chdman", "extractraw",
        "-i", chd_path,
        "-o", out_bin, "-f",
    ]
    # Try range extraction first (fast path ~KB not GB)
    proc = subprocess.run(cmd_range, capture_output=True, text=True, timeout=60)
    if proc.returncode == 0 and os.path.exists(out_bin) and os.path.getsize(out_bin) > 0:
        return out_bin, True  # (path, is_partial)
    # chdman doesn't support -s/-c range — fall back to full extraction
    out_full = os.path.join(tmp_dir, "full.bin")
    proc2 = subprocess.run(
        ["chdman", "extractraw", "-i", chd_path, "-o", out_full, "-f"],
        capture_output=True, text=True, timeout=300
    )
    if proc2.returncode != 0:
        # Last resort: try extractcd
        cue_path = os.path.join(tmp_dir, "disc.cue")
        bin_path = os.path.join(tmp_dir, "disc.bin")
        proc3 = subprocess.run(
            ["chdman", "extractcd", "-i", chd_path, "-o", cue_path, "-ob", bin_path, "-f"],
            capture_output=True, text=True, timeout=300
        )
        if proc3.returncode != 0:
            raise RuntimeError(f"chdman failed: {proc3.stderr[-300:]}")
        # Find the data bin from the cue
        data_bin = _first_data_track_bin(cue_path) if os.path.exists(cue_path) else bin_path
        if data_bin and os.path.exists(data_bin):
            return data_bin, False
        raise RuntimeError("No data track found after extractcd")
    return out_full, False


def compute_ra_hash(chd_path, log_fn=None, progress_fn=None):
    """
    Compute the RetroAchievements PS2 hash for a CHD file.

    Fast path: extracts only the sectors we need (~100 sectors = ~230 KB)
    rather than the full disc (1-4 GB).

    Returns:
        (md5_hash: str|None, exe_name: str|None, error: str|None)
    """
    if not os.path.exists(chd_path):
        return None, None, "File not found"

    tmp_dir = tempfile.mkdtemp(prefix="ra_")
    try:
        if progress_fn: progress_fn(5)
        if log_fn: log_fn(f"[RA] Reading sectors from {os.path.basename(chd_path)}…")

        # Step 1: Extract just LBA 16–80 (PVD + a bit of root dir) — ~65 sectors = 150 KB
        try:
            bin_path, is_partial = _extract_sectors(chd_path, 0, 80, tmp_dir)
        except Exception as e:
            return None, None, f"Extraction failed: {e}"

        if progress_fn: progress_fn(30)

        # Step 2: Detect sector layout
        with open(bin_path, 'rb') as f:
            user_offset = _detect_user_offset(f)
            if user_offset is None:
                # Might be Mode1 raw or unusual layout — try full extract
                if is_partial:
                    if log_fn: log_fn(f"[RA] Partial read failed ISO detect, trying full extract…")
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                    tmp_dir = tempfile.mkdtemp(prefix="ra_")
                    try:
                        bin_path, _ = _extract_sectors(chd_path, 0, 999999, tmp_dir)
                    except Exception as e:
                        return None, None, f"Full extraction failed: {e}"
                    with open(bin_path, 'rb') as f2:
                        user_offset = _detect_user_offset(f2)
                    if user_offset is None:
                        return None, None, "Not a valid ISO9660 disc image"
                    f = open(bin_path, 'rb')
                else:
                    return None, None, "Not a valid ISO9660 disc image"
            else:
                f.seek(0)  # keep f open for reads below

            # Step 3: Read PVD to find root dir location
            try:
                pvd = _read_sector(f, 16, user_offset)
                root_lba  = struct.unpack_from('<I', pvd, 156 + 2)[0]
                root_size = struct.unpack_from('<I', pvd, 156 + 10)[0]
            except Exception as e:
                return None, None, f"PVD read failed: {e}"

            if progress_fn: progress_fn(40)

            # If partial extract doesn't cover root dir, re-extract with more sectors
            root_end_lba = root_lba + (root_size + 2047) // 2048 + 10
            f_stat = os.path.getsize(bin_path)
            available_lbas = f_stat // SECTOR_SIZE

            if is_partial and root_end_lba > available_lbas:
                if log_fn: log_fn(f"[RA] Root dir at LBA {root_lba}, extending read…")
                f.close()
                shutil.rmtree(tmp_dir, ignore_errors=True)
                tmp_dir = tempfile.mkdtemp(prefix="ra_")
                try:
                    bin_path, is_partial = _extract_sectors(chd_path, 0, root_end_lba + 50, tmp_dir)
                except Exception:
                    try:
                        bin_path, is_partial = _extract_sectors(chd_path, 0, 999999, tmp_dir)
                    except Exception as e:
                        return None, None, f"Extended extraction failed: {e}"
                f = open(bin_path, 'rb')
                pvd = _read_sector(f, 16, user_offset)
                root_lba  = struct.unpack_from('<I', pvd, 156 + 2)[0]
                root_size = struct.unpack_from('<I', pvd, 156 + 10)[0]

            if progress_fn: progress_fn(50)

            # Step 4: Find SYSTEM.CNF in root dir
            try:
                lba, size = _find_in_root(f, user_offset, "SYSTEM.CNF")
            except Exception as e:
                return None, None, f"Root dir read failed: {e}"

            if lba is None:
                return None, None, "SYSTEM.CNF not found — may not be a PS2 disc"

            # Check if we need more sectors to read SYSTEM.CNF
            if is_partial and lba + (size + 2047)//2048 > available_lbas:
                f.close()
                shutil.rmtree(tmp_dir, ignore_errors=True)
                tmp_dir = tempfile.mkdtemp(prefix="ra_")
                bin_path, is_partial = _extract_sectors(chd_path, 0, 999999, tmp_dir)
                f = open(bin_path, 'rb')

            cnf_data = _read_file_from_iso(f, lba, size, user_offset)
            exe_name = _parse_system_cnf(cnf_data)
            if not exe_name:
                return None, None, f"Could not parse executable from SYSTEM.CNF: {cnf_data[:120]!r}"

            if log_fn: log_fn(f"[RA] Boot executable: {exe_name}")
            if progress_fn: progress_fn(60)

            # Step 5: Find executable
            exe_lba, exe_size = _find_in_root(f, user_offset, exe_name)
            if exe_lba is None:
                # Try subdirectories
                for name, flags, dlba, dsize in _iter_dir_entries(f, root_lba, root_size, user_offset):
                    if flags & 0x02 and name not in ('.', '..', '\x00', '\x01'):
                        for n2, fl2, l2, s2 in _iter_dir_entries(f, dlba, dsize, user_offset):
                            if n2 == exe_name:
                                exe_lba, exe_size = l2, s2
                                break
                    if exe_lba:
                        break

            if exe_lba is None:
                return None, None, f"Executable '{exe_name}' not found on disc"

            # Check coverage for exe
            if is_partial and exe_lba + (exe_size + 2047)//2048 > os.path.getsize(bin_path)//SECTOR_SIZE:
                if log_fn: log_fn(f"[RA] Exe at LBA {exe_lba}, extending read…")
                f.close()
                shutil.rmtree(tmp_dir, ignore_errors=True)
                tmp_dir = tempfile.mkdtemp(prefix="ra_")
                exe_end = exe_lba + (exe_size + 2047)//2048 + 5
                bin_path, _ = _extract_sectors(chd_path, 0, exe_end, tmp_dir)
                f = open(bin_path, 'rb')

            if progress_fn: progress_fn(75)
            exe_data = _read_file_from_iso(f, exe_lba, exe_size, user_offset)
            f.close()

        if progress_fn: progress_fn(90)

        # Step 6: MD5
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
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass

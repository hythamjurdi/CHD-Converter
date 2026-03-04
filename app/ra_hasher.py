"""
RetroAchievements PS2 hash computation.

Algorithm (from rcheevos rc_hash_ps2):
  1. Extract disc sectors via chdman
  2. Scan for ISO9660 PVD (handles any sector format + pregap)
  3. Read SYSTEM.CNF -> boot executable path
  4. Read boot executable bytes
  5. MD5 hash

Key insight: we scan the binary for the CD001 magic rather than assuming
a fixed offset, making it robust to any sector size / pregap combination.
"""

import os, re, hashlib, struct, subprocess, tempfile, shutil, logging
from urllib.request import urlopen, Request
from urllib.error import URLError
import json

logger = logging.getLogger(__name__)

USER_DATA_SIZE = 2048

# All known CD/DVD sector configurations (size, user-data-offset)
SECTOR_CONFIGS = [
    (2048,  0),   # cooked / DVD / extractraw common output
    (2352, 24),   # raw Mode2 XA (PS2 CD)
    (2352, 16),   # raw Mode1
    (2336, 16),   # raw Mode2 (non-XA)
]


# ── PVD scanner ───────────────────────────────────────────────────

def _scan_for_pvd(f):
    """
    Scan a binary file for the ISO9660 PVD magic b'\\x01CD001'.
    Try every known sector config at LBA 16, with pregaps 0–300.
    Returns (sector_size, user_offset, pregap) or raises ValueError.
    """
    # Fast path: try common configurations first
    for pregap in (0, 150, 75, 16, 2):
        for sec_size, usr_off in SECTOR_CONFIGS:
            pos = (pregap + 16) * sec_size + usr_off
            try:
                f.seek(pos)
                pvd = f.read(8)
                if len(pvd) >= 6 and pvd[1:6] == b'CD001':
                    return sec_size, usr_off, pregap
            except Exception:
                continue

    # Slow path: scan the file for the magic bytes directly
    # Find all occurrences of b'\x01CD001', then work out sec_size/pregap
    f.seek(0)
    data = f.read(min(20 * 1024 * 1024, 50 * 1024 * 1024))  # read up to 50 MB
    magic = b'\x01CD001'
    pos = 0
    while True:
        idx = data.find(magic, pos)
        if idx == -1:
            break
        # idx is the start of the PVD user data
        # Try each sector config to see if idx is consistent
        for sec_size, usr_off in SECTOR_CONFIGS:
            if (idx - usr_off) % sec_size == 0:
                lba = (idx - usr_off) // sec_size
                pregap = lba - 16
                if 0 <= pregap <= 300:
                    return sec_size, usr_off, pregap
        pos = idx + 1

    raise ValueError("Not a valid ISO9660 disc image (PVD not found)")


# ── Sector reader ─────────────────────────────────────────────────

def _read_sector(f, lba, sec_size, usr_off, pregap=0):
    f.seek((pregap + lba) * sec_size + usr_off)
    return f.read(USER_DATA_SIZE)


def _iter_dir_entries(f, dir_lba, dir_size, sec_size, usr_off, pregap):
    data = b''
    for i in range((dir_size + 2047) // 2048):
        data += _read_sector(f, dir_lba + i, sec_size, usr_off, pregap)
    offset = 0
    while offset < min(dir_size, len(data)):
        rec_len = data[offset]
        if rec_len == 0:
            offset = ((offset // 2048) + 1) * 2048
            continue
        if offset + rec_len > len(data):
            break
        name_len  = data[offset + 32]
        name_raw  = data[offset + 33: offset + 33 + name_len]
        flags     = data[offset + 25]
        file_lba  = struct.unpack_from('<I', data, offset + 2)[0]
        file_size = struct.unpack_from('<I', data, offset + 10)[0]
        try:
            name = name_raw.decode('ascii', errors='replace').split(';')[0].upper()
        except Exception:
            name = ''
        yield name, flags, file_lba, file_size
        offset += rec_len


def _read_file_from_iso(f, file_lba, file_size, sec_size, usr_off, pregap):
    data = b''
    for i in range((file_size + 2047) // 2048):
        data += _read_sector(f, file_lba + i, sec_size, usr_off, pregap)
    return data[:file_size]


def _find_in_dir(f, dir_lba, dir_size, sec_size, usr_off, pregap, target_name):
    target = target_name.upper()
    for name, flags, lba, size in _iter_dir_entries(f, dir_lba, dir_size, sec_size, usr_off, pregap):
        if name == target:
            return lba, size
    return None, None


# ── SYSTEM.CNF parser ─────────────────────────────────────────────

def _parse_system_cnf(data):
    try:
        text = data.decode('ascii', errors='replace')
    except Exception:
        return None
    for line in text.splitlines():
        if 'BOOT2' in line.upper():
            match = re.search(
                r'(?:cdrom\d*[:\\/]+)?([A-Z]{2,6}[_-]\d{3}[\._]\d{2})',
                line, re.IGNORECASE
            )
            if match:
                return match.group(1).upper()
    return None


# ── CUE parser ────────────────────────────────────────────────────

def _first_data_track_bin(cue_path):
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
                    if 'MODE' in line.upper() or 'AUDIO' not in line.upper():
                        if os.path.exists(current_file):
                            return current_file
    except Exception:
        pass
    for f in os.listdir(cue_dir):
        if f.lower().endswith('.bin'):
            return os.path.join(cue_dir, f)
    return None


# ── RA API ────────────────────────────────────────────────────────

RA_GAMEID_URL   = "https://retroachievements.org/dorequest.php?r=gameid&m={hash}"
RA_GAMEINFO_URL = "https://retroachievements.org/API/API_GetGame.php?z={user}&y={key}&i={gid}"

def lookup_ra_hash(md5_hash, ra_username=None, ra_api_key=None, timeout=8):
    result = {"found": False, "game_id": None, "game_title": None, "error": None}
    try:
        url = RA_GAMEID_URL.format(hash=md5_hash)
        req = Request(url, headers={"User-Agent": "CHD-Converter/1.1.2"})
        with urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read())
        if data.get("Success") and data.get("GameID", 0) > 0:
            result["found"]   = True
            result["game_id"] = data["GameID"]
        else:
            return result
    except URLError as e:
        result["error"] = "Network error: %s" % e.reason
        return result
    except Exception as e:
        result["error"] = str(e)
        return result

    if ra_username and ra_api_key and result["game_id"]:
        try:
            url2 = RA_GAMEINFO_URL.format(
                user=ra_username, key=ra_api_key, gid=result["game_id"]
            )
            req2 = Request(url2, headers={"User-Agent": "CHD-Converter/1.1.2"})
            with urlopen(req2, timeout=timeout) as r2:
                data2 = json.loads(r2.read())
            result["game_title"] = data2.get("Title") or data2.get("GameTitle")
        except Exception:
            pass
    return result


# ── Main entry point ──────────────────────────────────────────────

def compute_ra_hash(chd_path, log_fn=None, progress_fn=None):
    """
    Compute the RA PS2 hash for a CHD file.
    Returns (md5_hash, exe_name, error).
    """
    if not os.path.exists(chd_path):
        return None, None, "File not found"

    tmp_dir = tempfile.mkdtemp(prefix="ra_")
    try:
        fname = os.path.basename(chd_path)
        if log_fn: log_fn("[RA] Extracting %s…" % fname)
        if progress_fn: progress_fn(10)

        # Try extractraw first (fast, outputs native sectors)
        bin_path = os.path.join(tmp_dir, "track.bin")
        proc = subprocess.run(
            ["chdman", "extractraw", "-i", chd_path, "-o", bin_path, "-f"],
            capture_output=True, text=True, timeout=120
        )

        use_bin = bin_path
        if proc.returncode != 0 or not os.path.exists(bin_path) or os.path.getsize(bin_path) < 65536:
            # Fall back to extractcd
            if log_fn: log_fn("[RA] extractraw failed, trying extractcd…")
            cue_path = os.path.join(tmp_dir, "disc.cue")
            cd_bin   = os.path.join(tmp_dir, "disc.bin")
            proc2 = subprocess.run(
                ["chdman", "extractcd", "-i", chd_path,
                 "-o", cue_path, "-ob", cd_bin, "-f"],
                capture_output=True, text=True, timeout=300
            )
            if proc2.returncode != 0:
                return None, None, "chdman extraction failed: %s" % proc2.stderr[-200:]
            use_bin = _first_data_track_bin(cue_path) if os.path.exists(cue_path) else cd_bin
            if not use_bin or not os.path.exists(use_bin):
                return None, None, "No data track found after extractcd"

        if progress_fn: progress_fn(50)

        with open(use_bin, 'rb') as f:
            # Scan-based detection — works for any sector format and pregap
            try:
                sec_size, usr_off, pregap = _scan_for_pvd(f)
            except ValueError as e:
                return None, None, str(e)

            if log_fn: log_fn("[RA] Format: %dB sectors, pregap=%d" % (sec_size, pregap))
            if progress_fn: progress_fn(55)

            pvd       = _read_sector(f, 16, sec_size, usr_off, pregap)
            root_lba  = struct.unpack_from('<I', pvd, 156 + 2)[0]
            root_size = struct.unpack_from('<I', pvd, 156 + 10)[0]

            if progress_fn: progress_fn(60)

            cnf_lba, cnf_size = _find_in_dir(
                f, root_lba, root_size, sec_size, usr_off, pregap, "SYSTEM.CNF")
            if cnf_lba is None:
                return None, None, "SYSTEM.CNF not found — may not be a PS2 disc"

            cnf_data = _read_file_from_iso(f, cnf_lba, cnf_size, sec_size, usr_off, pregap)
            exe_name = _parse_system_cnf(cnf_data)
            if not exe_name:
                return None, None, "Could not parse boot exe from SYSTEM.CNF: %r" % cnf_data[:80]

            if log_fn: log_fn("[RA] Boot exe: %s" % exe_name)
            if progress_fn: progress_fn(70)

            exe_lba, exe_size = _find_in_dir(
                f, root_lba, root_size, sec_size, usr_off, pregap, exe_name)

            if exe_lba is None:
                # Search one level of subdirectories
                for name, flags, dlba, dsize in _iter_dir_entries(
                        f, root_lba, root_size, sec_size, usr_off, pregap):
                    if flags & 0x02 and name not in ('.', '..', '\x00', '\x01'):
                        exe_lba, exe_size = _find_in_dir(
                            f, dlba, dsize, sec_size, usr_off, pregap, exe_name)
                        if exe_lba:
                            break

            if exe_lba is None:
                return None, None, "Executable '%s' not found on disc" % exe_name

            if progress_fn: progress_fn(85)
            exe_data = _read_file_from_iso(f, exe_lba, exe_size, sec_size, usr_off, pregap)

        if progress_fn: progress_fn(95)
        md5 = hashlib.md5(exe_data).hexdigest()
        if log_fn: log_fn("[RA] Hash: %s" % md5)
        if progress_fn: progress_fn(100)

        return md5, exe_name, None

    except subprocess.TimeoutExpired:
        return None, None, "Extraction timed out"
    except Exception as e:
        logger.exception("RA hash error for %s" % chd_path)
        return None, None, str(e)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

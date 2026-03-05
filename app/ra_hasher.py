"""
RetroAchievements PS2 hash computation.

Algorithm (rcheevos rc_hash_ps2):
  1. Parse CHD metadata with `chdman info` to get exact track type/sector size
  2. Extract with `chdman extractcd` -> BIN/CUE (always correct)
  3. Parse the BIN with the known sector geometry
  4. Read SYSTEM.CNF -> boot executable name
  5. Read boot executable bytes -> MD5
"""

import os, re, hashlib, struct, subprocess, tempfile, shutil, logging
from urllib.request import urlopen, Request
from urllib.error import URLError
import json

logger = logging.getLogger(__name__)

USER_DATA_SIZE = 2048


# ── chdman info parser ────────────────────────────────────────────

def _get_chd_track_info(chd_path):
    """
    Run `chdman info` and return list of track dicts:
      [{type, subtype, frames, pregap, ...}, ...]
    Track types from chdman: MODE1, MODE1_RAW, MODE2, MODE2_RAW, AUDIO, etc.
    """
    try:
        proc = subprocess.run(
            ["chdman", "info", "-i", chd_path],
            capture_output=True, text=True, timeout=15
        )
        tracks = []
        for line in proc.stdout.splitlines():
            # e.g. "  TRACK:1 TYPE:MODE2_RAW SUBTYPE:NONE FRAMES:284672 PREGAP:150 ..."
            if "TRACK:" in line and "TYPE:" in line:
                m = re.search(r'TRACK:(\d+).*?TYPE:(\S+).*?FRAMES:(\d+)', line)
                pregap_m = re.search(r'PREGAP:(\d+)', line)
                if m:
                    tracks.append({
                        "track":  int(m.group(1)),
                        "type":   m.group(2),
                        "frames": int(m.group(3)),
                        "pregap": int(pregap_m.group(1)) if pregap_m else 0,
                    })
        return tracks
    except Exception:
        return []


def _track_geometry(track_type):
    """
    Return (sector_size, user_offset) for a given chdman track type.
    _RAW variants include the full 2352-byte raw sector (sync + headers + user data).
    Non-RAW variants from chdman extractraw give just user data (2048 bytes).
    """
    raw_types = {
        "MODE1_RAW": (2352, 16),   # sync(12) + header(4)
        "MODE2_RAW": (2352, 24),   # sync(12) + header(4) + subheader(8)
        "AUDIO":     (2352, 0),
    }
    cooked_types = {
        "MODE1": (2048, 0),
        "MODE2": (2048, 0),
        "MODE2_FORM1": (2048, 0),
        "MODE2_FORM2": (2324, 0),
    }
    t = track_type.upper()
    if t in raw_types:
        return raw_types[t]
    if t in cooked_types:
        return cooked_types[t]
    # Unknown: assume cooked 2048
    return (2048, 0)


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


def _find_in_dir(f, dir_lba, dir_size, sec_size, usr_off, pregap, target):
    for name, flags, lba, size in _iter_dir_entries(f, dir_lba, dir_size, sec_size, usr_off, pregap):
        if name == target.upper():
            return lba, size
    return None, None


# ── SYSTEM.CNF parser ─────────────────────────────────────────────

def _parse_system_cnf(data):
    """
    Parse BOOT2 line from SYSTEM.CNF and return the boot executable filename.
    e.g. BOOT2 = cdrom0:\\SLUS_20572.02;1  ->  SLUS_20572.02

    The old regex required exactly 3 digits (d{3}) which matched zero real
    PS2 disc IDs — all have 5 digits like SLUS_20572. Fixed to capture the
    full filename after the cdrom path, then take the last path component.
    """
    try:
        text = data.decode('ascii', errors='replace')
    except Exception:
        return None
    for line in text.splitlines():
        if 'BOOT2' in line.upper():
            # Capture everything after cdrom0:\ including optional subdirs
            m = re.search(r'BOOT2\s*=\s*cdrom\d*[:\\/]+([\w\\/\.]+)', line, re.IGNORECASE)
            if m:
                raw = m.group(1).strip()
                # Take the last path component (the filename itself)
                fname = re.split(r'[\\/]', raw)[-1]
                # Strip ISO 9660 version suffix ;1
                fname = fname.split(';')[0]
                return fname.upper() if fname else None
    return None


# ── CUE parser ────────────────────────────────────────────────────

def _parse_index_time(ts):
    """Convert MM:SS:FF timecode string to frame count."""
    m = re.match(r'(\d+):(\d+):(\d+)', ts)
    if m:
        return int(m.group(1)) * 60 * 75 + int(m.group(2)) * 75 + int(m.group(3))
    return 0


def _parse_cue(cue_path):
    """Return list of track dicts with pregap correctly derived from INDEX 00/01.

    chdman extractcd uses INDEX 00/INDEX 01 to express pregap, NOT the PREGAP
    keyword. Previously only PREGAP was checked, so all extracted CHDs had
    cue_pregap=0 and sector reads were offset by 150 frames.
    """
    cue_dir = os.path.dirname(cue_path)
    tracks = []
    current_bin = None
    try:
        with open(cue_path, errors='replace') as f:
            for line in f:
                ls = line.strip()
                lu = ls.upper()
                if lu.startswith('FILE'):
                    # Handle both quoted and unquoted FILE lines
                    parts = ls.split('"')
                    if len(parts) >= 3:
                        bin_ref = parts[1]
                    else:
                        tokens = ls.split()
                        bin_ref = tokens[1] if len(tokens) >= 2 else ''
                    if bin_ref:
                        current_bin = os.path.join(cue_dir, bin_ref)
                elif lu.startswith('TRACK') and current_bin:
                    m = re.match(r'TRACK\s+(\d+)\s+(\S+)', ls, re.IGNORECASE)
                    if m:
                        tracks.append({
                            "bin":    current_bin,
                            "track":  int(m.group(1)),
                            "type":   m.group(2).upper(),
                            "pregap": 0,
                            "_idx00": None,
                            "_idx01": None,
                        })
                elif lu.startswith('PREGAP') and tracks:
                    # Explicit PREGAP keyword (some CUE tools use this)
                    m = re.match(r'PREGAP\s+(\d+):(\d+):(\d+)', ls, re.IGNORECASE)
                    if m:
                        tracks[-1]["pregap"] = _parse_index_time(m.group(0).split(None,1)[1])
                elif lu.startswith('INDEX') and tracks:
                    m = re.match(r'INDEX\s+(\d+)\s+(\S+)', ls, re.IGNORECASE)
                    if m:
                        n, ts = int(m.group(1)), m.group(2)
                        if n == 0:
                            tracks[-1]["_idx00"] = _parse_index_time(ts)
                        elif n == 1:
                            tracks[-1]["_idx01"] = _parse_index_time(ts)
    except Exception:
        pass

    # Derive pregap from INDEX 00/01 difference if PREGAP keyword wasn't present
    for t in tracks:
        if t["pregap"] == 0 and t["_idx00"] is not None and t["_idx01"] is not None:
            t["pregap"] = t["_idx01"] - t["_idx00"]
        # Clean up temp keys
        t.pop("_idx00", None)
        t.pop("_idx01", None)

    return tracks


def _first_data_track(cue_path):
    """Return (bin_path, sec_size, usr_off, pregap) for the first data track."""
    tracks = _parse_cue(cue_path)
    for t in tracks:
        tt = t["type"]
        if "MODE" in tt:
            sec_size, usr_off = _track_geometry(tt)
            # extractcd preserves the raw sector format for RAW types
            return t["bin"], sec_size, usr_off, t["pregap"]
    # Fallback: first track, assume Mode2 raw
    if tracks:
        return tracks[0]["bin"], 2352, 24, tracks[0]["pregap"]
    return None, 2352, 24, 0


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
        result["error"] = "Network error: %s" % str(e.reason)
        return result
    except Exception as e:
        result["error"] = str(e)
        return result
    if ra_username and ra_api_key and result["game_id"]:
        try:
            url2 = RA_GAMEINFO_URL.format(user=ra_username, key=ra_api_key, gid=result["game_id"])
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
    Uses chdman extractcd (reliable, format-agnostic).
    Returns (md5_hash, exe_name, error).
    """
    if not os.path.exists(chd_path):
        return None, None, "File not found"

    # Use a sibling tmp dir in the same volume as the CHD so extraction
    # doesn't write into the Docker overlay filesystem (/tmp inside container)
    chd_dir  = os.path.dirname(chd_path)
    tmp_base = chd_dir if os.access(chd_dir, os.W_OK) else None
    tmp_dir  = tempfile.mkdtemp(prefix="ra_", dir=tmp_base)
    try:
        fname = os.path.basename(chd_path)
        if log_fn: log_fn("[RA] Extracting %s…" % fname)
        if progress_fn: progress_fn(5)

        # Get track info from CHD metadata
        tracks = _get_chd_track_info(chd_path)
        data_track = next((t for t in tracks if "MODE" in t.get("type","")), None)
        if data_track and log_fn:
            log_fn("[RA] Track type: %s, pregap: %d" % (data_track["type"], data_track["pregap"]))

        # Always use extractcd — it correctly handles all PS2 disc types
        cue_path = os.path.join(tmp_dir, "disc.cue")
        bin_path = os.path.join(tmp_dir, "disc.bin")
        proc = subprocess.run(
            ["chdman", "extractcd", "-i", chd_path,
             "-o", cue_path, "-ob", bin_path, "-f"],
            capture_output=True, text=True, timeout=600
        )
        if proc.returncode != 0:
            return None, None, "chdman extractcd failed: %s" % proc.stderr[-300:]

        if progress_fn: progress_fn(60)

        # Parse CUE to find data track BIN and its geometry
        data_bin, sec_size, usr_off, cue_pregap = _first_data_track(cue_path)

        # Override with chdman info if we got it
        if data_track:
            sec_size, usr_off = _track_geometry(data_track["type"])
            # Use the pregap from metadata if CUE didn't specify one
            if cue_pregap == 0 and data_track["pregap"] > 0:
                cue_pregap = data_track["pregap"]

        if not data_bin or not os.path.exists(data_bin):
            data_bin = bin_path  # fallback to single bin

        if not os.path.exists(data_bin):
            return None, None, "Data track BIN not found"

        if log_fn: log_fn("[RA] Format: %dB sectors, usr_off=%d, pregap=%d" % (sec_size, usr_off, cue_pregap))
        if progress_fn: progress_fn(65)

        with open(data_bin, 'rb') as f:
            # Verify we can find the PVD
            pvd_test = _read_sector(f, 16, sec_size, usr_off, cue_pregap)
            if pvd_test[1:6] != b'CD001':
                # Try pregap=0 if specified pregap failed
                pvd_test2 = _read_sector(f, 16, sec_size, usr_off, 0)
                if pvd_test2[1:6] == b'CD001':
                    cue_pregap = 0
                    if log_fn: log_fn("[RA] Using pregap=0 (override)")
                else:
                    # Last resort: scan for CD001
                    f.seek(0)
                    raw = f.read(min(5 * 1024 * 1024, os.path.getsize(data_bin)))
                    idx = raw.find(b'\x01CD001')
                    if idx == -1:
                        return None, None, "Not a valid ISO9660 disc image (PVD not found after extraction)"
                    # Derive geometry from found offset
                    found = False
                    for ss, uo in [(2352,24),(2352,16),(2048,0),(2336,16)]:
                        if (idx - uo) % ss == 0:
                            lba = (idx - uo) // ss
                            pg  = lba - 16
                            if 0 <= pg <= 300:
                                sec_size, usr_off, cue_pregap = ss, uo, pg
                                found = True
                                if log_fn: log_fn("[RA] Recovered geometry: sec=%d off=%d pregap=%d" % (ss,uo,pg))
                                break
                    if not found:
                        return None, None, "Could not determine disc geometry"

            pvd       = _read_sector(f, 16, sec_size, usr_off, cue_pregap)
            root_lba  = struct.unpack_from('<I', pvd, 156 + 2)[0]
            root_size = struct.unpack_from('<I', pvd, 156 + 10)[0]

            if progress_fn: progress_fn(70)

            cnf_lba, cnf_size = _find_in_dir(f, root_lba, root_size, sec_size, usr_off, cue_pregap, "SYSTEM.CNF")
            if cnf_lba is None:
                return None, None, "SYSTEM.CNF not found — may not be a PS2 disc"

            cnf_data = _read_file_from_iso(f, cnf_lba, cnf_size, sec_size, usr_off, cue_pregap)
            exe_name = _parse_system_cnf(cnf_data)
            if not exe_name:
                return None, None, "Could not parse boot exe from SYSTEM.CNF: %r" % cnf_data[:80]

            if log_fn: log_fn("[RA] Boot exe: %s" % exe_name)
            if progress_fn: progress_fn(80)

            exe_lba, exe_size = _find_in_dir(f, root_lba, root_size, sec_size, usr_off, cue_pregap, exe_name)
            if exe_lba is None:
                for name, flags, dlba, dsize in _iter_dir_entries(f, root_lba, root_size, sec_size, usr_off, cue_pregap):
                    if flags & 0x02 and name not in ('.','..'):
                        exe_lba, exe_size = _find_in_dir(f, dlba, dsize, sec_size, usr_off, cue_pregap, exe_name)
                        if exe_lba:
                            break
            if exe_lba is None:
                return None, None, "Executable '%s' not found on disc" % exe_name

            if progress_fn: progress_fn(90)
            exe_data = _read_file_from_iso(f, exe_lba, exe_size, sec_size, usr_off, cue_pregap)

        md5 = hashlib.md5(exe_data).hexdigest()
        if log_fn: log_fn("[RA] Hash: %s" % md5)
        if progress_fn: progress_fn(100)
        return md5, exe_name, None

    except subprocess.TimeoutExpired:
        return None, None, "Extraction timed out (>10 min)"
    except Exception as e:
        logger.exception("RA hash error for %s" % chd_path)
        return None, None, str(e)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

"""
RetroAchievements PS2 hash computation.

PS2 RA hash algorithm:
  1. Extract CHD sectors (tries fast partial read, falls back to full extractcd)
  2. Auto-detect sector size (2352 raw or 2048 cooked)
  3. Parse ISO9660 to find SYSTEM.CNF → boot executable name
  4. Read executable bytes
  5. MD5 hash them

Reference: rcheevos rc_hash_ps2 implementation
"""

import os, re, hashlib, struct, subprocess, tempfile, shutil, logging
from urllib.request import urlopen, Request
from urllib.error import URLError
import json

logger = logging.getLogger(__name__)

# ── Sector geometry constants ─────────────────────────────────────
# We support three layouts:
#   raw 2352-byte sectors, Mode2/XA  (user data at offset 24)
#   raw 2352-byte sectors, Mode1     (user data at offset 16)
#   cooked 2048-byte sectors         (user data at offset 0)
SECTOR_LAYOUTS = [
    (2352, 24),   # Mode2/XA  — most PS2 discs
    (2352, 16),   # Mode1
    (2048,  0),   # cooked / extractraw output
]
MODE2_USER_SIZE = 2048


# ── Low-level sector reader ───────────────────────────────────────

def _detect_layout(f):
    """
    Detect sector size and user-data offset by looking for ISO9660 magic at LBA 16.
    Returns (sector_size, user_offset) or (None, None).
    """
    for sector_size, user_offset in SECTOR_LAYOUTS:
        try:
            f.seek(16 * sector_size + user_offset + 1)
            magic = f.read(5)
            if magic == b'CD001':
                return sector_size, user_offset
        except Exception:
            pass
    return None, None


def _read_sector(f, lba, sector_size, user_offset):
    """Read 2048 bytes of user data from a sector."""
    f.seek(lba * sector_size + user_offset)
    return f.read(MODE2_USER_SIZE)


# ── ISO9660 directory traversal ───────────────────────────────────

def _iter_dir_entries(f, dir_lba, dir_size, sector_size, user_offset):
    """Yield (name_upper, flags, file_lba, file_size) for each directory entry."""
    data = b''
    for i in range((dir_size + 2047) // 2048):
        data += _read_sector(f, dir_lba + i, sector_size, user_offset)

    offset = 0
    while offset < min(dir_size, len(data)):
        rec_len = data[offset]
        if rec_len == 0:
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
        try:
            name = name_raw.decode('ascii', errors='replace').split(';')[0].upper()
        except Exception:
            name = ''
        yield name, flags, file_lba, file_size
        offset += rec_len


def _read_file_from_iso(f, file_lba, file_size, sector_size, user_offset):
    """Read a file given its LBA and size."""
    data = b''
    for i in range((file_size + 2047) // 2048):
        data += _read_sector(f, file_lba + i, sector_size, user_offset)
    return data[:file_size]


def _find_in_root(f, sector_size, user_offset, target_name):
    """Find a file in root directory. Returns (lba, size) or (None, None)."""
    pvd = _read_sector(f, 16, sector_size, user_offset)
    root_lba  = struct.unpack_from('<I', pvd, 156 + 2)[0]
    root_size = struct.unpack_from('<I', pvd, 156 + 10)[0]
    target = target_name.upper()
    for name, flags, lba, size in _iter_dir_entries(f, root_lba, root_size, sector_size, user_offset):
        if name == target:
            return lba, size
    return None, None


# ── SYSTEM.CNF parser ─────────────────────────────────────────────

def _parse_system_cnf(data):
    """Extract boot executable name from SYSTEM.CNF. Returns e.g. 'SLUS_200.40'."""
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
    """Return path of first data track BIN from a CUE sheet."""
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
                    if 'AUDIO' not in line.upper():
                        if os.path.exists(current_file):
                            return current_file
    except Exception:
        pass
    for fn in os.listdir(cue_dir):
        if fn.lower().endswith('.bin'):
            return os.path.join(cue_dir, fn)
    return None


# ── Extraction ────────────────────────────────────────────────────

def _extract_chd(chd_path, tmp_dir, log_fn=None):
    """
    Extract a CHD to a readable BIN/CUE using chdman extractcd.
    extractraw cannot be used for CD-type CHDs — it outputs raw hunk data
    with subcode headers that break flat sector addressing.
    extractcd always produces clean 2352-byte raw sectors.
    Returns (bin_path, sector_size, user_offset) or raises RuntimeError.
    """
    cue_path = os.path.join(tmp_dir, 'disc.cue')
    bin_path = os.path.join(tmp_dir, 'disc.bin')
    proc = subprocess.run(
        ['chdman', 'extractcd', '-i', chd_path,
         '-o', cue_path, '-ob', bin_path, '-f'],
        capture_output=True, text=True, timeout=300
    )
    if proc.returncode != 0:
        raise RuntimeError(f'chdman extractcd failed: {proc.stderr[-300:].strip()}')

    data_bin = _first_data_track_bin(cue_path) if os.path.exists(cue_path) else bin_path
    if not data_bin or not os.path.exists(data_bin):
        raise RuntimeError('No data track BIN found after extractcd')

    with open(data_bin, 'rb') as f:
        ss, uo = _detect_layout(f)
    if ss is None:
        raise RuntimeError('Not a valid ISO9660 disc image')
    return data_bin, ss, uo


# ── RA API ────────────────────────────────────────────────────────

RA_GAMEID_URL   = 'https://retroachievements.org/dorequest.php?r=gameid&m={hash}'
RA_GAMEINFO_URL = 'https://retroachievements.org/API/API_GetGame.php?z={user}&y={key}&i={gid}'

def lookup_ra_hash(md5_hash, ra_username=None, ra_api_key=None, timeout=8):
    result = {'found': False, 'game_id': None, 'game_title': None, 'error': None}
    try:
        url = RA_GAMEID_URL.format(hash=md5_hash)
        req = Request(url, headers={'User-Agent': 'CHD-Converter/1.1.2'})
        with urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read())
        if data.get('Success') and data.get('GameID', 0) > 0:
            result['found']   = True
            result['game_id'] = data['GameID']
        else:
            return result
    except URLError as e:
        result['error'] = f'Network error: {e.reason}'
        return result
    except Exception as e:
        result['error'] = str(e)
        return result

    if ra_username and ra_api_key and result['game_id']:
        try:
            url2 = RA_GAMEINFO_URL.format(
                user=ra_username, key=ra_api_key, gid=result['game_id'])
            req2 = Request(url2, headers={'User-Agent': 'CHD-Converter/1.1.2'})
            with urlopen(req2, timeout=timeout) as r2:
                data2 = json.loads(r2.read())
            result['game_title'] = data2.get('Title') or data2.get('GameTitle')
        except Exception:
            pass
    return result


# ── Main entry point ──────────────────────────────────────────────

def compute_ra_hash(chd_path, log_fn=None, progress_fn=None):
    """
    Compute the RetroAchievements PS2 hash for a CHD file.
    Returns (md5_hash, exe_name, error).
    """
    if not os.path.exists(chd_path):
        return None, None, 'File not found'

    tmp_dir = tempfile.mkdtemp(prefix='ra_')
    try:
        if progress_fn: progress_fn(5)
        if log_fn: log_fn(f'[RA] Extracting {os.path.basename(chd_path)}…')

        try:
            bin_path, sector_size, user_offset = _extract_chd(chd_path, tmp_dir, log_fn)
        except RuntimeError as e:
            return None, None, str(e)

        if progress_fn: progress_fn(50)

        with open(bin_path, 'rb') as f:
            # Read SYSTEM.CNF
            lba, size = _find_in_root(f, sector_size, user_offset, 'SYSTEM.CNF')
            if lba is None:
                return None, None, 'SYSTEM.CNF not found — may not be a PS2 disc'

            cnf_data = _read_file_from_iso(f, lba, size, sector_size, user_offset)
            exe_name = _parse_system_cnf(cnf_data)
            if not exe_name:
                return None, None, f'Could not parse executable from SYSTEM.CNF: {cnf_data[:80]!r}'

            if log_fn: log_fn(f'[RA] Boot executable: {exe_name}')
            if progress_fn: progress_fn(65)

            # Find executable (root dir first, then subdirs)
            exe_lba, exe_size = _find_in_root(f, sector_size, user_offset, exe_name)
            if exe_lba is None:
                pvd = _read_sector(f, 16, sector_size, user_offset)
                root_lba  = struct.unpack_from('<I', pvd, 156 + 2)[0]
                root_size = struct.unpack_from('<I', pvd, 156 + 10)[0]
                for name, flags, dlba, dsize in _iter_dir_entries(
                        f, root_lba, root_size, sector_size, user_offset):
                    if flags & 0x02 and name not in ('.', '..'):
                        for n2, fl2, l2, s2 in _iter_dir_entries(
                                f, dlba, dsize, sector_size, user_offset):
                            if n2 == exe_name:
                                exe_lba, exe_size = l2, s2
                                break
                    if exe_lba:
                        break

            if exe_lba is None:
                return None, None, f'Executable {exe_name!r} not found on disc'

            if progress_fn: progress_fn(80)
            exe_data = _read_file_from_iso(f, exe_lba, exe_size, sector_size, user_offset)

        if progress_fn: progress_fn(95)
        md5 = hashlib.md5(exe_data).hexdigest()
        if log_fn: log_fn(f'[RA] Hash: {md5}')
        if progress_fn: progress_fn(100)
        return md5, exe_name, None

    except subprocess.TimeoutExpired:
        return None, None, 'Extraction timed out'
    except Exception as e:
        logger.exception(f'RA hash error for {chd_path}')
        return None, None, str(e)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

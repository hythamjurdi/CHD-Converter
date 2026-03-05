import os, subprocess, shutil, threading, tempfile, logging, re, hashlib

logger = logging.getLogger(__name__)

ISO_EXTENSIONS     = {".iso", ".img", ".bin", ".cue", ".mdf", ".nrg"}
ARCHIVE_EXTENSIONS = {".7z", ".zip", ".rar", ".tar", ".gz", ".tar.gz", ".tgz"}
MIN_ISO_SIZE       = 1 * 1024 * 1024
MAX_ISO_SIZE       = 9.5 * 1024 * 1024 * 1024


KNOWN_EXTS = {'.chd', '.iso', '.img', '.bin', '.cue', '.7z', '.zip',
              '.rar', '.mdf', '.nrg', '.tar', '.gz', '.tgz'}


def _normalize_name(s):
    # Strip ONLY known media/archive extensions before normalizing.
    # os.path.splitext on "Game (v1.00)" gives ("Game (v1", ".00)") which
    # is wrong — so we only strip if the extension is a known one.
    base, ext = os.path.splitext(s)
    s = base if ext.lower() in KNOWN_EXTS else s
    # Strip all parenthetical/bracket groups: "(USA)", "[!]", "(v1.03)", etc.
    s = re.sub(r'[\(\[][^\)\]]*[\)\]]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s.lower()


def build_dest_chd_set(dest_folder):
    # Walk dest_folder once and return normalized names of all CHD/7z files.
    names = set()
    try:
        for root, _, files in os.walk(dest_folder):
            for f in files:
                if f.lower().endswith(('.chd', '.7z')):
                    names.add(_normalize_name(f))
    except Exception:
        pass
    return names


def make_temp_cue(bin_path, tmp_dir):
    """
    Auto-generate a minimal CUE sheet for a bare .bin file.

    Detection strategy (reads first 2352 bytes):
      1. Check for raw-sector sync pattern at byte 0: 00 FF*10 00
         - Byte 15 = mode byte: 1 → MODE1/2352, 2 → MODE2/2352
      2. If no sync at offset 0, check offset 2336 (some discs have a 2336-byte
         user-data layout with a different pregap): same sync check
      3. Use file size to infer sector size:
         - size % 2352 == 0 → raw sectors, default MODE2/2352
         - size % 2048 == 0 → cooked sectors, MODE1/2048
      4. Final fallback: MODE2/2352 (most common for PS2)
    """
    mode = 'MODE2/2352'
    try:
        file_size = os.path.getsize(bin_path)
        with open(bin_path, 'rb') as f:
            raw = f.read(2352)

        sync = b'\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00'

        # Check standard sector-0 sync
        if len(raw) >= 16 and raw[:12] == sync:
            mode_byte = raw[15]
            mode = 'MODE1/2352' if mode_byte == 1 else 'MODE2/2352'
        # Check sync at offset 16 (Mode2/Form2 XA header skip)
        elif len(raw) >= 28 and raw[16:28] == sync:
            mode = 'MODE2/2352'
        # Fall back to file-size inference
        elif file_size > 0:
            if file_size % 2352 == 0:
                mode = 'MODE2/2352'   # raw sectors — default PS2
            elif file_size % 2048 == 0:
                mode = 'MODE1/2048'   # cooked/ISO-style
            # else keep MODE2/2352 default
    except Exception:
        pass

    cue_name = os.path.splitext(os.path.basename(bin_path))[0] + '.cue'
    cue_path = os.path.join(tmp_dir, cue_name)
    with open(cue_path, 'w') as f:
        f.write('FILE "%s" BINARY\n' % os.path.basename(bin_path))
        f.write('  TRACK 01 %s\n' % mode)
        f.write('    INDEX 01 00:00:00\n')
    return cue_path


# ── Detection helpers ────────────────────────────────────────────

def detect_chd_type(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext in (".iso", ".img", ".bin", ".cue", ".mdf", ".nrg"):
        return "cd"
    return "cd"

def find_cue_files(folder):
    cues = []
    for root, _, files in os.walk(folder):
        for f in files:
            if f.lower().endswith(".cue"):
                cues.append(os.path.join(root, f))
    return cues


def _bin_has_cue(bin_path):
    """Return the .cue path if a matching .cue exists next to this .bin, else None."""
    cue = os.path.splitext(bin_path)[0] + ".cue"
    return cue if os.path.exists(cue) else None


def find_iso_files(folder):
    """
    Returns (convertible, orphan_bins).
    convertible = list of paths safe to pass to chdman.
    orphan_bins = list of .bin paths that have no .cue — callers should log
                  a warning for each rather than passing them to chdman
                  (which would hang at "Input tracks: 0").
    """
    convertible = []
    orphan_bins = []
    cue_bins    = set()

    # First pass: record which .bin files are claimed by a .cue
    for root, _, files in os.walk(folder):
        for f in files:
            if f.lower().endswith(".cue"):
                try:
                    with open(os.path.join(root, f)) as cf:
                        for line in cf:
                            if "FILE" in line.upper():
                                parts = line.strip().split('"')
                                if len(parts) >= 2:
                                    cue_bins.add(os.path.normpath(
                                        os.path.join(root, parts[1])))
                except:
                    pass

    for root, _, files in os.walk(folder):
        for f in files:
            full = os.path.normpath(os.path.join(root, f))
            ext  = os.path.splitext(f)[1].lower()
            if ext in (".iso", ".img", ".mdf", ".nrg"):
                convertible.append(full)
            elif ext == ".bin" and full not in cue_bins:
                orphan_bins.append(full)
            # .bin in cue_bins → handled by its .cue, skip

    return convertible, orphan_bins


def peek_archive_iso_names(archive_path):
    """
    List ISO/CUE basenames inside an archive WITHOUT extracting (~0.1-0.3s).
    Uses 7z's index read — no data is decompressed regardless of archive size.
    Returns list of basenames (no extension). Returns [] on any failure.
    """
    try:
        result = subprocess.run(
            ["7z", "l", "-slt", "-ba", archive_path],
            capture_output=True, text=True, timeout=15
        )
        names = []
        ISO_EXTS = {".iso", ".img", ".cue", ".mdf", ".nrg"}
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.lower().startswith("path ="):
                fname = line.split("=", 1)[1].strip()
                base, ext = os.path.splitext(os.path.basename(fname))
                if ext.lower() in ISO_EXTS:
                    names.append(base)
        return list(dict.fromkeys(names))  # dedupe, preserve order
    except Exception:
        return []


def check_bad_dump(iso_path, mode="size", log_fn=None):
    try:
        size = os.path.getsize(iso_path)
    except Exception as e:
        return True, f"Cannot read file: {e}"
    gb = size / (1024**3)
    mb = size / (1024**2)
    if size < MIN_ISO_SIZE:
        return True, f"File too small ({mb:.1f} MB) — likely corrupt or incomplete"
    if size > MAX_ISO_SIZE:
        return True, f"File too large ({gb:.2f} GB) — exceeds dual-layer DVD capacity"
    if mode == "checksum":
        if log_fn: log_fn(f"Computing MD5 of {os.path.basename(iso_path)} ({gb:.2f} GB)…")
        md5 = hashlib.md5()
        try:
            with open(iso_path, 'rb') as f:
                while chunk := f.read(8 * 1024 * 1024):
                    md5.update(chunk)
            if log_fn: log_fn(f"MD5: {md5.hexdigest().upper()} (verify at redump.org)")
        except Exception as e:
            if log_fn: log_fn(f"MD5 failed: {e}", "warn")
    return False, None


# ── Core subprocess runner ───────────────────────────────────────

def _run_with_progress(cmd, log_fn=None, progress_fn=None):
    """
    Run a subprocess, parse percentage progress lines (handles \\r and \\n).
    Logs each unique 5% step. Calls progress_fn on every change.
    """
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
    buf      = b""
    last_pct = -1   # tracks last reported value so we never go backwards / spam

    while True:
        ch = proc.stdout.read(1)
        if not ch:
            break
        if ch in (b'\r', b'\n'):
            line = buf.decode("utf-8", errors="replace").strip()
            buf  = b""
            if not line:
                continue
            m = re.search(r'(\d+)%', line)
            if m:
                pct = int(m.group(1))
                if pct > last_pct:          # only move forward, never repeat
                    last_pct = pct
                    if log_fn and pct % 5 == 0:   # log every 5%
                        log_fn(f"Progress: {pct}%")
                    if progress_fn:
                        progress_fn(pct)
            else:
                # Non-progress line (filenames, chdman info, errors, etc.)
                if log_fn: log_fn(line)
        else:
            buf += ch

    if buf:  # flush any partial line without a terminator
        line = buf.decode("utf-8", errors="replace").strip()
        if line and log_fn: log_fn(line)

    try:
        proc.wait(timeout=1800)  # 30-minute hard limit — kills hung chdman
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        if log_fn: log_fn("ERROR: Process timed out after 30 minutes and was killed.", "error")
        return -1
    return proc.returncode


def _run_with_timer_progress(cmd, log_fn=None, progress_fn=None, tick_interval=1.5, max_pct=94):
    """
    Run a subprocess while a background timer smoothly increments progress
    using a logarithmic curve (fast at start, slows near max_pct).
    Used for 7z/unrar which don't emit reliable progress when piped.
    """
    import time, math

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
    stop_timer = threading.Event()
    current_pct = [0]

    def timer_thread():
        tick = 0
        while not stop_timer.is_set():
            time.sleep(tick_interval)
            tick += 1
            new_pct = int(max_pct * (1 - 1 / math.log(tick + math.e)))
            if new_pct > current_pct[0]:
                current_pct[0] = new_pct
                if progress_fn:
                    progress_fn(new_pct)

    t = threading.Thread(target=timer_thread, daemon=True)
    t.start()

    buf = b""
    while True:
        ch = proc.stdout.read(1)
        if not ch:
            break
        if ch in (b'\r', b'\n'):
            line = buf.decode("utf-8", errors="replace").strip()
            buf = b""
            if line:
                # Skip bare percentage lines (e.g. "  0%") — handled by timer
                if not re.match(r'^\s*\d+%', line):  # filter bare progress lines like '0%', '45% - file.bin'
                    if log_fn: log_fn(line)
        else:
            buf += ch

    if buf:
        line = buf.decode("utf-8", errors="replace").strip()
        if line and not re.match(r'^\s*\d+%', line):
            if log_fn: log_fn(line)

    stop_timer.set()
    t.join(timeout=2)
    proc.wait()
    return proc.returncode


# ── Archive helpers ──────────────────────────────────────────────

def extract_archive(archive_path, dest_folder, log_fn=None, progress_fn=None):
    ext = archive_path.lower()
    os.makedirs(dest_folder, exist_ok=True)

    if not os.access(archive_path, os.R_OK):
        raise RuntimeError(
            f"Permission denied: '{os.path.basename(archive_path)}'. "
            f"Run: chmod 644 \"{archive_path}\""
        )

    if ext.endswith(".7z"):
        cmd = ["7z", "x", archive_path, f"-o{dest_folder}", "-y", "-bsp1"]
    elif ext.endswith(".zip"):
        cmd = ["unzip", "-o", archive_path, "-d", dest_folder]
    elif ext.endswith(".rar"):
        cmd = ["unrar", "x", "-o+", archive_path, dest_folder]
    elif ext.endswith((".tar.gz", ".tgz")):
        cmd = ["tar", "xzf", archive_path, "-C", dest_folder]
    elif ext.endswith(".tar"):
        cmd = ["tar", "xf",  archive_path, "-C", dest_folder]
    else:
        cmd = ["7z", "x", archive_path, f"-o{dest_folder}", "-y", "-bsp1"]

    if log_fn: log_fn(f"Extracting: {os.path.basename(archive_path)}")
    # 7z/unrar don't reliably emit progress when piped — use timer-based progress
    rc = _run_with_timer_progress(cmd, log_fn=log_fn, progress_fn=progress_fn)
    if progress_fn: progress_fn(100)   # snap to 100% on completion
    if rc != 0:
        raise RuntimeError(f"Extraction failed (exit {rc})")
    return dest_folder


def rezip_to_7z(chd_path, output_7z, compression=5, log_fn=None, progress_fn=None):
    cmd = ["7z", "a", f"-mx={compression}", "-bsp1", output_7z, chd_path]
    if log_fn: log_fn(f"Creating archive: {os.path.basename(output_7z)} (level {compression})")
    rc = _run_with_timer_progress(cmd, log_fn=log_fn, progress_fn=progress_fn)
    if rc != 0:
        raise RuntimeError(f"7z compression failed (exit {rc})")
    if os.path.exists(chd_path):
        os.remove(chd_path)
        if log_fn: log_fn(f"Removed CHD (now inside archive): {os.path.basename(chd_path)}", "warn")
    return output_7z


# ── chdman wrapper ───────────────────────────────────────────────

def run_chdman(input_file, output_file, chd_type, log_fn=None, progress_fn=None):
    # Auto-generate a temp CUE if only a bare .bin is given — chdman needs it
    _tmp_cue_dir = None
    try:
        if input_file.lower().endswith(".bin"):
            cue = os.path.splitext(input_file)[0] + ".cue"
            if not os.path.exists(cue):
                _tmp_cue_dir = tempfile.mkdtemp(prefix="cue_")
                # Copy/link the bin so the temp CUE can reference it by name
                import shutil as _sh
                _sh.copy2(input_file, os.path.join(_tmp_cue_dir, os.path.basename(input_file)))
                cue = make_temp_cue(input_file, _tmp_cue_dir)
                if log_fn: log_fn(f"Auto-generated CUE for bare BIN ({os.path.basename(input_file)})", "warn")
                input_file = cue  # pass the CUE to chdman
        if chd_type == "hd":
            cmd = ["chdman", "createhd", "-i", input_file, "-o", output_file, "-f"]
        else:
            cmd = ["chdman", "createcd", "-i", input_file, "-o", output_file, "-f"]
        if log_fn: log_fn(f"Running: {' '.join(cmd)}")
        rc = _run_with_progress(cmd, log_fn=log_fn, progress_fn=progress_fn)
        if rc != 0:
            raise RuntimeError(f"chdman failed (exit code {rc})")
        return output_file
    finally:
        if _tmp_cue_dir:
            shutil.rmtree(_tmp_cue_dir, ignore_errors=True)


# ── Conversion Worker ────────────────────────────────────────────

class ConversionWorker:
    def __init__(self, job_queue, jobs, jobs_lock, settings, update_job_fn,
                 log_fn, broadcast_fn, conflict_events, conflict_resolutions, apply_to_all_resolution, queue_paused=None):
        self.job_queue   = job_queue
        self.jobs        = jobs
        self.jobs_lock   = jobs_lock
        self.settings    = settings
        self.update_job  = update_job_fn
        self.log         = log_fn
        self.broadcast   = broadcast_fn
        self.conflict_events      = conflict_events
        self.conflict_resolutions = conflict_resolutions
        self.apply_to_all         = apply_to_all_resolution
        self.queue_paused         = queue_paused or [False]
        self._stop = False

    def run(self):
        while not self._stop:
            # Respect pause — sit idle without consuming jobs
            if self.queue_paused[0]:
                import time as _time; _time.sleep(0.5)
                continue
            job_id = None
            try:
                try:
                    job_id = self.job_queue.get(timeout=1)
                except Exception:
                    continue
                with self.jobs_lock:
                    if job_id not in self.jobs:
                        self.job_queue.task_done()
                        continue
                    if self.jobs[job_id]["status"] == "cancelled":
                        self.job_queue.task_done()
                        continue
                self.process_job(job_id)
            except Exception as e:
                # Should never reach here — process_job has its own handler.
                # Belt-and-suspenders: make sure job doesn't stay stuck in running.
                logger.exception(f"Worker outer exception for job {job_id}: {e}")
                if job_id:
                    try:
                        self.update_job(job_id, status="failed", error=f"Unexpected error: {e}", progress=0)
                    except Exception:
                        pass
            finally:
                if job_id is not None:
                    try:
                        self.job_queue.task_done()
                    except Exception:
                        pass

    def _out_base(self, src_path, archive_path, game_name=None):
        """Determine the output base filename.
        Always uses the archive name (or source filename) — never the game DB name.
        Game DB is display-only; it never affects where files are written.
        """
        if archive_path:
            return os.path.splitext(os.path.basename(archive_path))[0]
        return os.path.splitext(os.path.basename(src_path))[0]

    def process_job(self, job_id):
        with self.jobs_lock:
            job = dict(self.jobs[job_id])

        import time as _time
        job_start = _time.monotonic()

        self.update_job(job_id, status="running", progress=5)
        file_path   = job["file_path"]
        ext         = os.path.splitext(file_path)[1].lower()
        dest_folder = self.settings.get("destination_folder", "/destination")
        dest_sub    = self.settings.get("dest_subfolder", "").strip("/")
        if dest_sub:
            dest_folder = os.path.join(dest_folder, dest_sub)
        os.makedirs(dest_folder, exist_ok=True)

        # Record input size for compression ratio report
        try:
            input_bytes = os.path.getsize(file_path)
        except Exception:
            input_bytes = 0
        self.update_job(job_id, input_bytes=input_bytes)

        do_lookup  = self.settings.get("lookup_game_name", False)
        dump_mode  = self.settings.get("bad_dump_detection", "off")
        do_rezip   = self.settings.get("rezip_after_conversion", False)
        rezip_lvl  = self.settings.get("rezip_compression_level", 5)

        def log(msg, level="info"):
            self.log(job_id, msg, level)

        def finish(status, **kw):
            elapsed = round(_time.monotonic() - job_start, 1)
            # Try to capture output file size
            with self.jobs_lock:
                j = self.jobs.get(job_id, {})
                out = j.get("rezip_path") or j.get("output_path")
            try:
                out_bytes = os.path.getsize(out) if out and os.path.exists(out) else 0
            except Exception:
                out_bytes = 0
            self.update_job(job_id, status=status, elapsed_sec=elapsed,
                            output_bytes=out_bytes, **kw)

        try:
            is_archive = any(file_path.lower().endswith(a) for a in ARCHIVE_EXTENSIONS) \
                         or ext in (".7z", ".zip", ".rar", ".gz", ".tgz", ".tar")

            if is_archive:
                if not self.settings.get("extract_archives", True):
                    log("Archive extraction disabled. Skipping.", "warn")
                    self.update_job(job_id, status="skipped")
                    return

                # ── Always-on fast duplicate check (uses zip name, no extraction) ──
                # Normalised comparison ignores ALL parentheticals:
                # "Game (USA) (v1.03)" == "Game (USA) (v2.00)" == "Game"
                archive_base  = os.path.splitext(os.path.basename(file_path))[0]
                overwrite_set = self.settings.get("overwrite_existing", "ask")
                norm_archive  = _normalize_name(archive_base)
                dest_names    = build_dest_chd_set(dest_folder)

                if norm_archive in dest_names:
                    if overwrite_set == "skip":
                        self.update_job(job_id, status="skipped", progress=100)
                        log(f"⏭ Skipped — already exists in destination: {archive_base}", "warn")
                        return
                    elif overwrite_set == "ask":
                        match_path = None
                        for root, _, files in os.walk(dest_folder):
                            for fname in files:
                                if fname.lower().endswith(('.chd', '.7z')) and \
                                        _normalize_name(fname) == norm_archive:
                                    match_path = os.path.join(root, fname)
                                    break
                            if match_path:
                                break
                        if match_path:
                            conflict_result = self._handle_conflict(job_id, match_path, log)
                            if conflict_result == "skip":
                                self.update_job(job_id, status="skipped", progress=100)
                                return
                    # overwrite_set == "overwrite": fall through and proceed

                # Use dest volume for tmp so extraction doesn't fill Docker overlay
                _tmp_base = dest_folder if os.path.isdir(dest_folder) else None
                tmp_dir = tempfile.mkdtemp(prefix="chd_extract_", dir=_tmp_base)
                try:
                    # ── EXTRACTING ────────────────────────────────
                    self.update_job(job_id, status="extracting", progress=5)

                    def ext_pfn(pct):
                        self.update_job(job_id, progress=int(5 + pct * 0.20))  # 5→25%

                    extract_archive(file_path, tmp_dir, log_fn=log, progress_fn=ext_pfn)
                    self.update_job(job_id, progress=25)

                    cue_files              = find_cue_files(tmp_dir)
                    iso_files, orphan_bins = find_iso_files(tmp_dir)

                    # Auto-generate CUE sheets for orphan .bin files (no paired .cue found).
                    # make_temp_cue() sniffs the sector header to detect MODE1 vs MODE2.
                    # Generated .cue goes in the same tmp_dir so chdman can find the .bin
                    # by relative name — no file copying needed.
                    for obin in orphan_bins:
                        try:
                            gen_cue = make_temp_cue(obin, os.path.dirname(obin))
                            cue_files.append(gen_cue)
                            log(f"📄 Auto-generated CUE for '{os.path.basename(obin)}'", "info")
                        except Exception as cue_err:
                            log(f"⚠️  Could not generate CUE for '{os.path.basename(obin)}': {cue_err}", "warn")

                    convertible = cue_files if cue_files else iso_files

                    if not convertible:
                        if orphan_bins:
                            log("Skipped: archive contains only .bin files and CUE generation failed.", "warn")
                            self.update_job(job_id, status="skipped", progress=100)
                        else:
                            log("No convertible files found in archive.", "error")
                            self.update_job(job_id, status="failed", error="No ISO/CUE files found")
                        return

                    log(f"Found {len(convertible)} file(s) to convert")
                    total = len(convertible)
                    success_count = 0

                    # For archive→CHD: use extracted ISO size not the archive size,
                    # so the compression ratio compares apples to apples.
                    # Exception: if we're rezipping to 7z, keep archive size (7z→7z is fair).
                    if not do_rezip:
                        try:
                            # convertible may be CUE files — sum the actual BIN/ISO data files instead.
                            # For each CUE, resolve its referenced BIN. For ISOs, use directly.
                            data_files = []
                            for f in convertible:
                                if f.lower().endswith('.cue'):
                                    # Resolve all BIN references in the CUE
                                    # Handles both quoted: FILE "name.bin" BINARY
                                    # and unquoted:        FILE name.bin BINARY
                                    try:
                                        with open(f) as cf:
                                            for line in cf:
                                                ls = line.strip()
                                                if ls.upper().startswith('FILE'):
                                                    # Try quoted first
                                                    parts = ls.split('"')
                                                    if len(parts) >= 3:
                                                        bin_ref = parts[1]
                                                    else:
                                                        # Unquoted: FILE name.bin BINARY
                                                        tokens = ls.split()
                                                        bin_ref = tokens[1] if len(tokens) >= 2 else ''
                                                    if bin_ref:
                                                        bin_path = os.path.join(os.path.dirname(f), bin_ref)
                                                        if os.path.exists(bin_path):
                                                            data_files.append(bin_path)
                                    except Exception:
                                        pass
                                else:
                                    data_files.append(f)
                            # Deduplicate (multi-track CUEs may reference same BIN multiple times)
                            data_files = list(dict.fromkeys(data_files))
                            iso_bytes = sum(os.path.getsize(f) for f in data_files if os.path.exists(f))
                            if iso_bytes > 0:
                                self.update_job(job_id, input_bytes=iso_bytes)
                        except Exception:
                            pass

                    for i, src in enumerate(convertible):
                        # ── RUNNING ───────────────────────────────
                        self.update_job(job_id, status="running")
                        chd_type = self.settings.get("chd_type", "auto")
                        if chd_type == "auto":

                            chd_type = detect_chd_type(src)

                        if dump_mode != "off":
                            is_bad, reason = check_bad_dump(src, mode=dump_mode, log_fn=log)
                            self.update_job(job_id, bad_dump=is_bad, bad_dump_reason=reason)
                            if is_bad: log(f"⚠️  Bad dump: {reason}", "warn")

                        # Output is always named after the archive — game DB is display-only
                        base    = self._out_base(src, file_path)
                        out_chd = os.path.join(dest_folder, base + ".chd")

                        # Disc ID detection: display badge only, never affects output name
                        if do_lookup:
                            from game_db import get_game_name
                            disc_id, game_name = get_game_name(src, iso_path=src)
                            if game_name:
                                log(f"🎮 {game_name} ({disc_id})", "success")
                                self.update_job(job_id, disc_id=disc_id, game_name=game_name)
                            elif disc_id:
                                log(f"Disc ID: {disc_id} (not in database)", "warn")
                                self.update_job(job_id, disc_id=disc_id)

                        log(f"[{i+1}/{total}] {os.path.basename(src)} → {base}.chd ({chd_type.upper()})")

                        conflict_result = self._handle_conflict(job_id, out_chd, log)
                        if conflict_result == "skip":
                            log(f"⏭ Skipping: '{base}.chd' already exists in destination", "warn")
                            continue

                        base_p  = 25 + int(65 * i / total)
                        range_p = max(1, int(65 / total))

                        def make_pfn(bp, rp, jid=job_id, ct=chd_type, op=out_chd):
                            def pfn(pct):
                                self.update_job(jid, progress=int(bp + (pct/100)*rp),
                                                chd_type_used=ct, output_path=op)
                            return pfn

                        run_chdman(src, out_chd, chd_type, log_fn=log,
                                   progress_fn=make_pfn(base_p, range_p))
                        log(f"Done: {base}.chd", "success")
                        success_count += 1
                        self.update_job(job_id,
                                        progress=25 + int(65 * (i+1) / total),
                                        output_path=out_chd, chd_type_used=chd_type)

                        # ── REZIPPING ─────────────────────────────
                        if do_rezip and os.path.exists(out_chd):
                            self.update_job(job_id, status="rezipping", progress=90)
                            out_7z = os.path.splitext(out_chd)[0] + ".7z"

                            def rz_pfn(pct):
                                self.update_job(job_id, progress=int(90 + pct * 0.09))

                            rezip_to_7z(out_chd, out_7z, compression=rezip_lvl,
                                        log_fn=log, progress_fn=rz_pfn)
                            self.update_job(job_id, rezip_path=out_7z, output_path=out_7z)
                            log(f"Archived: {os.path.basename(out_7z)}", "success")

                    if self.settings.get("delete_archive_after", False):
                        os.remove(file_path)
                        log(f"Deleted source archive: {os.path.basename(file_path)}", "warn")

                    # RA hash the output CHD(s) if enabled
                    if success_count > 0:
                        for i2, src2 in enumerate(convertible):
                            base2   = self._out_base(src2, file_path, None)
                            chd2    = os.path.join(dest_folder, base2 + ".chd")
                            if os.path.exists(chd2):
                                self._try_ra_hash(job_id, chd2, log)

                    # If nothing was actually converted, mark as skipped not completed
                    if success_count == 0:
                        finish("skipped", progress=100)
                        log(f"Skipped — all {total} file(s) already exist in destination", "warn")
                    else:
                        finish("completed", progress=100)
                        log(f"Completed {success_count}/{total} conversions", "success")

                finally:
                    shutil.rmtree(tmp_dir, ignore_errors=True)

            else:
                # ── Direct ISO ────────────────────────────────────
                chd_type = self.settings.get("chd_type", "auto")
                if chd_type == "auto":
                    chd_type = detect_chd_type(file_path)

                if dump_mode != "off":
                    is_bad, reason = check_bad_dump(file_path, mode=dump_mode, log_fn=log)
                    self.update_job(job_id, bad_dump=is_bad, bad_dump_reason=reason)
                    if is_bad: log(f"⚠️  Bad dump: {reason}", "warn")

                # Output is always named after the source file — game DB is display-only
                base    = self._out_base(file_path, None)

                # Disc ID detection: display badge only, never affects output name
                if do_lookup:
                    from game_db import get_game_name
                    disc_id, game_name = get_game_name(file_path, iso_path=file_path)
                    if game_name:
                        log(f"🎮 {game_name} ({disc_id})", "success")
                        self.update_job(job_id, disc_id=disc_id, game_name=game_name)
                    elif disc_id:
                        log(f"Disc ID: {disc_id} (not in DB)", "warn")
                        self.update_job(job_id, disc_id=disc_id)
                out_chd = os.path.join(dest_folder, base + ".chd")

                # Fast normalized duplicate check (same logic as archive path)
                norm_src  = _normalize_name(os.path.basename(file_path))
                dest_names = build_dest_chd_set(dest_folder)
                overwrite_set = self.settings.get("overwrite_existing", "ask")
                if norm_src in dest_names and overwrite_set == "skip":
                    self.update_job(job_id, status="skipped", progress=100)
                    log(f"⏭ Skipped — already exists in destination: {base}", "warn")
                    return

                log(f"Converting: {os.path.basename(file_path)} → {base}.chd ({chd_type.upper()})")
                self.update_job(job_id, progress=10, chd_type_used=chd_type)

                conflict_result = self._handle_conflict(job_id, out_chd, log)
                if conflict_result == "skip":
                    self.update_job(job_id, status="skipped", progress=100)
                    return

                def iso_pfn(pct):
                    self.update_job(job_id, progress=int(10 + (pct/100)*75))

                run_chdman(file_path, out_chd, chd_type, log_fn=log, progress_fn=iso_pfn)
                self.update_job(job_id, progress=85, output_path=out_chd)

                if self.settings.get("delete_iso_after", False):
                    if ext in (".iso", ".img", ".mdf", ".nrg"):
                        os.remove(file_path)
                        log(f"Deleted source: {os.path.basename(file_path)}", "warn")
                    elif ext == ".cue":
                        try:
                            with open(file_path) as cf:
                                for line in cf:
                                    if "FILE" in line.upper():
                                        parts = line.strip().split('"')
                                        if len(parts) >= 2:
                                            bp = os.path.join(os.path.dirname(file_path), parts[1])
                                            if os.path.exists(bp): os.remove(bp)
                        except: pass
                        os.remove(file_path)
                        log("Deleted source files", "warn")

                if do_rezip and os.path.exists(out_chd):
                    self.update_job(job_id, status="rezipping", progress=90)
                    out_7z = os.path.splitext(out_chd)[0] + ".7z"

                    def iso_rz_pfn(pct):
                        self.update_job(job_id, progress=int(90 + pct * 0.09))

                    rezip_to_7z(out_chd, out_7z, compression=rezip_lvl,
                                log_fn=log, progress_fn=iso_rz_pfn)
                    self.update_job(job_id, rezip_path=out_7z, output_path=out_7z)
                    log(f"Archived: {os.path.basename(out_7z)}", "success")

                self._try_ra_hash(job_id, out_chd, log)
                finish("completed", progress=100)
                log(f"Done: {base}.chd", "success")

        except Exception as e:
            logger.exception(f"Job {job_id} failed")
            finish("failed", error=str(e), progress=0)
            log(f"Error: {e}", "error")

    def _try_ra_hash(self, job_id, chd_path, log):
        """Compute RA hash for a finished CHD if the setting is enabled. Non-blocking."""
        if not self.settings.get("ra_hash_on_convert", False):
            return
        if not os.path.exists(chd_path):
            return
        try:
            from ra_hasher import compute_ra_hash, lookup_ra_hash
            log(f"[RA] Computing hash for {os.path.basename(chd_path)}…")
            md5, exe, err = compute_ra_hash(chd_path)
            if err:
                log(f"[RA] Hash error: {err}", "warn")
                return
            log(f"[RA] Hash: {md5}  ({exe})")
            ra = lookup_ra_hash(
                md5,
                ra_username=self.settings.get("ra_username", ""),
                ra_api_key=self.settings.get("ra_api_key", "")
            )
            if ra.get("error"):
                log(f"[RA] API error: {ra['error']}", "warn")
            elif ra["found"]:
                title = ra.get("game_title") or f"Game ID {ra['game_id']}"
                log(f"[RA] ✅ Recognized: {title} (ID {ra['game_id']})", "success")
                self.update_job(job_id, ra_hash=md5, ra_game_id=ra["game_id"], ra_game_title=title)
            else:
                log(f"[RA] ⚠️  Hash not found in RA database — achievements may not work", "warn")
                self.update_job(job_id, ra_hash=md5, ra_game_id=None)
        except Exception as e:
            log(f"[RA] Hash failed: {e}", "warn")

    def _handle_conflict(self, job_id, out_path, log):
        if not os.path.exists(out_path):
            return "overwrite"
        if self.apply_to_all[0]:
            return self.apply_to_all[0]
        setting = self.settings.get("overwrite_existing", "ask")
        if setting == "skip":
            log(f"Already exists, skipping: {os.path.basename(out_path)}", "warn")
            return "skip"
        if setting == "overwrite":
            log(f"Already exists, overwriting: {os.path.basename(out_path)}", "warn")
            return "overwrite"
        log(f"File exists — waiting for your decision: {os.path.basename(out_path)}", "warn")
        event = threading.Event()
        self.conflict_events[job_id] = event
        self.broadcast("conflict", {"job_id": job_id,
                                    "filename": os.path.basename(out_path),
                                    "path": out_path})
        event.wait(timeout=300)
        resolution = self.conflict_resolutions.get(job_id, "skip")
        self.conflict_events.pop(job_id, None)
        self.conflict_resolutions.pop(job_id, None)
        return resolution

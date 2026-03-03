import os, json, uuid, threading, queue, logging
from datetime import datetime
from flask import Flask, render_template, request, jsonify, Response, stream_with_context

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)
CONFIG_PATH = "/config/settings.json"

DEFAULT_SETTINGS = {
    "source_folder":           "/source",
    "destination_folder":      "/destination",
    "extract_archives":        True,
    "delete_iso_after":        False,
    "delete_archive_after":    False,
    "overwrite_existing":      "ask",
    "max_workers":             1,
    "auto_scan":               False,
    "scan_interval":           60,
    "chd_type":                "auto",
    "recursive_scan":          True,
    "dark_mode":               True,
    "rename_to_archive":       False,
    # New settings
    "rezip_after_conversion":    False,
    "rezip_compression_level":   5,
    "lookup_game_name":          False,
    "bad_dump_detection":        "off",   # off | size | checksum
}

job_queue            = queue.Queue()
jobs                 = {}
jobs_lock            = threading.Lock()
sse_clients          = []
sse_lock             = threading.Lock()
conflict_events      = {}
conflict_resolutions = {}
apply_to_all_resolution = [None]
settings             = {}
scanner_instance     = [None]

APP_VERSION = "1.0.1"

@app.route("/version")
def get_version():
    return jsonify({"version": APP_VERSION})



def load_settings():
    global settings
    os.makedirs("/config", exist_ok=True)
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            settings = {**DEFAULT_SETTINGS, **json.load(f)}
    else:
        settings = DEFAULT_SETTINGS.copy()
        save_settings()


def save_settings():
    os.makedirs("/config", exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(settings, f, indent=2)


def broadcast_event(event_type, data):
    # Strip logs from job_update broadcasts — clients fetch logs on demand
    if event_type == "job_update":
        data = _slim_job(data)
    msg = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
    with sse_lock:
        dead = []
        for q in sse_clients:
            try: q.put_nowait(msg)
            except: dead.append(q)
        for q in dead: sse_clients.remove(q)


def update_job(job_id, **kwargs):
    data = {}
    completed = False
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id].update(kwargs)
            jobs[job_id]["updated_at"] = datetime.now().isoformat()
            data = dict(jobs[job_id])
            if kwargs.get("status") in ("completed", "failed", "skipped"):
                completed = True
    if data:
        broadcast_event("job_update", data)
    if completed:
        from history import history_manager
        history_manager.add(data)


def log_to_job(job_id, message, level="info"):
    entry = {"time": datetime.now().strftime("%H:%M:%S"), "msg": message, "level": level}
    with jobs_lock:
        if job_id in jobs: jobs[job_id]["log"].append(entry)
    broadcast_event("job_log", {"job_id": job_id, "entry": entry})


def add_job(file_path, job_type="file"):
    job_id = str(uuid.uuid4())
    job = {
        "id":            job_id,
        "file_path":     file_path,
        "filename":      os.path.basename(file_path),
        "status":        "queued",
        "progress":      0,
        "log":           [],
        "created_at":    datetime.now().isoformat(),
        "updated_at":    datetime.now().isoformat(),
        "type":          job_type,
        "output_path":   None,
        "rezip_path":    None,
        "error":         None,
        "chd_type_used": None,
        "disc_id":       None,
        "game_name":     None,
        "bad_dump":      False,
        "bad_dump_reason": None,
    }
    with jobs_lock: jobs[job_id] = job
    job_queue.put(job_id)
    broadcast_event("job_added", job)
    return job_id


# ── Routes ────────────────────────────────────────────────────────

@app.route("/")
def index(): return render_template("index.html")

@app.route("/api/settings", methods=["GET"])
def get_settings(): return jsonify(settings)

@app.route("/api/settings", methods=["POST"])
def update_settings_route():
    global settings
    settings.update(request.json)
    save_settings()
    if scanner_instance[0]: scanner_instance[0].update_settings(settings)
    return jsonify({"success": True, "settings": settings})

@app.route("/api/jobs", methods=["GET"])
def get_jobs():
    status   = request.args.get("status")       # filter by status
    page     = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    slim     = request.args.get("slim", "0") == "1"  # strip logs

    with jobs_lock:
        all_jobs = list(jobs.values())

    if status:
        statuses = status.split(",")
        all_jobs = [j for j in all_jobs if j["status"] in statuses]

    # Sort: active first, then by updated_at desc
    def sort_key(j):
        active_order = {"running": 0, "extracting": 0, "rezipping": 0, "queued": 1}
        return (active_order.get(j["status"], 2), j.get("updated_at", ""))
    all_jobs.sort(key=sort_key)

    total  = len(all_jobs)
    start  = (page - 1) * per_page
    paged  = all_jobs[start:start + per_page]
    if slim:
        paged = [_slim_job(j) for j in paged]

    return jsonify({
        "jobs":     paged,
        "total":    total,
        "page":     page,
        "per_page": per_page,
        "pages":    max(1, (total + per_page - 1) // per_page),
    })

@app.route("/api/jobs/<job_id>", methods=["GET"])
def get_single_job(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job: return jsonify({"error": "Not found"}), 404
    return jsonify(job)  # includes log

@app.route("/api/jobs/scan", methods=["POST"])
def scan_and_queue():
    from scanner import find_convertible_files
    files = find_convertible_files(settings.get("source_folder", "/source"), settings.get("recursive_scan", True))
    added, skipped = [], []
    for f in files:
        already = any(j["file_path"] == f and j["status"] in ("queued","running","extracting","rezipping","completed")
                      for j in jobs.values())
        if not already:
            jid = add_job(f)
            added.append({"job_id": jid, "file": f})
        else:
            skipped.append(f)
    return jsonify({"added": len(added), "skipped": len(skipped), "jobs": added})

@app.route("/api/jobs/add", methods=["POST"])
def add_specific_job():
    path = request.json.get("path")
    if not path or not os.path.exists(path): return jsonify({"error": "File not found"}), 400
    return jsonify({"job_id": add_job(path)})

@app.route("/api/jobs/<job_id>/cancel", methods=["POST"])
def cancel_job(job_id):
    data = {}
    with jobs_lock:
        if job_id in jobs and jobs[job_id]["status"] == "queued":
            jobs[job_id]["status"] = "cancelled"
            data = dict(jobs[job_id])
    if data: broadcast_event("job_update", data)
    return jsonify({"success": True})

@app.route("/api/jobs/<job_id>/retry", methods=["POST"])
def retry_job(job_id):
    with jobs_lock:
        if job_id not in jobs: return jsonify({"error": "Not found"}), 404
        jobs[job_id].update({"status": "queued", "progress": 0, "log": [], "error": None})
        data = dict(jobs[job_id])
    job_queue.put(job_id)
    broadcast_event("job_update", data)
    return jsonify({"success": True})

@app.route("/api/jobs/clear", methods=["POST"])
def clear_completed():
    statuses = (request.json or {}).get("statuses", ["completed","cancelled","failed","skipped"])
    removed = []
    with jobs_lock:
        to_remove = [jid for jid,j in jobs.items() if j["status"] in statuses]
        for jid in to_remove:
            del jobs[jid]; removed.append(jid)
    broadcast_event("jobs_cleared", {"ids": removed})
    return jsonify({"removed": len(removed)})

@app.route("/api/conflict/resolve", methods=["POST"])
def resolve_conflict():
    data = request.json
    job_id, resolution = data.get("job_id"), data.get("resolution")
    if data.get("apply_to_all"): apply_to_all_resolution[0] = resolution
    conflict_resolutions[job_id] = resolution
    if job_id in conflict_events: conflict_events[job_id].set()
    return jsonify({"success": True})

@app.route("/api/browse", methods=["GET"])
def browse_folder():
    path = request.args.get("path", "/")
    try:
        entries = [{"name": n, "path": os.path.join(path, n), "type": "dir"}
                   for n in sorted(os.listdir(path)) if os.path.isdir(os.path.join(path, n))]
        return jsonify({"path": path, "entries": entries, "parent": os.path.dirname(path)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/api/stats", methods=["GET"])
def get_stats():
    with jobs_lock:
        by_status = {}
        for j in jobs.values():
            by_status[j["status"]] = by_status.get(j["status"], 0) + 1
        return jsonify({
            "total":     len(jobs),
            "by_status": by_status,
            "active":    sum(by_status.get(s, 0) for s in ("queued","running","extracting","rezipping")),
        })

# ── History endpoints ─────────────────────────────────────────────


@app.route("/api/scan/preview", methods=["GET"])
def scan_preview():
    """Return list of convertible files without queueing them."""
    from scanner import find_convertible_files
    files = find_convertible_files(
        settings.get("source_folder", "/source"),
        settings.get("recursive_scan", True)
    )
    active_paths = {j["file_path"] for j in jobs.values()
                    if j["status"] in ("queued","running","extracting","rezipping","completed")}
    result = []
    for f in files:
        result.append({
            "path": f,
            "name": os.path.basename(f),
            "size": os.path.getsize(f) if os.path.exists(f) else 0,
            "already_queued": f in active_paths,
        })
    return jsonify(result)

@app.route("/api/jobs/queue_files", methods=["POST"])
def queue_selected_files():
    """Queue a specific list of file paths."""
    paths = request.json.get("paths", [])
    added, skipped = [], []
    for p in paths:
        if not os.path.exists(p):
            skipped.append(p)
            continue
        already = any(j["file_path"] == p and j["status"] in ("queued","running","extracting","rezipping","completed")
                      for j in jobs.values())
        if not already:
            jid = add_job(p)
            added.append({"job_id": jid, "file": p})
        else:
            skipped.append(p)
    return jsonify({"added": len(added), "skipped": len(skipped), "jobs": added})

@app.route("/api/history", methods=["GET"])
def get_history():
    from history import history_manager
    return jsonify(history_manager.get_entries())

@app.route("/api/history/clear", methods=["POST"])
def clear_history():
    from history import history_manager
    history_manager.clear()
    broadcast_event("history_cleared", {})
    return jsonify({"success": True})

# ── SSE stream ────────────────────────────────────────────────────

ACTIVE_STATUSES = {"queued", "running", "extracting", "rezipping"}
INIT_MAX_QUEUED    = 20   # queued jobs to include in init (rest load on demand)
INIT_MAX_COMPLETED = 50   # completed jobs to include in init
INIT_MAX_FAILED    = 20   # failed/cancelled jobs to include in init

def _slim_job(job):
    """Strip log from job for SSE init — logs are fetched on demand."""
    j = dict(job)
    j.pop("log", None)
    return j

@app.route("/stream")
def stream():
    def event_stream():
        q = queue.Queue()
        with sse_lock: sse_clients.append(q)
        try:
            with jobs_lock:
                all_jobs = list(jobs.values())

            # Truly in-progress (never capped)
            running  = [j for j in all_jobs if j["status"] in ("running", "extracting", "rezipping")]
            # Queued — cap to first N (they're worked FIFO so show the front)
            queued   = [j for j in all_jobs if j["status"] == "queued"][:INIT_MAX_QUEUED]
            # Terminal — most recent first, capped
            failed   = sorted(
                [j for j in all_jobs if j["status"] in ("failed", "cancelled")],
                key=lambda j: j.get("updated_at", ""), reverse=True
            )[:INIT_MAX_FAILED]
            done     = sorted(
                [j for j in all_jobs if j["status"] in ("completed", "skipped")],
                key=lambda j: j.get("updated_at", ""), reverse=True
            )[:INIT_MAX_COMPLETED]

            init_jobs = running + queued + failed + done

            total_counts = {}
            for j in all_jobs:
                s = j["status"]
                total_counts[s] = total_counts.get(s, 0) + 1

            payload = {
                "jobs": [_slim_job(j) for j in init_jobs],
                "settings": settings,
                "total_counts": total_counts,
                "total_jobs": len(all_jobs),
            }
            yield f"event: init\ndata: {json.dumps(payload)}\n\n"
            while True:
                try: yield q.get(timeout=25)
                except queue.Empty: yield "event: ping\ndata: {}\n\n"
        except GeneratorExit:
            with sse_lock:
                if q in sse_clients: sse_clients.remove(q)
    return Response(stream_with_context(event_stream()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    load_settings()

    from history import history_manager
    history_manager.load()

    from converter import ConversionWorker
    from scanner   import FolderScanner

    w = ConversionWorker(job_queue, jobs, jobs_lock, settings, update_job, log_to_job,
                         broadcast_event, conflict_events, conflict_resolutions, apply_to_all_resolution)
    threading.Thread(target=w.run, daemon=True).start()

    sc = FolderScanner(settings, add_job, jobs, jobs_lock)
    scanner_instance[0] = sc
    threading.Thread(target=sc.run, daemon=True).start()

    app.run(host="0.0.0.0", port=9292, threaded=True, debug=False)

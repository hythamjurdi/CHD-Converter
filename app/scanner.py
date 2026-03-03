import os, time, logging

logger = logging.getLogger(__name__)

# Note: .bin is intentionally excluded — bare .bin files without a .cue
# cannot be converted and would hang chdman. Only .cue files are queued;
# the .bin is read automatically via the .cue sheet.
CONVERTIBLE_EXTENSIONS = {".iso", ".img", ".cue", ".mdf", ".nrg",
                           ".7z", ".zip", ".rar", ".tar", ".gz", ".tgz"}

def find_convertible_files(folder, recursive=True):
    if not os.path.isdir(folder):
        return []
    results = []
    if recursive:
        for root, _, files in os.walk(folder):
            for f in files:
                if os.path.splitext(f)[1].lower() in CONVERTIBLE_EXTENSIONS:
                    results.append(os.path.join(root, f))
    else:
        for f in os.listdir(folder):
            full = os.path.join(folder, f)
            if os.path.isfile(full) and os.path.splitext(f)[1].lower() in CONVERTIBLE_EXTENSIONS:
                results.append(full)
    return results


class FolderScanner:
    def __init__(self, settings, add_job_fn, jobs, jobs_lock):
        self.settings = settings
        self.add_job = add_job_fn
        self.jobs = jobs
        self.jobs_lock = jobs_lock

    def update_settings(self, settings):
        self.settings = settings

    def run(self):
        while True:
            try:
                if self.settings.get("auto_scan", False):
                    interval = self.settings.get("scan_interval", 60)
                    source = self.settings.get("source_folder", "/source")
                    recursive = self.settings.get("recursive_scan", True)
                    files = find_convertible_files(source, recursive)
                    for f in files:
                        already = False
                        with self.jobs_lock:
                            already = any(j["file_path"] == f and j["status"] in ("queued","running")
                                         for j in self.jobs.values())
                        if not already:
                            self.add_job(f)
                    time.sleep(interval)
                else:
                    time.sleep(10)
            except Exception as e:
                logger.error(f"Scanner error: {e}")
                time.sleep(30)

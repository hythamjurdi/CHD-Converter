"""
Persistent statistics manager.
Stores per-job stats to /config/stats.json and exposes aggregate totals.
"""
import os, json, logging
from datetime import datetime

logger = logging.getLogger(__name__)
STATS_PATH = "/config/stats.json"

class StatsManager:
    def __init__(self):
        self._data = None

    def load(self):
        os.makedirs("/config", exist_ok=True)
        if os.path.exists(STATS_PATH):
            try:
                with open(STATS_PATH) as f:
                    self._data = json.load(f)
                # migrate: ensure expected keys exist
                self._data.setdefault("entries", [])
                self._data.setdefault("totals", {})
            except Exception as e:
                logger.warning(f"Could not load stats: {e}")
                self._data = {"entries": [], "totals": {}}
        else:
            self._data = {"entries": [], "totals": {}}
        return self._data

    def _save(self):
        try:
            os.makedirs("/config", exist_ok=True)
            with open(STATS_PATH, "w") as f:
                json.dump(self._data, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save stats: {e}")

    def record(self, job):
        """Record a completed/failed/skipped job."""
        if self._data is None:
            self.load()
        status = job.get("status", "unknown")
        entry = {
            "id":           job.get("id"),
            "filename":     job.get("filename") or os.path.basename(job.get("file_path", "")),
            "game_name":    job.get("game_name"),
            "disc_id":      job.get("disc_id"),
            "status":       status,
            "chd_type":     job.get("chd_type_used"),
            "input_bytes":  job.get("input_bytes", 0) or 0,
            "output_bytes": job.get("output_bytes", 0) or 0,
            "elapsed_sec":  job.get("elapsed_sec", 0) or 0,
            "error":        job.get("error"),
            "timestamp":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        # Derived fields
        ib, ob = entry["input_bytes"], entry["output_bytes"]
        entry["saved_bytes"]  = max(0, ib - ob) if (ib > 0 and ob > 0) else 0
        # ratio_pct: only compute when input is meaningful (>1KB).
        # Zero input means size wasn't captured (e.g. CUE-only before the BIN-size fix).
        if ib > 1024 and ob > 0:
            entry["ratio_pct"] = round((1 - ob / ib) * 100)
        else:
            entry["ratio_pct"] = None
        entry["speed_mbps"]   = round(ib / entry["elapsed_sec"] / 1_000_000, 1)                                 if (ib > 1024 and entry["elapsed_sec"] > 0) else None

        self._data["entries"].insert(0, entry)
        self._data["entries"] = self._data["entries"][:5000]  # cap at 5000

        # Recompute totals
        self._recompute_totals()
        self._save()

    def _recompute_totals(self):
        entries = self._data["entries"]
        t = {
            "total_jobs":      len(entries),
            "completed":       sum(1 for e in entries if e["status"] == "completed"),
            "failed":          sum(1 for e in entries if e["status"] == "failed"),
            "skipped":         sum(1 for e in entries if e["status"] == "skipped"),
            "total_iso_bytes": sum(e["input_bytes"]  for e in entries if e["status"] == "completed"),
            "total_chd_bytes": sum(e["output_bytes"] for e in entries if e["status"] == "completed"),
            "total_saved_bytes": sum(e["saved_bytes"] for e in entries if e["status"] == "completed"),
            "total_elapsed_sec": sum(e["elapsed_sec"] for e in entries if e["status"] == "completed"),
        }
        completed_with_speed = [e for e in entries
                                 if e["status"] == "completed" and e.get("speed_mbps")]
        t["avg_speed_mbps"] = round(
            sum(e["speed_mbps"] for e in completed_with_speed) / len(completed_with_speed), 1
        ) if completed_with_speed else None
        t["overall_ratio_pct"] = round(
            (1 - t["total_chd_bytes"] / t["total_iso_bytes"]) * 100
        ) if t["total_iso_bytes"] > 0 else None
        self._data["totals"] = t

    def get_all(self):
        if self._data is None:
            self.load()
        return {
            "entries": self._data["entries"],
            "totals":  self._data["totals"],
        }

    def clear(self):
        self._data = {"entries": [], "totals": {}}
        self._save()

stats_manager = StatsManager()

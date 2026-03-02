import os, json, logging
from datetime import datetime

logger = logging.getLogger(__name__)
HISTORY_PATH = "/config/history.json"

class HistoryManager:
    def __init__(self):
        self._data = None

    def load(self):
        os.makedirs("/config", exist_ok=True)
        if os.path.exists(HISTORY_PATH):
            try:
                with open(HISTORY_PATH) as f:
                    self._data = json.load(f)
            except Exception as e:
                logger.warning(f"Could not load history: {e}")
                self._data = {"entries": []}
        else:
            self._data = {"entries": []}
        return self._data

    def _save(self):
        try:
            os.makedirs("/config", exist_ok=True)
            with open(HISTORY_PATH, "w") as f:
                json.dump(self._data, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save history: {e}")

    def add(self, job):
        if self._data is None:
            self.load()
        entry = {
            "id":              job.get("id"),
            "filename":        job.get("filename"),
            "disc_id":         job.get("disc_id"),
            "game_name":       job.get("game_name"),
            "output_path":     job.get("output_path"),
            "rezip_path":      job.get("rezip_path"),
            "status":          job.get("status"),
            "chd_type_used":   job.get("chd_type_used"),
            "bad_dump":        job.get("bad_dump", False),
            "bad_dump_reason": job.get("bad_dump_reason"),
            "timestamp":       datetime.now().isoformat(),
        }
        self._data["entries"].insert(0, entry)
        self._data["entries"] = self._data["entries"][:1000]
        self._save()

    def get_entries(self):
        if self._data is None:
            self.load()
        return self._data.get("entries", [])

    def clear(self):
        self._data = {"entries": []}
        self._save()

history_manager = HistoryManager()

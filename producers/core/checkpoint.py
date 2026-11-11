"""Persist the last safely-acknowledged cursor so a restart can resume."""
import json
import logging
import os

log = logging.getLogger("checkpoint")


class Checkpoint:
    def __init__(self, path):
        self._path = path
        self._last_saved = None

    def load(self):
        """Return the saved cursor, or None if there is no usable checkpoint."""
        try:
            with open(self._path) as f:
                self._last_saved = json.load(f)["event_id"]
        except FileNotFoundError:
            return None
        except (ValueError, KeyError, OSError) as exc:
            log.warning("ignoring unreadable checkpoint %s: %r", self._path, exc)
            return None
        return self._last_saved

    def save(self, event_id):
        """Write the cursor atomically (temp file + rename); skips if unchanged."""
        if event_id is None or event_id == self._last_saved:
            return
        tmp = self._path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"event_id": event_id}, f)
        os.replace(tmp, self._path)  # atomic: never a half-written file
        self._last_saved = event_id

#!/usr/bin/env python3
"""
Deluge Hardlink Cleanup Script
===============================
Removes torrents from Deluge whose files are no longer hardlinked elsewhere.

When Sonarr/Radarr import media, they create hardlinks. When you later delete
from Sonarr/Radarr, the library-side link is removed, dropping the link count
back to 1. This script finds those "orphaned" torrents and cleans them up.

Designed for Unraid — run via the User Scripts plugin on a schedule.

Safety features:
  - DRY RUN by default (set DRY_RUN = False to actually delete)
  - Minimum age guard (won't touch recently-added torrents)
  - Minimum seed time guard (respects private tracker requirements)
  - Size threshold (ignores small non-media files like .nfo, .txt)
  - Logging with full detail of what was checked and why
  - Label/category filtering (optional)
"""

import json
import os
import sys
import time
import logging
import urllib.request
import urllib.error
import http.cookiejar

# =============================================================================
# CONFIGURATION — Edit these to match your setup
# =============================================================================

# Deluge Web UI connection
DELUGE_WEB_URL = "http://localhost:8112"  # Change if needed
DELUGE_WEB_PASSWORD = "deluge"            # Default Deluge Web UI password

# Safety: Set to False only after you've verified dry-run output looks correct
DRY_RUN = True

# Minimum torrent age in hours before considering for cleanup.
# Prevents removing torrents that were just added and haven't been
# hardlinked by Sonarr/Radarr yet.
MIN_AGE_HOURS = 24

# Minimum actual seeding time in hours before considering for cleanup.
# This uses Deluge's tracked seeding time, which only counts time spent
# actively seeding — not time spent paused, stalled, or downloading.
# Set this to meet your private tracker seed time requirements.
MIN_SEED_TIME_HOURS = 336

# Minimum file size in MB to consider "significant" when checking hardlinks.
# Small files (.nfo, .txt, subtitles) are often not hardlinked by the *arrs,
# so we ignore them to avoid false positives.
MIN_FILE_SIZE_MB = 10

# File extensions to consider as media files. Only these are checked for
# hardlink status. If ALL significant media files have nlink == 1, the
# torrent is considered orphaned.
MEDIA_EXTENSIONS = {
    ".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".flv", ".mov", ".ts",
    ".iso", ".img",  # disc images sometimes used
}

# Optional: Only clean up torrents with these labels/categories.
# Leave empty to check ALL torrents.
# Example: LIMIT_TO_LABELS = {"sonarr", "radarr", "tv-sonarr", "movies-radarr"}
LIMIT_TO_LABELS = ()

# Whether to also delete the torrent's data files from disk when removing.
# True  = remove torrent AND files (reclaim space)
# False = remove torrent from Deluge only (files stay on disk — not useful)
DELETE_DATA = True

# Path mapping: If the script runs in a different context (e.g., host vs Docker)
# and file paths differ, set a mapping here.
# Example: If Deluge sees "/downloads/..." but on the host it's "/mnt/user/downloads/..."
#   PATH_MAP = {"/data": "/mnt/user/downloads"}
# Leave empty if paths are the same.
PATH_MAP = {}

# =============================================================================
# END CONFIGURATION
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("deluge-cleanup")


class DelugeWebAPI:
    """Minimal Deluge Web UI JSON-RPC client."""

    def __init__(self, url, password):
        self.url = url.rstrip("/") + "/json"
        self.password = password
        self.request_id = 0
        self.cookie_jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookie_jar)
        )

    def _call(self, method, params=None):
        """Make a JSON-RPC call to Deluge Web UI."""
        self.request_id += 1
        payload = json.dumps({
            "method": method,
            "params": params or [],
            "id": self.request_id,
        }).encode("utf-8")

        req = urllib.request.Request(
            self.url,
            data=payload,
            headers={"Content-Type": "application/json"},
        )

        try:
            with self.opener.open(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.URLError as e:
            log.error("Failed to connect to Deluge Web UI at %s: %s", self.url, e)
            sys.exit(1)

        if data.get("error"):
            raise RuntimeError(f"Deluge RPC error: {data['error']}")
        return data.get("result")

    def login(self):
        result = self._call("auth.login", [self.password])
        if not result:
            log.error("Authentication failed. Check DELUGE_WEB_PASSWORD.")
            sys.exit(1)
        # Also need to connect to the first available daemon
        connected = self._call("web.connected")
        if not connected:
            hosts = self._call("web.get_hosts")
            if hosts:
                self._call("web.connect", [hosts[0][0]])
            else:
                log.error("No Deluge daemon hosts found in Web UI.")
                sys.exit(1)
        log.info("Connected to Deluge Web UI.")

    def get_torrents(self):
        """Fetch all torrents with the fields we need."""
        fields = [
            "name", "save_path", "files", "time_added",
            "label", "total_size", "state", "hash", "seeding_time",
        ]
        # Empty dict = all torrents
        return self._call("core.get_torrents_status", [{}, fields])

    def remove_torrent(self, torrent_id, remove_data=True):
        return self._call("core.remove_torrent", [torrent_id, remove_data])


def map_path(path):
    """Apply path mapping if configured."""
    for src, dst in PATH_MAP.items():
        if path.startswith(src):
            return dst + path[len(src):]
    return path


def check_torrent(torrent_id, info):
    """
    Check if a torrent's media files are orphaned (nlink == 1).

    Returns:
        (should_remove: bool, reason: str)
    """
    name = info.get("name", torrent_id)
    save_path = info.get("save_path", "")
    files = info.get("files", [])
    time_added = info.get("time_added", 0)
    label = info.get("label", "")

    # Label filter
    if LIMIT_TO_LABELS and label not in LIMIT_TO_LABELS:
        return False, f"skipped (label '{label}' not in LIMIT_TO_LABELS)"

    # Age check
    age_hours = (time.time() - time_added) / 3600
    if age_hours < MIN_AGE_HOURS:
        return False, f"skipped (age {age_hours:.1f}h < {MIN_AGE_HOURS}h minimum)"

    # Seed time check
    seeding_time = info.get("seeding_time", 0)  # Deluge reports this in seconds
    seed_hours = seeding_time / 3600
    if seed_hours < MIN_SEED_TIME_HOURS:
        return False, f"skipped (seed time {seed_hours:.1f}h < {MIN_SEED_TIME_HOURS}h minimum)"

    # Collect significant media files
    media_files = []
    for f in files:
        file_path = os.path.join(save_path, f["path"])
        file_path = map_path(file_path)
        _, ext = os.path.splitext(f["path"])

        if ext.lower() in MEDIA_EXTENSIONS and f.get("size", 0) >= MIN_FILE_SIZE_MB * 1024 * 1024:
            media_files.append(file_path)

    if not media_files:
        return False, "skipped (no significant media files found in torrent)"

    # Check hardlink counts
    all_orphaned = True
    missing_files = 0

    for fpath in media_files:
        try:
            stat = os.stat(fpath)
            nlink = stat.st_nlink
            if nlink > 1:
                all_orphaned = False
                log.debug("  %s -> nlink=%d (still linked)", fpath, nlink)
                break  # No need to check further
            else:
                log.debug("  %s -> nlink=%d (orphaned)", fpath, nlink)
        except FileNotFoundError:
            # File already gone from disk — treat as orphaned
            missing_files += 1
            log.debug("  %s -> MISSING", fpath)
        except OSError as e:
            log.warning("  %s -> error: %s", fpath, e)
            # Be conservative — don't remove if we can't check
            return False, f"skipped (OS error checking files: {e})"

    if missing_files == len(media_files):
        return True, "all media files missing from disk"

    if all_orphaned:
        return True, f"all {len(media_files)} media file(s) have nlink=1"

    return False, "still has hardlinked media files (in use)"


def main():
    mode = "DRY RUN" if DRY_RUN else "LIVE"
    log.info("=" * 60)
    log.info("Deluge Hardlink Cleanup — %s MODE", mode)
    log.info("=" * 60)

    if DRY_RUN:
        log.info("No torrents will be removed. Set DRY_RUN = False to enable.")

    api = DelugeWebAPI(DELUGE_WEB_URL, DELUGE_WEB_PASSWORD)
    api.login()

    torrents = api.get_torrents()
    if not torrents:
        log.info("No torrents found in Deluge.")
        return

    log.info("Found %d torrent(s) to evaluate.", len(torrents))

    to_remove = []
    skipped = 0
    kept = 0

    for torrent_id, info in torrents.items():
        name = info.get("name", torrent_id)
        should_remove, reason = check_torrent(torrent_id, info)

        if should_remove:
            log.info("  [REMOVE] %s — %s", name, reason)
            to_remove.append((torrent_id, name))
        else:
            log.info("  [KEEP]   %s — %s", name, reason)
            if "skipped" in reason:
                skipped += 1
            else:
                kept += 1

    # Summary
    log.info("-" * 60)
    log.info(
        "Summary: %d to remove, %d kept (still linked), %d skipped",
        len(to_remove), kept, skipped,
    )

    if not to_remove:
        log.info("Nothing to clean up.")
        return

    if DRY_RUN:
        log.info("DRY RUN — would have removed %d torrent(s):", len(to_remove))
        for tid, tname in to_remove:
            log.info("  - %s", tname)
        return

    # Actually remove
    removed = 0
    errors = 0
    for torrent_id, name in to_remove:
        try:
            api.remove_torrent(torrent_id, DELETE_DATA)
            log.info("  Removed: %s", name)
            removed += 1
        except Exception as e:
            log.error("  Failed to remove %s: %s", name, e)
            errors += 1

    log.info("Done. Removed %d torrent(s), %d error(s).", removed, errors)


if __name__ == "__main__":
    main()

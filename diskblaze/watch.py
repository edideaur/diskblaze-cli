"""Continuous upload watching for the DiskBlaze CLI.

Rather than pulling in an OS-specific filesystem-watching dependency, the
watcher polls the local tree on an interval and uploads any file whose size or
modification time has changed since the previous sweep. Combined with the
client's resume logic, this makes re-uploading cheap and idempotent.
"""

from __future__ import annotations

import fnmatch
import time
from pathlib import Path

from .client import DiskBlazeClient, join_remote, normalize_remote_path


def _match(name: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(name, pattern) for pattern in patterns)


class FolderWatcher:
    """Poll ``local`` and upload changes to ``remote`` until stopped."""

    def __init__(
        self,
        client: DiskBlazeClient,
        local: Path,
        remote: str,
        *,
        interval: float = 5.0,
        checksum: bool = True,
        create_folders: bool = True,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
        on_event=None,
    ):
        self.client = client
        self.local = Path(local)
        self.remote_root = normalize_remote_path(remote)
        self.interval = max(0.5, float(interval))
        self.checksum = checksum
        self.create_folders = create_folders
        self.include = include or []
        self.exclude = exclude or []
        self.on_event = on_event
        self._stop = False
        self._state: dict[str, tuple[int, float]] = {}

    def stop(self) -> None:
        self._stop = True

    def _snapshot(self) -> dict[str, tuple[int, float]]:
        snap: dict[str, tuple[int, float]] = {}
        for path in self.local.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(self.local).as_posix()
            if (
                self.include
                and not _match(rel, self.include)
                and not _match(path.name, self.include)
            ):
                continue
            if self.exclude and (_match(rel, self.exclude) or _match(path.name, self.exclude)):
                continue
            stat = path.stat()
            snap[rel] = (stat.st_size, stat.st_mtime)
        return snap

    def _emit(self, message: str) -> None:
        if self.on_event:
            self.on_event(message)

    def sweep(self) -> int:
        """Run a single pass. Returns the number of files uploaded."""
        current = self._snapshot()
        uploaded = 0
        for rel, (size, mtime) in current.items():
            previous = self._state.get(rel)
            if previous == (size, mtime):
                continue
            remote_path = join_remote(self.remote_root, rel)
            try:
                self.client.upload_file(
                    self.local / rel,
                    remote_path,
                    workers=8,
                    checksum=self.checksum,
                    resume=True,
                    ensure_parent=self.create_folders,
                )
                uploaded += 1
                self._emit(f"uploaded {rel}")
            except Exception as exc:  # keep watching despite individual failures
                self._emit(f"failed {rel}: {exc}")
        self._state = current
        return uploaded

    def run(self) -> None:
        self._state = self._snapshot()
        self._emit(f"watching {self.local} -> {self.remote_root} (every {self.interval:g}s)")
        while not self._stop:
            self.sweep()
            time.sleep(self.interval)

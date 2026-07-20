"""Sync planning between a local directory and a remote DiskBlaze folder.

A sync is computed by enumerating both sides, matching files by their relative
path, and deciding what to upload, download, or delete. ``content_sha256`` is
used (when available) to detect changes without re-transferring identical bytes.
"""

from __future__ import annotations

import fnmatch
import hashlib
from dataclasses import dataclass, field
from pathlib import Path

from .client import DiskBlazeClient, FileNode, join_remote, normalize_remote_path


def _match(name: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(name, pattern) for pattern in patterns)


def _filtered_local_files(root: Path, include: list[str], exclude: list[str]) -> dict[str, Path]:
    """Map relative posix path -> local file path, honoring include/exclude."""
    result: dict[str, Path] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if include and not _match(rel, include) and not _match(path.name, include):
            continue
        if exclude and (_match(rel, exclude) or _match(path.name, exclude)):
            continue
        result[rel] = path
    return result


@dataclass
class SyncPlan:
    to_upload: list[tuple[str, str]] = field(default_factory=list)  # (local_rel, remote_path)
    to_download: list[tuple[str, str]] = field(default_factory=list)  # (remote_path, local_rel)
    to_delete_remote: list[str] = field(default_factory=list)
    to_delete_local: list[str] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not (
            self.to_upload or self.to_download or self.to_delete_remote or self.to_delete_local
        )

    def summarize(self) -> str:
        return (
            f"{len(self.to_upload)} to upload, "
            f"{len(self.to_download)} to download, "
            f"{len(self.to_delete_remote)} to delete remotely, "
            f"{len(self.to_delete_local)} to delete locally"
        )


def _local_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def plan_sync(
    client: DiskBlazeClient,
    local: Path,
    remote: str,
    *,
    two_way: bool = False,
    delete: bool = False,
    checksum: bool = True,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
) -> SyncPlan:
    """Compute the set of operations needed to reconcile ``local`` and ``remote``."""
    include = include or []
    exclude = exclude or []
    remote_root = normalize_remote_path(remote)

    local_files = _filtered_local_files(local, include, exclude)
    remote_nodes = client.list_recursive(remote_root)
    remote_files: dict[str, FileNode] = {
        node.path[len(remote_root.rstrip("/") + "/") :]: node
        for node in remote_nodes
        if node.path != remote_root
    }

    plan = SyncPlan()

    for rel, local_path in local_files.items():
        remote_path = join_remote(remote_root, rel)
        node = remote_files.get(rel)
        changed = node is None
        if node is not None:
            size_differs = node.size_bytes != local_path.stat().st_size
            hash_differs = (
                checksum
                and node.content_sha256
                and node.content_sha256.lower() != _local_sha256(local_path).lower()
            )
            changed = size_differs or hash_differs
        if changed:
            plan.to_upload.append((rel, remote_path))

    for rel, node in remote_files.items():
        if rel in local_files:
            continue
        if two_way:
            local_rel = rel
            plan.to_download.append((node.path, local_rel))
        elif delete:
            plan.to_delete_remote.append(node.path)

    if two_way and delete:
        for rel, local_path in local_files.items():
            if rel not in remote_files:
                plan.to_delete_local.append(str(local_path))

    return plan

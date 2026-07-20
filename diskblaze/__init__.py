"""DiskBlaze — a fast Python client and CLI for the DiskBlaze storage API."""

from __future__ import annotations

from .client import (
    DEFAULT_ENDPOINT,
    CopyJob,
    CurrentUser,
    DiskBlazeClient,
    DiskBlazeError,
    FileNode,
    ShareLink,
    TransferProgress,
    TrashEntry,
    UploadPart,
    UploadPlan,
    endpoint_from_base,
    join_remote,
    normalize_remote_path,
)
from .sync import SyncPlan, plan_sync
from .watch import FolderWatcher

__version__ = "0.1.3"

__all__ = [
    "CopyJob",
    "CurrentUser",
    "DEFAULT_ENDPOINT",
    "DiskBlazeClient",
    "DiskBlazeError",
    "FileNode",
    "FolderWatcher",
    "ShareLink",
    "SyncPlan",
    "TransferProgress",
    "TrashEntry",
    "UploadPart",
    "UploadPlan",
    "endpoint_from_base",
    "join_remote",
    "normalize_remote_path",
    "plan_sync",
    "__version__",
]

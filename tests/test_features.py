from __future__ import annotations

import argparse
import concurrent.futures as cf
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest
import requests
from requests.structures import CaseInsensitiveDict

from diskblaze.cli import QuietProgress, transfer_progress
from diskblaze.client import (
    CurrentUser,
    DiskBlazeClient,
    FileNode,
    TransferProgress,
    UploadPlan,
    _parse_retry_after,
    normalize_remote_path,
)
from diskblaze.sync import plan_sync
from diskblaze.watch import FolderWatcher


class FullClient(DiskBlazeClient):
    """In-memory client exercising downloads, tree uploads, and deletion."""

    def __init__(self):
        super().__init__(token="dummy")
        self.remote: dict[str, FileNode] = {}
        self.downloaded: list[tuple[str, Path]] = []
        self.deleted: list[str] = []
        self.put_calls = 0

    def ensure_folder(self, path: str) -> None:
        return None

    def get_node(self, path: str):
        return self.remote.get(path)

    def list_recursive(self, path: str = "/") -> list[FileNode]:
        return list(self.remote.values())

    def create_upload_plan(self, path, *, size_bytes, content_sha256=None, part_size=None):
        return UploadPlan(
            token=path,
            path=path,
            size_bytes=size_bytes,
            part_size=0,
            upload_id=None,
            put_url="https://upload.invalid/o",
            parts=[],
        )

    def _put_stream(self, url, body, *, length, progress=None):
        self.put_calls += 1
        return "etag"

    def complete_upload(self, token, *, completed_parts=None, content_sha256=None):
        return FileNode(
            id="n",
            name="f",
            path=token,
            parent_path="/",
            is_dir=False,
            size_bytes=0,
            size="0",
            updated_at="",
        )

    def download_url(self, path, *, expires_seconds=3600):
        return f"https://dl.invalid/{path.lstrip('/')}"

    def zip_url(self, path, *, expires_seconds=3600):
        return f"https://zip.invalid/{path.lstrip('/')}"

    def _download_url(self, url, output, *, display_path, workers, progress):
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"downloaded")
        self.downloaded.append((display_path, output))
        return output

    def delete(self, path: str) -> str:
        self.deleted.append(path)
        self.remote.pop(path, None)
        return "deleted"

    def copy(self, src: str, dst: str):
        return type(
            "_CJ",
            (),
            {"source_path": src, "destination_path": dst, "id": "job", "status": "complete"},
        )()

    def trash(self):
        return []

    def restore_trash(self, deletion_id: str) -> str:
        return "restored"

    def purge_trash(self, deletion_id: str) -> str:
        return "purged"

    def share_links(self, path: str):
        return []

    def create_share_link(self, path, *, password=None, instructions=None, expires_hours=None):
        return type(
            "_SL",
            (),
            {
                "id": "link1",
                "name": path,
                "path": path,
                "url": "https://diskblaze.com/share/x",
                "expires_at": None,
            },
        )()

    def revoke_share_link(self, share_id: str) -> str:
        return "revoked"

    def stream(self, path, *, expires_seconds=3600, chunk_size=1024 * 1024):
        yield b"streamed"


def test_download_resume_skips_existing_local_file(tmp_path: Path):
    client = FullClient()
    local = tmp_path / "a.bin"
    local.write_bytes(b"already here")
    node = client.download("/private/a.bin", local, resume=True)
    assert node == local
    assert client.downloaded == []


def test_download_dry_run_no_transfer(tmp_path: Path):
    client = FullClient()
    local = tmp_path / "a.bin"
    node = client.download("/private/a.bin", local, dry_run=True)
    assert client.downloaded == []
    assert node == local


def test_upload_tree_respects_no_create_folders(tmp_path: Path):
    src = tmp_path / "src"
    (src / "sub").mkdir(parents=True)
    (src / "sub" / "x.bin").write_bytes(b"x")
    client = FullClient()
    client.upload_tree(src, "/private/dst", create_folders=False, file_workers=1)
    # No folders created and upload still proceeds for the file's parent via ensure_parent.
    assert client.put_calls == 1


def test_upload_tree_resume_skips_unchanged(tmp_path: Path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "x.bin").write_bytes(b"data")
    client = FullClient()
    client.remote["/private/dst/x.bin"] = FileNode(
        id="r",
        name="x.bin",
        path="/private/dst/x.bin",
        parent_path="/private/dst",
        is_dir=False,
        size_bytes=4,
        size="4",
        updated_at="",
        content_sha256=None,
    )
    client.upload_tree(src, "/private/dst", resume=True, checksum=False, file_workers=1)
    assert client.put_calls == 0


def test_watcher_uploads_new_and_changed_files(tmp_path: Path):
    watch_dir = tmp_path / "w"
    watch_dir.mkdir()
    (watch_dir / "a.txt").write_bytes(b"v1")
    client = FullClient()
    watcher = FolderWatcher(client, watch_dir, "/remote", interval=1, checksum=False)
    watcher.sweep()
    assert client.put_calls == 1

    # Unchanged second sweep should upload nothing.
    client.put_calls = 0
    watcher.sweep()
    assert client.put_calls == 0

    # Modified file triggers another upload.
    (watch_dir / "a.txt").write_bytes(b"v2-changed")
    time.sleep(0.01)
    watcher.sweep()
    assert client.put_calls == 1


def test_watcher_respects_exclude(tmp_path: Path):
    watch_dir = tmp_path / "w"
    watch_dir.mkdir()
    (watch_dir / "keep.txt").write_bytes(b"x")
    (watch_dir / "skip.log").write_bytes(b"y")
    client = FullClient()
    watcher = FolderWatcher(
        client, watch_dir, "/remote", interval=1, checksum=False, exclude=["*.log"]
    )
    watcher.sweep()
    # Only keep.txt uploaded.
    assert client.put_calls == 1


def test_command_rm_dry_run_does_not_delete():
    args = argparse.Namespace(
        path="/private/x",
        yes=False,
        dry_run=True,
        endpoint=None,
        token=None,
        timeout=120.0,
        workers=64,
        file_workers=8,
    )
    client = FullClient()
    import diskblaze.cli as c

    orig = c.build_client
    c.build_client = lambda a: client  # ty: ignore[invalid-assignment]
    try:
        rc = c.command_rm(args)
    finally:
        c.build_client = orig
    assert rc == 0
    assert client.deleted == []


def _ns(**kw):
    base = dict(
        endpoint=None,
        token=None,
        timeout=120.0,
        workers=64,
        file_workers=8,
        verbose=False,
        resume=False,
        dry_run=False,
        no_sha256=False,
        no_create_folders=False,
        include=None,
        exclude=None,
        part_size=None,
        two_way=False,
        delete=False,
        expires=3600,
        interval=5.0,
        local=None,
        remote=None,
        path=None,
        src=None,
        dst=None,
        json=False,
        password=None,
        instructions=None,
        expires_hours=None,
        expire_in=None,
        older_than=None,
        revoke_all=False,
        share_id=None,
        deletion_id=None,
        all=False,
        yes=False,
    )
    base.update(kw)
    return argparse.Namespace(**base)


def test_command_sync_dry_run_only_plans(tmp_path: Path):
    local = tmp_path / "l"
    local.mkdir()
    (local / "a.txt").write_bytes(b"x")
    client = FullClient()

    import diskblaze.cli as c

    orig = c.build_client
    c.build_client = lambda a: client  # ty: ignore[invalid-assignment]
    try:
        rc = c.command_sync(_ns(local=local, remote="/remote", dry_run=True))
    finally:
        c.build_client = orig
    assert rc == 0
    # Dry-run must not perform uploads.
    assert client.put_calls == 0


def test_command_sync_executes_plan(tmp_path: Path):
    local = tmp_path / "l"
    local.mkdir()
    (local / "a.txt").write_bytes(b"x")
    client = FullClient()

    import diskblaze.cli as c

    orig = c.build_client
    c.build_client = lambda a: client  # ty: ignore[invalid-assignment]
    try:
        rc = c.command_sync(_ns(local=local, remote="/remote"))
    finally:
        c.build_client = orig
    assert rc == 0
    assert client.put_calls == 1


def test_command_watch_dry_run_no_upload(tmp_path: Path):
    local = tmp_path / "l"
    local.mkdir()
    (local / "a.txt").write_bytes(b"x")
    client = FullClient()

    import diskblaze.cli as c

    orig = c.build_client
    c.build_client = lambda a: client  # ty: ignore[invalid-assignment]
    try:
        rc = c.command_watch(_ns(local=local, remote="/remote", dry_run=True))
    finally:
        c.build_client = orig
    assert rc == 0
    assert client.put_calls == 0


def test_plan_sync_include_exclude_filters(tmp_path: Path):
    local = tmp_path / "l"
    local.mkdir()
    (local / "keep.txt").write_bytes(b"x")
    (local / "skip.log").write_bytes(b"y")
    (local / "docs").mkdir()
    (local / "docs" / "read.md").write_bytes(b"z")
    client = FullClient()
    plan = plan_sync(client, local, "/remote", checksum=False, include=["*.txt", "docs/*"])
    rels = {rel for rel, _ in plan.to_upload}
    assert "keep.txt" in rels
    assert "docs/read.md" in rels
    assert "skip.log" not in rels


def _run_command(func, **kw):
    args = _ns(**kw)
    client = FullClient()
    import diskblaze.cli as c

    orig = c.build_client
    c.build_client = lambda a: client  # ty: ignore[invalid-assignment]
    try:
        return func(args), client
    finally:
        c.build_client = orig


def _seed_listing_client() -> _ListingClient:
    client = _ListingClient()
    client.remote["/private/a.txt"] = FileNode(
        id="r",
        name="a.txt",
        path="/private/a.txt",
        parent_path="/private",
        is_dir=False,
        size_bytes=100,
        size="100",
        updated_at="t",
    )
    client.remote["/private/b"] = FileNode(
        id="r2",
        name="b",
        path="/private/b",
        parent_path="/private",
        is_dir=True,
        size_bytes=0,
        size="0",
        updated_at="t",
    )
    client.remote["/private/b/c.txt"] = FileNode(
        id="r3",
        name="c.txt",
        path="/private/b/c.txt",
        parent_path="/private/b",
        is_dir=False,
        size_bytes=50,
        size="50",
        updated_at="t",
    )
    return client


def _run_listing(func_name, **kw):
    args = _ns(**kw)
    client = _seed_listing_client()
    import diskblaze.cli as c

    orig = c.build_client
    c.build_client = lambda a: client  # ty: ignore[invalid-assignment]
    try:
        return getattr(c, func_name)(args), client
    finally:
        c.build_client = orig


def test_command_cp_starts_job():
    rc, client = _run_command(
        lambda a: __import__("diskblaze.cli", fromlist=["command_cp"]).command_cp(a),
        src="/private/a",
        dst="/private/b",
    )
    assert rc == 0


def test_command_stat_json(capsys):
    client = FullClient()
    client.remote["/private/a"] = FileNode(
        id="r",
        name="a",
        path="/private/a",
        parent_path="/private",
        is_dir=False,
        size_bytes=4,
        size="4",
        updated_at="t",
    )
    import diskblaze.cli as c

    orig = c.build_client
    c.build_client = lambda a: client  # ty: ignore[invalid-assignment]
    try:
        rc = c.command_stat(_ns(path="/private/a", json=True))
    finally:
        c.build_client = orig
    assert rc == 0
    import json

    out = json.loads(capsys.readouterr().out)
    assert out["path"] == "/private/a"


def test_command_du_json(capsys):
    client = FullClient()
    client.remote["/private/a"] = FileNode(
        id="r",
        name="a",
        path="/private/a",
        parent_path="/private",
        is_dir=False,
        size_bytes=100,
        size="100",
        updated_at="t",
    )
    import diskblaze.cli as c

    orig = c.build_client
    c.build_client = lambda a: client  # ty: ignore[invalid-assignment]
    try:
        rc = c.command_du(_ns(path="/private", json=True))
    finally:
        c.build_client = orig
    assert rc == 0
    import json

    out = json.loads(capsys.readouterr().out)
    assert out["bytes"] == 100


def test_command_trash_json(capsys):
    rc, _ = _run_command(
        lambda a: __import__("diskblaze.cli", fromlist=["command_trash"]).command_trash(a),
        json=True,
    )
    assert rc == 0
    import json

    assert json.loads(capsys.readouterr().out) == []


def test_command_share_reports_url(capsys):
    rc, client = _run_command(
        lambda a: __import__("diskblaze.cli", fromlist=["command_share"]).command_share(a),
        path="/private/a",
    )
    assert rc == 0
    assert "shared" in capsys.readouterr().out


def test_command_revoke(capsys):
    rc, client = _run_command(
        lambda a: __import__("diskblaze.cli", fromlist=["command_revoke"]).command_revoke(a),
        share_id="sid",
    )
    assert rc == 0
    assert "revoked" in capsys.readouterr().out


def test_command_cat_streams_bytes(capsys):
    rc, client = _run_command(
        lambda a: __import__("diskblaze.cli", fromlist=["command_cat"]).command_cat(a),
        path="/private/a",
    )
    assert rc == 0
    assert capsys.readouterr().out == "streamed"


class _ListingClient(FullClient):
    """FullClient variant that serves listing data without network access."""

    def me(self):
        return CurrentUser(
            id="u",
            username="tester",
            quota="1 TB",
            used="10 GB",
            remaining="990 GB",
            quota_bytes=1_000_000_000_000,
            used_bytes=10_000_000_000,
            remaining_bytes=990_000_000_000,
            api_access_enabled=True,
            direct_ul_enabled=False,
        )

    def list_files(self, path: str = "/"):
        parent = normalize_remote_path(path)
        out = []
        for node in self.remote.values():
            node_parent = node.parent_path.rstrip("/") or "/"
            if node_parent == parent:
                out.append(node)
        return sorted(out, key=lambda n: n.name)

    def list_recursive(self, path: str = "/"):
        return list(self.remote.values())

    def trash(self):
        return [
            type(
                "_TE",
                (),
                {
                    "deletion_id": "d1",
                    "name": "gone.txt",
                    "path": "/private/gone.txt",
                    "size_bytes": 10,
                    "deleted_at": "2026-01-01",
                    "expires_at": "2026-02-01",
                },
            )()
        ]

    def share_links(self, path: str):
        return [
            type(
                "_SL",
                (),
                {"name": "x.txt", "url": "https://diskblaze.com/s/x", "expires_at": "2026-03-01"},
            )()
        ]


def test_build_client_wires_retries_and_backoff():
    import diskblaze.cli as c

    captured = {}

    class _Capture(DiskBlazeClient):
        def __init__(self, **kwargs):
            captured.update(kwargs)
            # Skip the network/session setup of the real client.
            self.retries = kwargs.get("retries", 4)
            self.retry_backoff = kwargs.get("retry_backoff", 0.5)

    orig = c.DiskBlazeClient
    c.DiskBlazeClient = _Capture  # ty: ignore[invalid-assignment]
    try:
        c.build_client(
            argparse.Namespace(
                endpoint=None,
                token="dummy",
                timeout=120.0,
                workers=64,
                file_workers=8,
                retries=9,
                backoff=2.5,
            )
        )
    finally:
        c.DiskBlazeClient = orig
    assert captured["retries"] == 9
    assert captured["retry_backoff"] == 2.5


def test_parser_exposes_retries_and_backoff_flags():
    import diskblaze.cli as c

    args = c.build_parser().parse_args(["ls", "/", "--retries", "7", "--backoff", "1.5"])
    assert args.retries == 7
    assert args.backoff == 1.5


def test_parser_exposes_output_flag():
    import diskblaze.cli as c

    args = c.build_parser().parse_args(["ls", "/", "--output", "csv"])
    assert args.output == "csv"


def test_completion_subcommand_emits_bash_script(capsys):
    import diskblaze.cli as c

    rc = c.main(["completion", "bash"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "diskblaze" in out
    assert "_ARGCOMPLETE" in out


def test_completion_subcommand_emits_fish_script(capsys):
    import diskblaze.cli as c

    rc = c.main(["completion", "fish"])
    assert rc == 0
    assert "diskblaze" in capsys.readouterr().out


def test_ls_csv_output(capsys):
    rc, _ = _run_listing("command_ls", path="/private", output="csv")
    assert rc == 0
    import csv as _csv
    from io import StringIO

    reader = list(_csv.reader(StringIO(capsys.readouterr().out)))
    assert reader[0] == ["Name", "Type", "Size", "Modified"]
    names = {row[0] for row in reader[1:]}
    assert "a.txt" in names
    assert "b" in names


def test_ls_legacy_json_flag(capsys):
    rc, _ = _run_listing("command_ls", path="/private", json=True)
    assert rc == 0
    import json

    out = json.loads(capsys.readouterr().out)
    assert any(n["name"] == "a.txt" for n in out)


def test_du_csv_output(capsys):
    rc, _ = _run_listing("command_du", path="/private", output="csv")
    assert rc == 0
    import csv as _csv
    from io import StringIO

    reader = list(_csv.reader(StringIO(capsys.readouterr().out)))
    assert reader[0] == ["Path", "Size"]
    assert reader[-1][0] == "TOTAL"


def test_trash_csv_output(capsys):
    rc, _ = _run_listing("command_trash", output="csv")
    assert rc == 0
    import csv as _csv
    from io import StringIO

    reader = list(_csv.reader(StringIO(capsys.readouterr().out)))
    assert reader[0] == ["Name", "Path", "Size", "Deleted", "Expires"]
    assert reader[1][0] == "gone.txt"


def test_shares_csv_output(capsys):
    rc, _ = _run_listing("command_share_list", path="/private", output="csv")
    assert rc == 0
    import csv as _csv
    from io import StringIO

    reader = list(_csv.reader(StringIO(capsys.readouterr().out)))
    assert reader[0] == ["Name", "URL", "Expires"]
    assert reader[1][0] == "x.txt"


def test_tree_csv_output(capsys):
    rc, _ = _run_listing("command_tree", path="/private", output="csv")
    assert rc == 0
    import csv as _csv
    from io import StringIO

    reader = list(_csv.reader(StringIO(capsys.readouterr().out)))
    assert reader[0] == ["Path", "Depth", "Type", "Size"]


def test_whoami_csv_output(capsys):
    rc, _ = _run_listing("command_whoami", output="csv")
    assert rc == 0
    import csv as _csv
    from io import StringIO

    reader = list(_csv.reader(StringIO(capsys.readouterr().out)))
    assert reader[0] == ["Key", "Value"]


class _FlagClient(DiskBlazeClient):
    """Captures upload_file kwargs so CLI flag wiring can be asserted."""

    def __init__(self):
        super().__init__(token="dummy")
        self.upload_kwargs: list[dict] = []

    def ensure_folder(self, path: str) -> None:
        return None

    def list_recursive(self, path: str = "/"):
        return []

    def get_node(self, path: str):
        return None

    def upload_file(self, local, remote, **kwargs):  # ty: ignore[invalid-method-override]
        self.upload_kwargs.append(kwargs)
        return FileNode(
            id="n",
            name="f",
            path=remote,
            parent_path="/",
            is_dir=False,
            size_bytes=0,
            size="0",
            updated_at="",
        )


def _run_with(client, func_name, **kw):
    args = _ns(**kw)
    import diskblaze.cli as c

    orig = c.build_client
    c.build_client = lambda a: client  # ty: ignore[invalid-assignment]
    try:
        return getattr(c, func_name)(args)
    finally:
        c.build_client = orig


def test_upload_defaults_enable_folders_and_sha256(tmp_path: Path):
    local = tmp_path / "a.bin"
    local.write_bytes(b"data")
    client = _FlagClient()
    rc = _run_with(client, "command_upload", local=str(local), remote="/private/a.bin")
    assert rc == 0
    assert len(client.upload_kwargs) == 1
    kw = client.upload_kwargs[0]
    assert kw["ensure_parent"] is True
    assert kw["checksum"] is True


def test_upload_respects_no_create_folders_and_no_sha256(tmp_path: Path):
    local = tmp_path / "a.bin"
    local.write_bytes(b"data")
    client = _FlagClient()
    rc = _run_with(
        client,
        "command_upload",
        local=str(local),
        remote="/private/a.bin",
        no_create_folders=True,
        no_sha256=True,
    )
    assert rc == 0
    assert len(client.upload_kwargs) == 1
    kw = client.upload_kwargs[0]
    assert kw["ensure_parent"] is False
    assert kw["checksum"] is False


def test_sync_passes_ensure_parent_from_no_create_folders(tmp_path: Path):
    local = tmp_path / "l"
    local.mkdir()
    (local / "a.txt").write_bytes(b"x")
    client = _FlagClient()
    rc = _run_with(client, "command_sync", local=local, remote="/remote", no_create_folders=True)
    assert rc == 0
    assert any(kw.get("ensure_parent") is False for kw in client.upload_kwargs)


def test_parser_no_create_folders_flag():
    import diskblaze.cli as c

    args = c.build_parser().parse_args(["upload", "x", "/p", "--no-create-folders"])
    assert args.no_create_folders is True


def test_parser_no_sha256_flag():
    import diskblaze.cli as c

    args = c.build_parser().parse_args(["upload", "x", "/p", "--no-sha256"])
    assert args.no_sha256 is True


def test_stat_csv_output(capsys):
    rc, _ = _run_listing("command_stat", path="/private/a.txt", output="csv")
    assert rc == 0
    import csv as _csv
    from io import StringIO

    reader = list(_csv.reader(StringIO(capsys.readouterr().out)))
    assert reader[0] == ["Key", "Value"]
    assert ("Name", "a.txt") in [tuple(r) for r in reader[1:]]


def test_stat_legacy_json_flag(capsys):
    rc, _ = _run_listing("command_stat", path="/private/a.txt", json=True)
    assert rc == 0
    import json

    out = json.loads(capsys.readouterr().out)
    assert out["name"] == "a.txt"


def test_share_csv_output(capsys):
    rc, _ = _run_command(
        lambda a: __import__("diskblaze.cli", fromlist=["command_share"]).command_share(a),
        path="/private/a",
        output="csv",
    )
    assert rc == 0
    import csv as _csv
    from io import StringIO

    reader = list(_csv.reader(StringIO(capsys.readouterr().out)))
    assert reader[0] == ["Name", "Path", "URL", "Expires"]
    assert reader[1][0] == "/private/a"


def test_share_expire_in_converts_to_hours(capsys):
    import diskblaze.cli as c

    captured = {}

    class _CaptureShare(FullClient):
        def create_share_link(self, path, *, password=None, instructions=None, expires_hours=None):
            captured["expires_hours"] = expires_hours
            return super().create_share_link(
                path, password=password, instructions=instructions, expires_hours=expires_hours
            )

    client = _CaptureShare()
    args = _ns(path="/private/a", expire_in="7d")
    orig = c.build_client
    c.build_client = lambda a: client  # ty: ignore[invalid-assignment]
    try:
        rc = c.command_share(args)
    finally:
        c.build_client = orig
    assert rc == 0
    assert captured["expires_hours"] == 168


def test_shares_revoke_all(capsys):
    import diskblaze.cli as c

    class _RevokeClient(FullClient):
        def __init__(self):
            super().__init__()
            self.revoked: list[str] = []

        def share_links(self, path):
            return [type("_SL", (), {"id": "s1", "name": "x", "url": "u", "expires_at": None})()]

        def revoke_share_link(self, share_id):
            self.revoked.append(share_id)
            return "revoked"

    client = _RevokeClient()
    args = _ns(path="/private", revoke_all=True)
    orig = c.build_client
    c.build_client = lambda a: client  # ty: ignore[invalid-assignment]
    try:
        rc = c.command_share_list(args)
    finally:
        c.build_client = orig
    assert rc == 0
    assert client.revoked == ["s1"]


def test_rm_dry_run_no_delete(capsys):
    rc, client = _run_command(
        lambda a: __import__("diskblaze.cli", fromlist=["command_rm"]).command_rm(a),
        path="/private/a",
        dry_run=True,
    )
    assert rc == 0
    assert client.deleted == []


def test_cp_dry_run_no_copy(capsys):
    rc, client = _run_command(
        lambda a: __import__("diskblaze.cli", fromlist=["command_cp"]).command_cp(a),
        src="/private/a",
        dst="/private/b",
        dry_run=True,
    )
    assert rc == 0
    assert "dry-run" in capsys.readouterr().out


def test_trash_older_than_filters_old_entries(capsys):
    rc, _ = _run_listing("command_trash", older_than="10y", output="csv")
    assert rc == 0
    import csv as _csv
    from io import StringIO

    reader = list(_csv.reader(StringIO(capsys.readouterr().out)))
    assert len(reader) == 1


def test_trash_older_than_keeps_recent_entries(capsys):
    rc, _ = _run_listing("command_trash", older_than="1d", output="csv")
    assert rc == 0
    import csv as _csv
    from io import StringIO

    reader = list(_csv.reader(StringIO(capsys.readouterr().out)))
    assert len(reader) == 2


class _FakeResponse(requests.Response):
    def __init__(self, status, headers=None, payload=None):
        super().__init__()
        self.status_code = status
        self.headers = CaseInsensitiveDict(headers or {})
        self._payload = payload or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def json(self, **kwargs):
        return self._payload


class _FakeSession(requests.Session):
    def __init__(self, responses):
        super().__init__()
        self._responses = list(responses)

    def post(self, *args, **kwargs):
        return self._responses.pop(0)


class _RetryClient(DiskBlazeClient):
    def __init__(self, responses):
        super().__init__(token="dummy")
        self._fake = _FakeSession(responses)

    def _session(self):
        return self._fake


def test_parse_retry_after():
    assert _parse_retry_after("30") == 30.0
    assert _parse_retry_after("") is None
    assert _parse_retry_after(None) is None
    assert _parse_retry_after("not-a-date") is None
    future = (datetime.now(timezone.utc) + timedelta(seconds=45)).strftime(
        "%a, %d %b %Y %H:%M:%S GMT"
    )
    value = _parse_retry_after(future)
    assert value is not None and 40.0 <= value <= 50.0


def test_retry_delay_caps_exponential_and_respects_retry_after():
    client = DiskBlazeClient(token="dummy", retry_backoff=0.5, backoff_cap=60.0)
    assert client._retry_delay(10, None) == 60.0
    assert client._retry_delay(0, 120.0) == 120.0
    assert client._retry_delay(5, 1.0) == 16.0


def test_graphql_retries_on_429_and_honors_retry_after():
    client = _RetryClient(
        [
            _FakeResponse(429, {"Retry-After": "2"}),
            _FakeResponse(429, {"Retry-After": "3"}),
            _FakeResponse(200, {}, {"data": {"ok": True}}),
        ]
    )
    sleeps = []
    with mock.patch("time.sleep", sleeps.append):
        data = client.graphql("query { ok }")
    assert data == {"ok": True}
    assert sleeps == [2.0, 3.0]


def test_graphql_surfaces_permanent_error_without_retry():
    client = _RetryClient([_FakeResponse(403, {})])
    with mock.patch("time.sleep", lambda _s: None), pytest.raises(requests.HTTPError):
        client.graphql("query { ok }")


class _SlowSession(requests.Session):
    def __init__(self, holder):
        super().__init__()
        self._holder = holder

    def post(self, *args, **kwargs):
        with self._holder.lock:
            self._holder.current += 1
            self._holder.peak = max(self._holder.peak, self._holder.current)
        time.sleep(0.05)
        with self._holder.lock:
            self._holder.current -= 1
        return _FakeResponse(200, {}, {"data": {"ok": True}})


class _Holder:
    def __init__(self):
        self.lock = threading.Lock()
        self.current = 0
        self.peak = 0


class _SlowClient(DiskBlazeClient):
    def __init__(self):
        super().__init__(token="dummy", graphql_concurrency=2)
        self._holder = _Holder()
        self._slow = _SlowSession(self._holder)

    def _session(self):
        return self._slow


def test_graphql_concurrency_is_bounded():
    client = _SlowClient()
    assert client._graphql_sem._value == 2
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(lambda _: client.graphql("query { ok }"), range(12)))
    assert client._holder.peak <= 2


def test_transfer_progress_disable_flag():
    assert transfer_progress(disable=True).disable is True
    assert transfer_progress().disable is False


def test_quiet_progress_emits_one_line_per_phase(capsys):
    qp = QuietProgress()
    qp(TransferProgress("/a.mp3", 0, 100, "uploading", 0))
    qp(TransferProgress("/a.mp3", 100, 100, "completing", 0))
    qp(TransferProgress("/a.mp3", 100, 100, "completing", 0))  # dedupe
    qp(TransferProgress("/b.mp3", 0, 50, "uploading", 0))
    err = capsys.readouterr().err
    assert "uploading" in err and "completing" in err
    assert err.count("completing") == 1


def test_upload_single_file_appends_name_into_existing_dir(tmp_path):
    import diskblaze.cli as c

    class _Up(DiskBlazeClient):
        def __init__(self):
            super().__init__(token="dummy")
            self.recorded = None

        def get_node(self, path):
            if path == "/public/SpaceGhostPurrp":
                return FileNode(
                    id="d",
                    name="SpaceGhostPurrp",
                    path="/public/SpaceGhostPurrp",
                    parent_path="/public",
                    is_dir=True,
                    size_bytes=0,
                    size="0",
                    updated_at="",
                )
            return None

        def upload_file(self, local_path, remote_path, **kwargs):
            self.recorded = remote_path
            return FileNode(
                id="f",
                name="f",
                path=remote_path,
                parent_path="/public",
                is_dir=False,
                size_bytes=0,
                size="0",
                updated_at="",
            )

    client = _Up()
    local = tmp_path / "song.mp3"
    local.write_bytes(b"x")
    args = _ns(
        local=str(local),
        remote="/public/SpaceGhostPurrp",
        no_create_folders=True,
        no_sha256=True,
    )
    orig = c.build_client
    c.build_client = lambda a: client  # ty: ignore[invalid-assignment]
    try:
        rc = c.command_upload(args)
    finally:
        c.build_client = orig
    assert rc == 0
    assert client.recorded == "/public/SpaceGhostPurrp/song.mp3"

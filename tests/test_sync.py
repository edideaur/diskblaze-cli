from __future__ import annotations

import hashlib
from pathlib import Path

from diskblaze import sync as sync_mod
from diskblaze.client import DiskBlazeClient, FileNode, UploadPlan


class FakeClient(DiskBlazeClient):
    """In-memory client for exercising upload resume and dry-run behavior."""

    def __init__(self):
        super().__init__(token="dummy")
        self.remote: dict[str, FileNode] = {}
        self.put_calls: list[str] = []
        self.completed: list[str] = []

    def ensure_folder(self, path: str) -> None:
        return None

    def get_node(self, path: str):
        return self.remote.get(path)

    def create_upload_plan(
        self,
        path: str,
        *,
        size_bytes: int,
        content_sha256: str | None = None,
        part_size: int | None = None,
    ):
        return UploadPlan(
            token="tok",
            path=path,
            size_bytes=size_bytes,
            part_size=0,
            upload_id=None,
            put_url="https://upload.invalid/o",
            parts=[],
        )

    def _put_stream(self, url, body, *, length, progress=None):
        data = b"".join(body)
        assert len(data) == length
        self.put_calls.append(url)
        return "etag"

    def complete_upload(self, token, *, completed_parts=None, content_sha256=None):
        self.completed.append(token)
        return FileNode(
            id="n",
            name="f",
            path=token,
            parent_path="/",
            is_dir=False,
            size_bytes=0,
            size="0",
            updated_at="now",
            content_sha256=content_sha256,
        )


def test_upload_resume_skips_unchanged_file(tmp_path: Path):
    local = tmp_path / "a.bin"
    local.write_bytes(b"hello diskblaze")
    sha = hashlib.sha256(local.read_bytes()).hexdigest()
    client = FakeClient()
    client.remote["/private/a.bin"] = FileNode(
        id="x",
        name="a.bin",
        path="/private/a.bin",
        parent_path="/private",
        is_dir=False,
        size_bytes=len(local.read_bytes()),
        size="",
        updated_at="",
        content_sha256=sha,
    )

    node = client.upload_file(local, "/private/a.bin", checksum=True, resume=True)

    assert client.put_calls == []
    assert node.content_sha256 == sha


def test_upload_resume_reuploads_on_size_mismatch(tmp_path: Path):
    local = tmp_path / "a.bin"
    local.write_bytes(b"hello diskblaze")
    client = FakeClient()
    client.remote["/private/a.bin"] = FileNode(
        id="x",
        name="a.bin",
        path="/private/a.bin",
        parent_path="/private",
        is_dir=False,
        size_bytes=999,
        size="",
        updated_at="",
        content_sha256="deadbeef",
    )

    client.upload_file(local, "/private/a.bin", checksum=True, resume=True)

    assert client.put_calls == ["https://upload.invalid/o"]


def test_upload_dry_run_performs_no_transfer(tmp_path: Path):
    local = tmp_path / "a.bin"
    local.write_bytes(b"hello")
    client = FakeClient()

    node = client.upload_file(local, "/private/a.bin", dry_run=True)

    assert client.put_calls == []
    assert node.path == "/private/a.bin"


def test_list_recursive_returns_only_files(tmp_path: Path):
    files = {
        "/a/b.txt": FileNode(
            id="1",
            name="b.txt",
            path="/a/b.txt",
            parent_path="/a",
            is_dir=False,
            size_bytes=1,
            size="1",
            updated_at="",
        ),
        "/a": FileNode(
            id="2",
            name="a",
            path="/a",
            parent_path="/",
            is_dir=True,
            size_bytes=0,
            size="",
            updated_at="",
        ),
    }

    class ListClient(FakeClient):
        def list_files(self, path: str = "/") -> list[FileNode]:
            if path == "/":
                return [files["/a"]]
            if path == "/a":
                return [files["/a/b.txt"]]
            return []

    client = ListClient()
    result = client.list_recursive("/")
    assert [n.path for n in result] == ["/a/b.txt"]


def _remote_node(rel: str, size: int, sha: str | None) -> FileNode:
    return FileNode(
        id="r",
        name=rel,
        path=f"/remote/{rel}",
        parent_path="/remote",
        is_dir=False,
        size_bytes=size,
        size=str(size),
        updated_at="",
        content_sha256=sha,
    )


class SyncClient(FakeClient):
    def __init__(self, remote_nodes):
        super().__init__()
        self._nodes = remote_nodes

    def list_recursive(self, path: str = "/") -> list[FileNode]:
        return list(self._nodes)


def test_plan_sync_detects_new_local_file(tmp_path: Path):
    (tmp_path / "new.txt").write_bytes(b"data")
    client = SyncClient([])
    plan = sync_mod.plan_sync(client, tmp_path, "/remote", checksum=False)
    assert plan.to_upload == [("new.txt", "/remote/new.txt")]
    assert plan.empty is False


def test_plan_sync_skips_identical_remote_file(tmp_path: Path):
    content = b"same"
    (tmp_path / "same.txt").write_bytes(content)
    sha = hashlib.sha256(content).hexdigest()
    client = SyncClient([_remote_node("same.txt", len(content), sha)])
    plan = sync_mod.plan_sync(client, tmp_path, "/remote", checksum=True)
    assert plan.to_upload == []
    assert plan.empty


def test_plan_sync_two_way_downloads_missing_local(tmp_path: Path):
    (tmp_path / "keep.txt").write_bytes(b"x")
    client = SyncClient(
        [_remote_node("keep.txt", 1, None), _remote_node("only_remote.txt", 1, None)]
    )
    plan = sync_mod.plan_sync(client, tmp_path, "/remote", two_way=True, checksum=False)
    assert ("/remote/only_remote.txt", "only_remote.txt") in plan.to_download


def test_plan_sync_delete_remote_extras(tmp_path: Path):
    (tmp_path / "keep.txt").write_bytes(b"x")
    client = SyncClient([_remote_node("keep.txt", 1, None), _remote_node("orphan.txt", 1, None)])
    plan = sync_mod.plan_sync(client, tmp_path, "/remote", delete=True, checksum=False)
    assert plan.to_delete_remote == ["/remote/orphan.txt"]

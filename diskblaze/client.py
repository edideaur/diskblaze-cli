from __future__ import annotations

import hashlib
import os
import posixpath
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter


DEFAULT_ENDPOINT = "https://diskblaze.com/graphql"
MiB = 1024 * 1024


class DiskBlazeError(RuntimeError):
    pass


@dataclass(frozen=True)
class FileNode:
    id: str
    name: str
    path: str
    parent_path: str
    is_dir: bool
    size_bytes: int
    size: str
    updated_at: str
    readonly: bool = False
    content_sha256: str | None = None


@dataclass(frozen=True)
class CurrentUser:
    id: str
    username: str
    quota: str
    used: str
    remaining: str
    quota_bytes: int
    used_bytes: int
    remaining_bytes: int
    api_access_enabled: bool
    direct_ul_enabled: bool


@dataclass(frozen=True)
class UploadPart:
    number: int
    start: int
    end: int
    url: str


@dataclass(frozen=True)
class UploadPlan:
    token: str
    path: str
    size_bytes: int
    part_size: int
    upload_id: str | None
    put_url: str | None
    parts: list[UploadPart]


@dataclass(frozen=True)
class TransferProgress:
    path: str
    transferred_bytes: int
    total_bytes: int
    phase: str
    speed_bps: float


ProgressCallback = Callable[[TransferProgress], None]


CREATE_UPLOAD_PLAN = """
mutation CreateUploadPlan($path: String!, $sizeBytes: ID!, $contentSha256: String, $partSize: Int) {
  createUploadPlan(path: $path, sizeBytes: $sizeBytes, contentSha256: $contentSha256, partSize: $partSize) {
    token
    path
    sizeBytes
    partSize
    uploadId
    putUrl
    parts { number start end url }
  }
}
"""

COMPLETE_UPLOAD = """
mutation CompleteUpload($token: String!, $completedParts: [CompletedUploadPartInput!], $contentSha256: String) {
  completeUpload(token: $token, completedParts: $completedParts, contentSha256: $contentSha256) {
    id
    name
    path
    parentPath
    isDir
    sizeBytes
    size
    updatedAt
    readonly
    contentSha256
  }
}
"""

DOWNLOAD_URL = """
query DownloadUrl($path: String!, $expiresSeconds: Int) {
  downloadUrl(path: $path, expiresSeconds: $expiresSeconds) { url expiresSeconds }
}
"""

ZIP_URL = """
query ZipUrl($path: String!, $expiresSeconds: Int) {
  zipUrl(path: $path, expiresSeconds: $expiresSeconds) { url expiresSeconds }
}
"""

CREATE_FOLDER = """
mutation CreateFolder($path: String!) {
  createFolder(path: $path) { id name path parentPath isDir sizeBytes size updatedAt readonly contentSha256 }
}
"""

FILES = """
query Files($path: String!) {
  files(path: $path) {
    path
    parent
    items { id name path parentPath isDir sizeBytes size updatedAt readonly contentSha256 }
  }
}
"""

SEARCH_FILES = """
query SearchFiles(
  $query: String!
  $pathPrefix: String!
  $kind: String
  $minSizeBytes: ID
  $maxSizeBytes: ID
  $updatedAfter: String
  $updatedBefore: String
  $limit: Int
  $offset: Int
) {
  searchFiles(
    query: $query
    pathPrefix: $pathPrefix
    kind: $kind
    minSizeBytes: $minSizeBytes
    maxSizeBytes: $maxSizeBytes
    updatedAfter: $updatedAfter
    updatedBefore: $updatedBefore
    limit: $limit
    offset: $offset
  ) {
    query
    pathPrefix
    limit
    offset
    hasMore
    items { id name path parentPath isDir sizeBytes size updatedAt readonly contentSha256 }
  }
}
"""

MOVE_PATH = """
mutation MovePath($src: String!, $dst: String!) {
  movePath(src: $src, dst: $dst) {
    id name path parentPath isDir sizeBytes size updatedAt readonly contentSha256
  }
}
"""

DELETE_PATH = """
mutation DeletePath($path: String!) {
  deletePath(path: $path) { ok message }
}
"""

ME = """
query Me {
  me {
    id
    username
    quota
    used
    remaining
    quotaBytes
    usedBytes
    remainingBytes
    apiAccessEnabled
    directUlEnabled
  }
}
"""


def normalize_remote_path(path: str) -> str:
    value = "/" + str(path or "/").strip().lstrip("/")
    normalized = posixpath.normpath(value)
    return "/" if normalized == "." else normalized


def join_remote(parent: str, name: str) -> str:
    base = normalize_remote_path(parent)
    clean_name = str(name).replace("\\", "/").strip("/")
    return normalize_remote_path(posixpath.join(base, clean_name))


def preferred_part_size(size: int) -> int | None:
    if size >= 8 * 1024 * MiB:
        return 256 * MiB
    if size >= 1024 * MiB:
        return 128 * MiB
    if size >= 64 * MiB:
        return 64 * MiB
    return None


def _node_from_payload(data: dict) -> FileNode:
    return FileNode(
        id=str(data["id"]),
        name=str(data["name"]),
        path=str(data["path"]),
        parent_path=str(data.get("parentPath") or data.get("parent_path") or ""),
        is_dir=bool(data.get("isDir")),
        size_bytes=int(data.get("sizeBytes") or 0),
        size=str(data.get("size") or ""),
        updated_at=str(data.get("updatedAt") or ""),
        readonly=bool(data.get("readonly")),
        content_sha256=data.get("contentSha256"),
    )


def _user_from_payload(data: dict) -> CurrentUser:
    return CurrentUser(
        id=str(data["id"]),
        username=str(data["username"]),
        quota=str(data.get("quota") or ""),
        used=str(data.get("used") or ""),
        remaining=str(data.get("remaining") or ""),
        quota_bytes=int(data.get("quotaBytes") or 0),
        used_bytes=int(data.get("usedBytes") or 0),
        remaining_bytes=int(data.get("remainingBytes") or 0),
        api_access_enabled=bool(data.get("apiAccessEnabled")),
        direct_ul_enabled=bool(data.get("directUlEnabled")),
    )


class _ProgressReader:
    def __init__(
        self,
        handle,
        *,
        length: int,
        offset: int,
        callback: Callable[[int], None] | None,
        chunk_size: int = 1024 * 1024,
    ):
        self.handle = handle
        self.remaining = int(length)
        self.callback = callback
        self.chunk_size = int(chunk_size)
        self.lock = threading.Lock()
        handle.seek(int(offset))

    def __len__(self) -> int:
        return self.remaining

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        if self.remaining <= 0:
            raise StopIteration
        chunk = self.handle.read(min(self.chunk_size, self.remaining))
        if not chunk:
            raise StopIteration
        self.remaining -= len(chunk)
        if self.callback:
            self.callback(len(chunk))
        return chunk


class DiskBlazeClient:
    """Small high-throughput Python client for DiskBlaze GraphQL + gateway URLs."""

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        token: str | None = None,
        timeout: float = 120.0,
        pool_size: int = 64,
    ):
        self.endpoint = (endpoint or os.environ.get("DISKBLAZE_GQL_URL") or DEFAULT_ENDPOINT).rstrip("/")
        self.token = token or os.environ.get("DISKBLAZE_TOKEN") or os.environ.get("DISKBLAZE_API_KEY")
        if not self.token:
            raise DiskBlazeError("DISKBLAZE_TOKEN or DISKBLAZE_API_KEY is required")
        self.timeout = float(timeout)
        self.pool_size = int(pool_size)
        self._headers = {"Authorization": f"Bearer {self.token}"}
        self._local = threading.local()
        self.session = self._new_session()

    def _new_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(self._headers)
        adapter = HTTPAdapter(
            pool_connections=self.pool_size,
            pool_maxsize=self.pool_size,
            max_retries=0,
            pool_block=False,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = self._new_session()
            self._local.session = session
        return session

    @retry(
        retry=retry_if_exception_type((requests.RequestException, DiskBlazeError)),
        wait=wait_exponential_jitter(initial=0.5, max=8),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def graphql(self, query: str, variables: dict | None = None) -> dict:
        response = self._session().post(
            self.endpoint,
            json={"query": query, "variables": variables or {}},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            message = payload["errors"][0].get("message") if isinstance(payload["errors"], list) else str(payload["errors"])
            raise DiskBlazeError(message or "GraphQL request failed")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise DiskBlazeError("GraphQL response did not include data")
        return data

    def list_files(self, path: str = "/") -> list[FileNode]:
        data = self.graphql(FILES, {"path": normalize_remote_path(path)})
        return [_node_from_payload(item) for item in data["files"]["items"]]

    def me(self) -> CurrentUser:
        data = self.graphql(ME)
        return _user_from_payload(data["me"])

    def search_files(
        self,
        query: str,
        *,
        path_prefix: str | None = None,
        kind: str | None = None,
        min_size_bytes: int | None = None,
        max_size_bytes: int | None = None,
        updated_after: str | None = None,
        updated_before: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> tuple[list[FileNode], bool]:
        data = self.graphql(
            SEARCH_FILES,
            {
                "query": str(query),
                "pathPrefix": normalize_remote_path(path_prefix or "/"),
                "kind": kind,
                "minSizeBytes": str(int(min_size_bytes)) if min_size_bytes is not None else None,
                "maxSizeBytes": str(int(max_size_bytes)) if max_size_bytes is not None else None,
                "updatedAfter": updated_after,
                "updatedBefore": updated_before,
                "limit": int(limit),
                "offset": int(offset),
            },
        )
        result = data["searchFiles"]
        return [_node_from_payload(item) for item in result["items"]], bool(result.get("hasMore"))

    def create_folder(self, path: str) -> FileNode:
        data = self.graphql(CREATE_FOLDER, {"path": normalize_remote_path(path)})
        return _node_from_payload(data["createFolder"])

    def ensure_folder(self, path: str) -> None:
        normalized = normalize_remote_path(path)
        if normalized in {"/", "/private", "/public", "/inbox", "/shared"}:
            return
        current = ""
        for part in normalized.strip("/").split("/"):
            current = f"{current}/{part}"
            try:
                self.create_folder(current)
            except Exception:
                pass

    def move(self, src: str, dst: str) -> FileNode:
        data = self.graphql(
            MOVE_PATH,
            {"src": normalize_remote_path(src), "dst": normalize_remote_path(dst)},
        )
        return _node_from_payload(data["movePath"])

    def delete(self, path: str) -> str:
        data = self.graphql(DELETE_PATH, {"path": normalize_remote_path(path)})
        payload = data["deletePath"]
        if not payload.get("ok"):
            raise DiskBlazeError(str(payload.get("message") or "delete failed"))
        return str(payload.get("message") or "deleted")

    def create_upload_plan(
        self,
        path: str,
        *,
        size_bytes: int,
        content_sha256: str | None = None,
        part_size: int | None = None,
    ) -> UploadPlan:
        data = self.graphql(
            CREATE_UPLOAD_PLAN,
            {
                "path": normalize_remote_path(path),
                "sizeBytes": str(int(size_bytes)),
                "contentSha256": content_sha256,
                "partSize": part_size or preferred_part_size(int(size_bytes)),
            },
        )
        raw = data["createUploadPlan"]
        return UploadPlan(
            token=str(raw["token"]),
            path=str(raw["path"]),
            size_bytes=int(raw["sizeBytes"]),
            part_size=int(raw["partSize"] or 0),
            upload_id=raw.get("uploadId"),
            put_url=raw.get("putUrl"),
            parts=[
                UploadPart(
                    number=int(part["number"]),
                    start=int(part["start"]),
                    end=int(part["end"]),
                    url=str(part["url"]),
                )
                for part in (raw.get("parts") or [])
            ],
        )

    def complete_upload(
        self,
        token: str,
        *,
        completed_parts: list[dict] | None = None,
        content_sha256: str | None = None,
    ) -> FileNode:
        variables = {
            "token": token,
            "completedParts": completed_parts,
            "contentSha256": content_sha256,
        }
        data = self.graphql(COMPLETE_UPLOAD, variables)
        return _node_from_payload(data["completeUpload"])

    def upload_file(
        self,
        local_path: str | Path,
        remote_path: str,
        *,
        workers: int = 8,
        part_size: int | None = None,
        checksum: bool = False,
        ensure_parent: bool = True,
        progress: ProgressCallback | None = None,
    ) -> FileNode:
        path = Path(local_path)
        size = path.stat().st_size
        remote = normalize_remote_path(remote_path)
        parent = posixpath.dirname(remote)
        if ensure_parent and parent and parent != "/":
            self.ensure_folder(parent)
        sha256 = self.sha256(path, progress_path=remote, total=size, progress=progress) if checksum else None
        plan = self.create_upload_plan(remote, size_bytes=size, content_sha256=sha256, part_size=part_size)
        started = time.monotonic()
        transferred = 0
        lock = threading.Lock()

        def report_absolute(absolute: int, phase: str = "uploading") -> None:
            nonlocal transferred
            if not progress:
                return
            with lock:
                transferred = max(0, min(int(absolute), size))
                elapsed = max(time.monotonic() - started, 0.001)
                progress(TransferProgress(remote, transferred, size, phase, transferred / elapsed))

        def bump(delta: int, phase: str = "uploading") -> None:
            report_absolute(transferred + int(delta), phase)

        if plan.put_url:
            for attempt in range(4):
                try:
                    report_absolute(0)
                    with path.open("rb") as handle:
                        reader = _ProgressReader(handle, length=size, offset=0, callback=lambda n: bump(n))
                        self._put_stream(plan.put_url, reader, length=size)
                    break
                except requests.RequestException:
                    if attempt == 3:
                        raise
                    time.sleep(min(8.0, 0.5 * (2 ** attempt)))
            progress and progress(TransferProgress(remote, size, size, "completing", 0))
            return self.complete_upload(plan.token, content_sha256=sha256 or None)

        if not plan.parts:
            raise DiskBlazeError("upload plan did not include a PUT URL or multipart parts")
        completed: list[dict] = []
        part_progress: dict[int, int] = {}

        def bump_part(part_number: int, loaded: int) -> None:
            if not progress:
                return
            with lock:
                part_progress[int(part_number)] = max(0, int(loaded))
                total = min(size, sum(part_progress.values()))
                elapsed = max(time.monotonic() - started, 0.001)
                progress(TransferProgress(remote, total, size, "uploading", total / elapsed))

        max_workers = max(1, min(int(workers), len(plan.parts)))
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="diskblaze-upload") as executor:
            futures = {
                executor.submit(self._upload_part, path, part, bump_part): part
                for part in plan.parts
            }
            for future in as_completed(futures):
                part = futures[future]
                try:
                    completed.append(future.result())
                except Exception as exc:
                    raise DiskBlazeError(f"part {part.number} failed: {exc}") from exc
        completed.sort(key=lambda item: int(item["number"]))
        progress and progress(TransferProgress(remote, size, size, "completing", 0))
        return self.complete_upload(plan.token, completed_parts=completed, content_sha256=sha256 or None)

    def upload_tree(
        self,
        local_path: str | Path,
        remote_dir: str,
        *,
        workers: int = 8,
        file_workers: int = 2,
        checksum: bool = False,
        progress: ProgressCallback | None = None,
    ) -> list[FileNode]:
        root = Path(local_path)
        if root.is_file():
            return [self.upload_file(root, join_remote(remote_dir, root.name), workers=workers, checksum=checksum, progress=progress)]
        files = [path for path in root.rglob("*") if path.is_file()]
        dirs = {normalize_remote_path(remote_dir)}
        for dir_path in (path for path in root.rglob("*") if path.is_dir()):
            dirs.add(join_remote(remote_dir, dir_path.relative_to(root).as_posix()))
        for file_path in files:
            parent = file_path.relative_to(root).parent.as_posix()
            if parent and parent != ".":
                dirs.add(join_remote(remote_dir, parent))
        for remote_folder in sorted(dirs, key=lambda item: item.count("/")):
            self.ensure_folder(remote_folder)
        results: list[FileNode] = []
        executor = ThreadPoolExecutor(max_workers=max(1, int(file_workers)), thread_name_prefix="diskblaze-file")
        failed = False
        try:
            futures = {}
            for file_path in files:
                rel = file_path.relative_to(root).as_posix()
                remote_path = join_remote(remote_dir, rel)
                futures[
                    executor.submit(
                        self.upload_file,
                        file_path,
                        remote_path,
                        workers=workers,
                        checksum=checksum,
                        ensure_parent=False,
                        progress=progress,
                    )
                ] = file_path
            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    failed = True
                    for pending in futures:
                        pending.cancel()
                    raise DiskBlazeError(f"upload failed for {file_path}: {exc}") from exc
        finally:
            executor.shutdown(wait=not failed, cancel_futures=failed)
        return results

    def download_url(self, path: str, *, expires_seconds: int = 3600) -> str:
        data = self.graphql(DOWNLOAD_URL, {"path": normalize_remote_path(path), "expiresSeconds": int(expires_seconds)})
        return str(data["downloadUrl"]["url"])

    def zip_url(self, path: str, *, expires_seconds: int = 3600) -> str:
        data = self.graphql(ZIP_URL, {"path": normalize_remote_path(path), "expiresSeconds": int(expires_seconds)})
        return str(data["zipUrl"]["url"])

    def download(
        self,
        remote_path: str,
        local_path: str | Path,
        *,
        workers: int = 8,
        expires_seconds: int = 3600,
        as_zip: bool | None = None,
        progress: ProgressCallback | None = None,
    ) -> Path:
        remote = normalize_remote_path(remote_path)
        output = Path(local_path)
        if as_zip is None:
            as_zip = output.suffix.lower() == ".zip"
        url = self.zip_url(remote, expires_seconds=expires_seconds) if as_zip else self.download_url(remote, expires_seconds=expires_seconds)
        if output.is_dir() or str(local_path).endswith(os.sep):
            name = posixpath.basename(remote.rstrip("/")) or "download"
            if as_zip and not name.endswith(".zip"):
                name += ".zip"
            output = output / name
        output.parent.mkdir(parents=True, exist_ok=True)
        return self._download_url(url, output, display_path=remote, workers=workers, progress=progress)

    def download_tree(
        self,
        remote_dir: str,
        local_dir: str | Path,
        *,
        workers: int = 16,
        file_workers: int = 8,
        expires_seconds: int = 3600,
        progress: ProgressCallback | None = None,
    ) -> list[Path]:
        """Recursively download a remote folder as normal local files.

        ZIP downloads are simpler, but this path lets fast clients saturate a
        link with multiple files and ranged downloads while preserving the tree.
        """
        root_remote = normalize_remote_path(remote_dir)
        root_local = Path(local_dir)
        root_local.mkdir(parents=True, exist_ok=True)
        files: list[FileNode] = []
        stack = [root_remote]
        while stack:
            folder = stack.pop()
            for node in self.list_files(folder):
                if node.is_dir:
                    stack.append(node.path)
                else:
                    files.append(node)

        results: list[Path] = []
        with ThreadPoolExecutor(max_workers=max(1, int(file_workers)), thread_name_prefix="diskblaze-dl-file") as executor:
            futures = {}
            for node in files:
                rel = node.path[len(root_remote.rstrip("/") + "/") :] if node.path != root_remote else node.name
                output = root_local / rel
                futures[
                    executor.submit(
                        self.download,
                        node.path,
                        output,
                        workers=workers,
                        expires_seconds=expires_seconds,
                        as_zip=False,
                        progress=progress,
                    )
                ] = node
            for future in as_completed(futures):
                node = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    raise DiskBlazeError(f"download failed for {node.path}: {exc}") from exc
        return results

    def _download_url(
        self,
        url: str,
        output: Path,
        *,
        display_path: str,
        workers: int,
        progress: ProgressCallback | None,
    ) -> Path:
        size = 0
        accepts_ranges = False
        try:
            probe = self._session().head(url, allow_redirects=True, timeout=self.timeout)
            if probe.status_code < 400:
                size = int(probe.headers.get("Content-Length") or 0)
                accepts_ranges = probe.headers.get("Accept-Ranges", "").lower() == "bytes"
            elif probe.status_code not in {405, 501}:
                probe.raise_for_status()
        except requests.RequestException:
            size = 0
            accepts_ranges = False
        if not size:
            range_probe = None
            try:
                range_probe = self._session().get(
                    url,
                    headers={"Range": "bytes=0-0"},
                    stream=True,
                    allow_redirects=True,
                    timeout=self.timeout,
                )
                if range_probe.status_code == 206:
                    content_range = range_probe.headers.get("Content-Range", "")
                    if "/" in content_range:
                        size = int(content_range.rsplit("/", 1)[1])
                    accepts_ranges = True
                elif range_probe.status_code < 400:
                    size = int(range_probe.headers.get("Content-Length") or 0)
                else:
                    range_probe.raise_for_status()
            finally:
                if range_probe is not None:
                    try:
                        range_probe.close()
                    except Exception:
                        pass
        ranges = accepts_ranges and size > 8 * MiB
        started = time.monotonic()
        transferred = 0
        lock = threading.Lock()

        def bump(delta: int) -> None:
            nonlocal transferred
            if not progress:
                return
            with lock:
                transferred += delta
                elapsed = max(time.monotonic() - started, 0.001)
                progress(TransferProgress(display_path, transferred, size, "downloading", transferred / elapsed))

        if ranges and workers > 1:
            output.write_bytes(b"")
            with output.open("r+b") as handle:
                handle.truncate(size)
            part_size = max(16 * MiB, min(128 * MiB, size // max(1, int(workers))))
            ranges_to_get = [(start, min(size, start + part_size)) for start in range(0, size, part_size)]
            range_progress: dict[int, int] = {}

            def bump_range(start: int, loaded: int) -> None:
                nonlocal transferred
                if not progress:
                    return
                with lock:
                    range_progress[int(start)] = max(0, int(loaded))
                    transferred = min(size, sum(range_progress.values()))
                    elapsed = max(time.monotonic() - started, 0.001)
                    progress(TransferProgress(display_path, transferred, size, "downloading", transferred / elapsed))

            with ThreadPoolExecutor(max_workers=max(1, int(workers)), thread_name_prefix="diskblaze-download") as executor:
                futures = [executor.submit(self._download_range, url, output, start, end, bump_range) for start, end in ranges_to_get]
                for future in as_completed(futures):
                    future.result()
        else:
            with self._session().get(url, stream=True, timeout=self.timeout) as response:
                response.raise_for_status()
                with output.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=4 * MiB):
                        if not chunk:
                            continue
                        handle.write(chunk)
                        bump(len(chunk))
        progress and progress(TransferProgress(display_path, size or transferred, size or transferred, "done", 0))
        return output

    def _put_stream(self, url: str, body: Iterable[bytes], *, length: int, progress: Callable[[int], None] | None = None) -> str:
        response = self._session().put(url, data=body, headers={"Content-Length": str(int(length))}, timeout=self.timeout)
        response.raise_for_status()
        return response.headers.get("ETag", "").replace('"', "")

    def _upload_part(self, path: Path, part: UploadPart, progress: Callable[[int, int], None] | None) -> dict:
        length = part.end - part.start
        last_error: Exception | None = None
        for attempt in range(4):
            try:
                if progress:
                    progress(part.number, 0)
                with path.open("rb") as handle:
                    loaded = 0

                    def bump(delta: int) -> None:
                        nonlocal loaded
                        loaded += int(delta)
                        if progress:
                            progress(part.number, loaded)

                    reader = _ProgressReader(handle, length=length, offset=part.start, callback=bump)
                    etag = self._put_stream(part.url, reader, length=length)
                break
            except requests.RequestException as exc:
                last_error = exc
                if attempt == 3:
                    raise
                time.sleep(min(8.0, 0.5 * (2 ** attempt)))
        else:
            raise last_error or DiskBlazeError("part upload failed")
        return {"number": part.number, "etag": etag}

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        wait=wait_exponential_jitter(initial=0.5, max=8),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def _download_range(self, url: str, output: Path, start: int, end: int, progress: Callable[[int, int], None]) -> None:
        headers = {"Range": f"bytes={start}-{end - 1}"}
        loaded = 0
        progress(start, 0)
        with self._session().get(url, headers=headers, stream=True, timeout=self.timeout) as response:
            response.raise_for_status()
            if response.status_code != 206:
                raise DiskBlazeError("server did not honor range request")
            with output.open("r+b") as handle:
                handle.seek(start)
                for chunk in response.iter_content(chunk_size=4 * MiB):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    loaded += len(chunk)
                    progress(start, loaded)

    @staticmethod
    def sha256(path: Path, *, progress_path: str, total: int, progress: ProgressCallback | None) -> str:
        digest = hashlib.sha256()
        read = 0
        started = time.monotonic()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(8 * MiB)
                if not chunk:
                    break
                digest.update(chunk)
                read += len(chunk)
                if progress:
                    elapsed = max(time.monotonic() - started, 0.001)
                    progress(TransferProgress(progress_path, read, total, "hashing", read / elapsed))
        return digest.hexdigest()


def endpoint_from_base(value: str) -> str:
    raw = value.strip()
    if raw.endswith("/graphql"):
        return raw
    parsed = urlparse(raw)
    if parsed.scheme and parsed.netloc:
        return raw.rstrip("/") + "/graphql"
    return raw

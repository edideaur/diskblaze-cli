from __future__ import annotations

import argparse
import csv
import getpass
import json
import os
import posixpath
import re
import sys
import threading
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from pathlib import Path

from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.table import Table

from . import config
from . import sync as sync_mod
from .client import (
    DiskBlazeClient,
    DiskBlazeError,
    TransferProgress,
    endpoint_from_base,
    join_remote,
    normalize_remote_path,
)
from .watch import FolderWatcher

# argcomplete is optional; when present, attach remote-path tab completion.
try:
    import argcomplete  # noqa: F401

    _HAS_ARGCOMPLETE = True
except ImportError:
    _HAS_ARGCOMPLETE = False


def add_remote_arg(parser: argparse.ArgumentParser, *args, **kwargs) -> None:
    """Add a positional argument that completes remote DiskBlaze paths."""
    action = parser.add_argument(*args, **kwargs)
    if _HAS_ARGCOMPLETE:
        action.completer = _remote_path_completer  # ty: ignore[unresolved-attribute]


console = Console()


# Whether to print per-file planning details. Toggled by --verbose.
VERBOSE = False


def log(message: str) -> None:
    """Print an informational message only when --verbose is enabled."""
    if VERBOSE:
        console.print(f"[dim]{message}[/dim]")


class ProgressMux:
    def __init__(self, progress: Progress):
        self.progress = progress
        self.lock = threading.Lock()
        self.tasks: dict[str, TaskID] = {}

    def __call__(self, event: TransferProgress) -> None:
        key = event.path
        with self.lock:
            task_id = self.tasks.get(key)
            if task_id is None:
                task_id = self.progress.add_task(
                    f"{event.phase} {short_path(key)}",
                    total=max(1, event.total_bytes),
                    start=True,
                )
                self.tasks[key] = task_id
            self.progress.update(
                task_id,
                description=f"{event.phase} {short_path(key)}",
                completed=min(
                    event.transferred_bytes, max(event.total_bytes, event.transferred_bytes)
                ),
                total=max(1, event.total_bytes or event.transferred_bytes or 1),
            )


class QuietProgress:
    """One stderr line per transfer phase; used with ``--no-progress``."""

    def __init__(self) -> None:
        self._phases: dict[str, str] = {}

    def __call__(self, event: TransferProgress) -> None:
        previous = self._phases.get(event.path)
        if previous != event.phase:
            self._phases[event.path] = event.phase
            print(
                f"{event.phase} {short_path(event.path)}",
                file=sys.stderr,
                flush=True,
            )


def short_path(value: str, width: int = 72) -> str:
    if len(value) <= width:
        return value
    keep = max(12, (width - 3) // 2)
    return f"{value[:keep]}...{value[-keep:]}"


def resolve_endpoint(args: argparse.Namespace) -> str:
    """Endpoint order: --endpoint, env, saved login endpoint, default host."""
    raw = (
        getattr(args, "endpoint", None)
        or os.environ.get("DISKBLAZE_URL")
        or os.environ.get("DISKBLAZE_GQL_URL")
        or config.stored_endpoint()
        or "https://diskblaze.com"
    )
    return endpoint_from_base(raw)


def resolve_token(args: argparse.Namespace) -> str | None:
    """Token order: --token, env vars, saved login credentials."""
    return (
        getattr(args, "token", None)
        or os.environ.get("DISKBLAZE_TOKEN")
        or os.environ.get("DISKBLAZE_API_KEY")
        or config.stored_token()
    )


def build_client(args: argparse.Namespace) -> DiskBlazeClient:
    token = resolve_token(args)
    if not token:
        raise DiskBlazeError(
            "not authenticated: run `diskblaze login`, pass --token, or set DISKBLAZE_TOKEN"
        )
    endpoint = resolve_endpoint(args)
    return DiskBlazeClient(
        endpoint=endpoint,
        token=token,
        timeout=args.timeout,
        pool_size=max(args.workers * max(args.file_workers, 1) + 8, 32),
        retries=args.retries,
        retry_backoff=args.backoff,
        backoff_cap=getattr(args, "backoff_cap", 60.0),
        graphql_concurrency=getattr(args, "graphql_concurrency", 4),
    )


def transfer_progress(*, disable: bool = False) -> Progress:
    return Progress(
        TextColumn("[progress.description]{task.description}", table_column=None),
        BarColumn(),
        DownloadColumn(binary_units=True),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
        disable=disable,
    )


def command_login(args: argparse.Namespace) -> int:
    token = (
        getattr(args, "token", None)
        or os.environ.get("DISKBLAZE_TOKEN")
        or os.environ.get("DISKBLAZE_API_KEY")
    )
    if not token:
        if not sys.stdin.isatty():
            raise DiskBlazeError("no token provided; pass --token or set DISKBLAZE_TOKEN")
        token = getpass.getpass("DiskBlaze API token: ").strip()
    if not token:
        raise DiskBlazeError("no token provided")

    endpoint = resolve_endpoint(args)
    # Validate the token before saving so a typo fails loudly here.
    client = DiskBlazeClient(
        endpoint=endpoint,
        token=token,
        timeout=args.timeout,
        backoff_cap=getattr(args, "backoff_cap", 60.0),
        graphql_concurrency=getattr(args, "graphql_concurrency", 4),
    )
    user = client.me()

    # Only persist a non-default endpoint so upstream URL changes still apply.
    save_endpoint = getattr(args, "endpoint", None) or config.stored_endpoint()
    path = config.save_credentials(token, endpoint if save_endpoint else None)
    console.print(f"[green]logged in[/green] as {user.username} ({user.used} used of {user.quota})")
    console.print(f"[dim]credentials saved to {path}[/dim]")
    return 0


def command_logout(args: argparse.Namespace) -> int:
    if config.clear_credentials():
        console.print("[green]logged out[/green]")
    else:
        console.print("[yellow]not logged in[/yellow]")
    return 0


def command_whoami(args: argparse.Namespace) -> int:
    client = build_client(args)
    user = client.me()
    fields = [
        ("Username", user.username),
        ("Used", user.used),
        ("Remaining", user.remaining),
        ("Quota", user.quota),
        ("API access", "enabled" if user.api_access_enabled else "disabled"),
        ("Direct UL", "enabled" if user.direct_ul_enabled else "disabled"),
    ]
    output = _resolve_output(args)
    if output == "json":
        _print_json(user.__dict__)
        return 0
    if output == "csv":
        _print_csv(["Key", "Value"], fields)
        return 0
    table = Table(show_header=False, box=None)
    table.add_column("Key", style="dim")
    table.add_column("Value")
    for key, value in fields:
        table.add_row(key, value)
    console.print(table)
    return 0


def command_ls(args: argparse.Namespace) -> int:
    client = build_client(args)
    rows = client.list_files(args.path)
    output = _resolve_output(args)
    if output == "json":
        _print_json([node.__dict__ for node in rows])
        return 0
    headers = ["Name", "Type", "Size", "Modified"]
    if output == "csv":
        _print_csv(
            headers,
            [
                (
                    node.name,
                    "dir" if node.is_dir else "file",
                    "" if node.is_dir else node.size,
                    node.updated_at,
                )
                for node in rows
            ],
        )
        return 0
    table = Table(title=args.path, show_header=True, header_style="bold")
    table.add_column("Name", overflow="fold")
    table.add_column("Type", style="dim")
    table.add_column("Size", justify="right")
    table.add_column("Modified", overflow="fold")
    for node in rows:
        table.add_row(
            node.name,
            "dir" if node.is_dir else "file",
            "" if node.is_dir else node.size,
            node.updated_at,
        )
    console.print(table)
    return 0


def command_search(args: argparse.Namespace) -> int:
    client = build_client(args)
    rows, has_more = client.search_files(
        args.query,
        path_prefix=args.path,
        kind=args.kind,
        min_size_bytes=args.min_size_bytes,
        max_size_bytes=args.max_size_bytes,
        updated_after=args.updated_after,
        updated_before=args.updated_before,
        limit=args.limit,
        offset=args.offset,
    )
    output = _resolve_output(args)
    if output == "json":
        _print_json({"hasMore": has_more, "items": [node.__dict__ for node in rows]})
        return 0
    headers = ["Path", "Type", "Size", "Modified"]
    if output == "csv":
        _print_csv(
            headers,
            [
                (
                    node.path,
                    "dir" if node.is_dir else "file",
                    "" if node.is_dir else node.size,
                    node.updated_at,
                )
                for node in rows
            ],
        )
        if has_more:
            sys.stderr.write("More results available. Increase --limit or use --offset.\n")
        return 0
    table = Table(title=f"Search: {args.query}", show_header=True, header_style="bold")
    table.add_column("Path", overflow="fold")
    table.add_column("Type", style="dim")
    table.add_column("Size", justify="right")
    table.add_column("Modified", overflow="fold")
    for node in rows:
        table.add_row(
            node.path,
            "dir" if node.is_dir else "file",
            "" if node.is_dir else node.size,
            node.updated_at,
        )
    console.print(table)
    if has_more:
        console.print("[yellow]More results available. Increase --limit or use --offset.[/yellow]")
    return 0


def command_mkdir(args: argparse.Namespace) -> int:
    client = build_client(args)
    client.ensure_folder(args.path)
    console.print(f"[green]created[/green] {args.path}")
    return 0


def command_mv(args: argparse.Namespace) -> int:
    if args.dry_run:
        console.print(f"[yellow]dry-run[/yellow] would move {args.src} -> {args.dst}")
        return 0
    client = build_client(args)
    node = client.move(args.src, args.dst)
    console.print(f"[green]moved[/green] {args.src} -> {node.path}")
    return 0


def command_rm(args: argparse.Namespace) -> int:
    if args.dry_run:
        console.print(f"[yellow]dry-run[/yellow] would move {args.path} to Trash")
        return 0
    if not args.yes:
        prompt = f"Move {args.path!r} to Trash? Type DELETE to continue: "
        if console.input(prompt) != "DELETE":
            console.print("[yellow]cancelled[/yellow]")
            return 130
    client = build_client(args)
    message = client.delete(args.path)
    console.print(f"[green]deleted[/green] {args.path}: {message}")
    return 0


def command_url(args: argparse.Namespace) -> int:
    client = build_client(args)
    url = (
        client.zip_url(args.path, expires_seconds=args.expires)
        if args.zip
        else client.download_url(args.path, expires_seconds=args.expires)
    )
    console.print(url)
    return 0


def command_upload(args: argparse.Namespace) -> int:
    client = build_client(args)
    local = Path(args.local).expanduser()
    if not local.exists():
        raise DiskBlazeError(f"local path does not exist: {local}")
    remote = args.remote or join_remote("/private", local.name)
    if args.dry_run:
        console.print(f"[yellow]dry-run[/yellow] would upload {local} -> {remote}")
        return 0
    progress = transfer_progress(disable=getattr(args, "no_progress", False))
    mux = QuietProgress() if getattr(args, "no_progress", False) else ProgressMux(progress)
    with progress:
        if local.is_dir():
            result = client.upload_tree(
                local,
                remote,
                workers=args.workers,
                file_workers=args.file_workers,
                checksum=not args.no_sha256,
                resume=args.resume,
                create_folders=not args.no_create_folders,
                progress=mux,
            )
            console.print(f"[green]uploaded[/green] {len(result)} files to {remote}")
        else:
            target = remote
            existing = client.get_node(target)
            if existing is not None and existing.is_dir:
                target = join_remote(target, local.name)
            node = client.upload_file(
                local,
                target,
                workers=args.workers,
                part_size=args.part_size,
                checksum=not args.no_sha256,
                resume=args.resume,
                ensure_parent=not args.no_create_folders,
                progress=mux,
            )
            console.print(f"[green]uploaded[/green] {node.path} ({node.size})")
    return 0


def command_download(args: argparse.Namespace) -> int:
    client = build_client(args)
    output = Path(args.local).expanduser()
    if args.dry_run:
        console.print(f"[yellow]dry-run[/yellow] would download {args.remote} -> {output}")
        return 0
    progress = transfer_progress(disable=getattr(args, "no_progress", False))
    mux = QuietProgress() if getattr(args, "no_progress", False) else ProgressMux(progress)
    with progress:
        if args.recursive:
            paths = client.download_tree(
                args.remote,
                output,
                workers=args.workers,
                file_workers=args.file_workers,
                expires_seconds=args.expires,
                resume=args.resume,
                progress=mux,
            )
            console.print(
                f"[green]downloaded[/green] {len(paths)} files from {args.remote} -> {output}"
            )
        else:
            path = client.download(
                args.remote,
                output,
                workers=args.workers,
                expires_seconds=args.expires,
                as_zip=args.zip,
                resume=args.resume,
                progress=mux,
            )
            console.print(f"[green]downloaded[/green] {args.remote} -> {path}")
    return 0


def _split_globs(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def command_sync(args: argparse.Namespace) -> int:
    client = build_client(args)
    local = Path(args.local).expanduser()
    if not local.exists() or not local.is_dir():
        raise DiskBlazeError(f"local directory does not exist: {local}")
    include = _split_globs(args.include)
    exclude = _split_globs(args.exclude)
    plan = sync_mod.plan_sync(
        client,
        local,
        args.remote,
        two_way=args.two_way,
        delete=args.delete,
        checksum=not args.no_sha256,
        include=include,
        exclude=exclude,
    )
    if args.dry_run or VERBOSE:
        for rel, remote_path in plan.to_upload:
            console.print(f"[cyan]+ upload[/cyan] {rel} -> {remote_path}")
        for remote_path, rel in plan.to_download:
            console.print(f"[magenta]+ download[/magenta] {remote_path} -> {rel}")
        for remote_path in plan.to_delete_remote:
            console.print(f"[red]- remote[/red] {remote_path}")
        for local_path in plan.to_delete_local:
            console.print(f"[red]- local[/red] {local_path}")
    if args.dry_run:
        console.print(f"[yellow]dry-run[/yellow] {plan.summarize()}")
        return 0
    if plan.empty:
        console.print("[green]in sync[/green]")
        return 0

    progress = transfer_progress(disable=getattr(args, "no_progress", False))
    mux = QuietProgress() if getattr(args, "no_progress", False) else ProgressMux(progress)
    with progress:
        for rel, remote_path in plan.to_upload:
            client.upload_file(
                local / rel,
                remote_path,
                workers=args.workers,
                checksum=not args.no_sha256,
                resume=True,
                ensure_parent=not args.no_create_folders,
                progress=mux,
            )
        for remote_path, rel in plan.to_download:
            client.download(
                remote_path,
                local / rel,
                workers=args.workers,
                expires_seconds=args.expires,
                resume=True,
                progress=mux,
            )
        for remote_path in plan.to_delete_remote:
            client.delete(remote_path)
            log(f"deleted remote {remote_path}")
        for local_path in plan.to_delete_local:
            Path(local_path).unlink()
            log(f"deleted local {local_path}")
    console.print(f"[green]synced[/green] {plan.summarize()}")
    return 0


def command_watch(args: argparse.Namespace) -> int:
    if args.dry_run:
        console.print(f"[yellow]dry-run[/yellow] would watch {args.local} -> {args.remote}")
        return 0
    client = build_client(args)
    local = Path(args.local).expanduser()
    if not local.exists() or not local.is_dir():
        raise DiskBlazeError(f"local directory does not exist: {local}")
    include = _split_globs(args.include)
    exclude = _split_globs(args.exclude)
    watcher = FolderWatcher(
        client,
        local,
        args.remote,
        interval=args.interval,
        checksum=not args.no_sha256,
        create_folders=not args.no_create_folders,
        include=include,
        exclude=exclude,
        on_event=lambda message: console.print(f"[dim]{message}[/dim]"),
    )
    try:
        watcher.run()
    except KeyboardInterrupt:
        watcher.stop()
        console.print("\n[yellow]stopped watching[/yellow]")
    return 0


def command_cp(args: argparse.Namespace) -> int:
    if args.dry_run:
        console.print(f"[yellow]dry-run[/yellow] would copy {args.src} -> {args.dst}")
        return 0
    client = build_client(args)
    job = client.copy(args.src, args.dst)
    console.print(
        f"[green]copy started[/green] {job.source_path} -> {job.destination_path} "
        f"(job {job.id}, status {job.status})"
    )
    return 0


def command_stat(args: argparse.Namespace) -> int:
    client = build_client(args)
    node = client.get_node(args.path)
    if node is None:
        raise DiskBlazeError(f"not found: {args.path}")
    fields = [
        ("Name", node.name),
        ("Path", node.path),
        ("Type", "dir" if node.is_dir else "file"),
        ("Size", "" if node.is_dir else node.size),
        ("Modified", node.updated_at),
        ("Readonly", "yes" if node.readonly else "no"),
        ("SHA-256", node.content_sha256 or ""),
    ]
    output = _resolve_output(args)
    if output == "json":
        _print_json(node.__dict__)
        return 0
    if output == "csv":
        _print_csv(["Key", "Value"], fields)
        return 0
    table = Table(title=args.path, show_header=False, box=None)
    table.add_column("Key", style="dim")
    table.add_column("Value")
    for key, value in fields:
        table.add_row(key, value)
    console.print(table)
    return 0


def command_du(args: argparse.Namespace) -> int:
    client = build_client(args)
    root = args.path
    nodes = client.list_recursive(root)
    total = sum(n.size_bytes for n in nodes)
    by_dir: dict[str, int] = {}
    for node in nodes:
        parent = posixpath.dirname(node.path.rstrip("/")) or "/"
        by_dir[parent] = by_dir.get(parent, 0) + node.size_bytes
    output = _resolve_output(args)
    if output == "json":
        _print_json({"path": root, "files": len(nodes), "bytes": total})
        return 0
    if output == "csv":
        _print_csv(
            ["Path", "Size"],
            [
                (path, by_dir[path])
                for path in sorted(by_dir, key=lambda item: by_dir[item], reverse=True)
            ]
            + [("TOTAL", total)],
        )
        return 0
    table = Table(title=f"Usage: {root}", show_header=True, header_style="bold")
    table.add_column("Path", overflow="fold")
    table.add_column("Size", justify="right")
    for path in sorted(by_dir, key=lambda item: by_dir[item], reverse=True):
        table.add_row(path, _human_size(by_dir[path]))
    table.add_row("[bold]TOTAL[/bold]", f"[bold]{_human_size(total)}[/bold]")
    console.print(table)
    return 0


def command_tree(args: argparse.Namespace) -> int:
    client = build_client(args)
    root = normalize_remote_path(args.path)
    entries: list[tuple[str, int, str, str]] = []

    def walk(path: str, depth: int) -> None:
        for node in client.list_files(path):
            node_type = "dir" if node.is_dir else "file"
            entries.append((node.path, depth, node_type, node.size if not node.is_dir else ""))
            if node.is_dir:
                walk(node.path, depth + 1)

    walk(root, 1)
    output = _resolve_output(args)
    if output == "json":
        _print_json(
            {
                "root": root,
                "entries": [
                    {"path": e[0], "depth": e[1], "type": e[2], "size": e[3]} for e in entries
                ],
            }
        )
        return 0
    if output == "csv":
        _print_csv(["Path", "Depth", "Type", "Size"], entries)
        return 0
    console.print(f"[blue]{root}/[/blue]")
    for path, depth, node_type, size in entries:
        prefix = "  " * depth
        name = path.rstrip("/").rsplit("/", 1)[-1]
        if node_type == "dir":
            console.print(f"{prefix}[blue]{name}/[/blue]")
        else:
            console.print(f"{prefix}{name}  [dim]{size}[/dim]")
    return 0


def command_trash(args: argparse.Namespace) -> int:
    client = build_client(args)
    entries = client.trash()
    older_than = getattr(args, "older_than", None)
    if older_than:
        cutoff = datetime.now(timezone.utc) - _parse_duration(older_than)
        kept: list = []
        for entry in entries:
            try:
                deleted = _parse_iso(entry.deleted_at)
            except Exception:
                kept.append(entry)
                continue
            if deleted.tzinfo is None:
                deleted = deleted.replace(tzinfo=timezone.utc)
            if deleted <= cutoff:
                kept.append(entry)
        entries = kept
    output = _resolve_output(args)
    if output == "json":
        _print_json([e.__dict__ for e in entries])
        return 0
    if output == "csv":
        _print_csv(
            ["Name", "Path", "Size", "Deleted", "Expires"],
            [(e.name, e.path, e.size_bytes, e.deleted_at, e.expires_at) for e in entries],
        )
        return 0
    table = Table(title=f"Trash ({len(entries)} items)", show_header=True, header_style="bold")
    table.add_column("Name")
    table.add_column("Path", overflow="fold")
    table.add_column("Size", justify="right")
    table.add_column("Deleted")
    table.add_column("Expires")
    for entry in entries:
        table.add_row(
            entry.name,
            entry.path,
            _human_size(entry.size_bytes),
            entry.deleted_at,
            entry.expires_at,
        )
    console.print(table)
    return 0


def command_restore(args: argparse.Namespace) -> int:
    client = build_client(args)
    if args.all:
        for entry in client.trash():
            message = client.restore_trash(entry.deletion_id)
            console.print(f"[green]restored[/green] {entry.path}: {message}")
        return 0
    if not args.deletion_id:
        raise DiskBlazeError("provide --deletion-id or --all")
    message = client.restore_trash(args.deletion_id)
    console.print(f"[green]restored[/green] {args.deletion_id}: {message}")
    return 0


def command_share(args: argparse.Namespace) -> int:
    client = build_client(args)
    link = client.create_share_link(
        args.path,
        password=args.password,
        instructions=args.instructions,
        expires_hours=_expires_hours(args),
    )
    output = _resolve_output(args)
    if output == "json":
        _print_json(link.__dict__)
        return 0
    if output == "csv":
        _print_csv(
            ["Name", "Path", "URL", "Expires"],
            [(link.name, link.path, link.url or "", link.expires_at or "")],
        )
        return 0
    console.print(f"[green]shared[/green] {link.path}")
    if link.url:
        console.print(link.url)
    if link.expires_at:
        console.print(f"[dim]expires {link.expires_at}[/dim]")
    return 0


def command_share_list(args: argparse.Namespace) -> int:
    client = build_client(args)
    links = client.share_links(args.path)
    if getattr(args, "revoke_all", False):
        for link in links:
            client.revoke_share_link(link.id)
            console.print(f"[green]revoked[/green] {link.id} ({link.name})")
        return 0
    output = _resolve_output(args)
    if output == "json":
        _print_json([link.__dict__ for link in links])
        return 0
    if output == "csv":
        _print_csv(
            ["Name", "URL", "Expires"],
            [(link.name, link.url or "", link.expires_at or "") for link in links],
        )
        return 0
    table = Table(title=f"Shares: {args.path}", show_header=True, header_style="bold")
    table.add_column("Name")
    table.add_column("URL", overflow="fold")
    table.add_column("Expires")
    for link in links:
        table.add_row(link.name, link.url or "", link.expires_at or "")
    console.print(table)
    return 0


def command_revoke(args: argparse.Namespace) -> int:
    client = build_client(args)
    message = client.revoke_share_link(args.share_id)
    console.print(f"[green]revoked[/green] {args.share_id}: {message}")
    return 0


def command_cat(args: argparse.Namespace) -> int:
    client = build_client(args)
    if args.dry_run:
        url = client.download_url(args.path, expires_seconds=args.expires)
        console.print(f"[yellow]dry-run[/yellow] would stream {args.path}\n{url}")
        return 0
    for chunk in client.stream(args.path, expires_seconds=args.expires):
        sys.stdout.buffer.write(chunk)
    sys.stdout.buffer.flush()
    return 0


def command_completion(args: argparse.Namespace) -> int:
    if not _HAS_ARGCOMPLETE:
        console.print(
            "[red]error:[/red] argcomplete is not installed; run: pip install 'diskblaze[shell]'"
        )
        return 1
    from argcomplete.shell_integration import shellcode

    shell = args.shell
    template_shell = "bash" if shell == "zsh" else shell
    code = shellcode(["diskblaze"], shell=template_shell, argcomplete_script="diskblaze")
    sys.stdout.write(code)
    if not code.endswith("\n"):
        sys.stdout.write("\n")
    sys.stdout.flush()
    return 0


def _print_json(obj: object) -> None:
    """Emit compact JSON to stdout, bypassing Rich (which mangles control chars)."""
    sys.stdout.write(json.dumps(obj, indent=2, default=str) + "\n")
    sys.stdout.flush()


def _print_csv(headers: list[str], rows: Iterable[Iterable[object]]) -> None:
    """Emit CSV to stdout, bypassing Rich (which mangles control chars)."""
    writer = csv.writer(sys.stdout)
    writer.writerow(headers)
    for row in rows:
        writer.writerow(row)
    sys.stdout.flush()


def _resolve_output(args: argparse.Namespace) -> str:
    """Return the effective output format, letting the legacy ``--json`` win."""
    if getattr(args, "json", False):
        return "json"
    return getattr(args, "output", "table")


def _parse_duration(value: str) -> timedelta:
    """Parse a duration such as ``7d``, ``12h``, ``30m``, ``60s`` or ``2w``."""
    match = re.fullmatch(r"\s*(\d+)\s*([smhdwy])?\s*", value)
    if not match:
        raise DiskBlazeError(f"invalid duration: {value!r} (examples: 7d, 12h, 30m)")
    amount = int(match.group(1))
    unit = match.group(2) or "d"
    return {
        "s": timedelta(seconds=amount),
        "m": timedelta(minutes=amount),
        "h": timedelta(hours=amount),
        "d": timedelta(days=amount),
        "w": timedelta(weeks=amount),
        "y": timedelta(days=365 * amount),
    }[unit]


def _parse_iso(value: str) -> datetime:
    """Best-effort ISO-8601 parse (tolerates a trailing ``Z``)."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text)


def _expires_hours(args: argparse.Namespace) -> int | None:
    """Resolve share expiry from ``--expire-in`` (preferred) or ``--expires-hours``."""
    expire_in = getattr(args, "expire_in", None)
    if expire_in:
        hours = _parse_duration(expire_in).total_seconds() / 3600
        return int(round(hours))
    return getattr(args, "expires_hours", None)


def add_output_arg(parser: argparse.ArgumentParser) -> None:
    """Add the unified ``--output`` flag (table/json/csv) to a listing command."""
    parser.add_argument(
        "--output",
        choices=["table", "json", "csv"],
        default="table",
        dest="output",
        help="Output format. Default: table.",
    )


def _human_size(num: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(num) < 1024:
            return f"{num:.0f} {unit}" if unit == "B" else f"{num:.1f} {unit}"
        num /= 1024
    return f"{num:.1f} EB"


def add_common(parser: argparse.ArgumentParser, *, suppress_defaults: bool = False) -> None:
    default = argparse.SUPPRESS if suppress_defaults else None
    timeout_default = argparse.SUPPRESS if suppress_defaults else 120.0
    workers_default = argparse.SUPPRESS if suppress_defaults else 64
    file_workers_default = argparse.SUPPRESS if suppress_defaults else 8
    parser.add_argument(
        "--endpoint",
        default=default,
        help="GraphQL endpoint or DiskBlaze base URL. Default: https://diskblaze.com/graphql",
    )
    parser.add_argument(
        "--token",
        default=default,
        help="API key. Default: saved login, DISKBLAZE_TOKEN, or DISKBLAZE_API_KEY",
    )
    parser.add_argument("--timeout", type=float, default=timeout_default)
    parser.add_argument(
        "--workers",
        type=int,
        default=workers_default,
        help="Multipart upload/download workers per file.",
    )
    parser.add_argument(
        "--file-workers",
        type=int,
        default=file_workers_default,
        help="Concurrent files for folder uploads/downloads.",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Print per-file planning and progress details."
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=4,
        help="Number of times to retry failed requests. Default: 4.",
    )
    parser.add_argument(
        "--backoff",
        type=float,
        default=0.5,
        help="Base retry backoff in seconds (exponential). Default: 0.5.",
    )
    parser.add_argument(
        "--backoff-cap",
        type=float,
        default=60.0,
        help="Maximum seconds between retries. Default: 60.",
    )
    parser.add_argument(
        "--graphql-concurrency",
        type=int,
        default=4,
        help="Max concurrent control-plane (GraphQL) requests. Default: 4.",
    )


def add_transfer_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip files already present remotely (matching size/hash) or locally for downloads.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without performing any transfers.",
    )
    parser.add_argument(
        "--no-sha256",
        action="store_true",
        help="Disable SHA-256 content hashing; rely on size for change detection.",
    )
    parser.add_argument(
        "--no-create-folders",
        action="store_true",
        help="Do not create remote folders automatically; fail if the parent is missing.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable the live progress bar; print one line per file instead.",
    )
    parser.add_argument(
        "--include", default=None, help="Comma-separated glob(s) of files to include."
    )
    parser.add_argument(
        "--exclude", default=None, help="Comma-separated glob(s) of files to exclude."
    )


def add_command_common(parser: argparse.ArgumentParser) -> None:
    add_common(parser, suppress_defaults=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="diskblaze",
        description="Fast DiskBlaze GraphQL/gateway CLI for uploads and downloads.",
    )
    add_common(parser)
    sub = parser.add_subparsers(dest="command", required=True)

    login_cmd = sub.add_parser("login", help="Save a DiskBlaze API token for later commands.")
    add_command_common(login_cmd)
    login_cmd.set_defaults(func=command_login)

    logout_cmd = sub.add_parser("logout", help="Remove saved DiskBlaze credentials.")
    add_command_common(logout_cmd)
    logout_cmd.set_defaults(func=command_logout)

    whoami_cmd = sub.add_parser("whoami", help="Show the authenticated DiskBlaze account.")
    add_command_common(whoami_cmd)
    whoami_cmd.add_argument(
        "--json", action="store_true", help="Deprecated alias for --output json."
    )
    add_output_arg(whoami_cmd)
    whoami_cmd.set_defaults(func=command_whoami)

    ls_cmd = sub.add_parser("ls", help="List a remote folder.")
    add_command_common(ls_cmd)
    ls_cmd.add_argument("path", nargs="?", default="/")
    ls_cmd.add_argument("--json", action="store_true", help="Deprecated alias for --output json.")
    add_output_arg(ls_cmd)
    ls_cmd.set_defaults(func=command_ls)

    search_cmd = sub.add_parser("search", help="Search remote files recursively.")
    add_command_common(search_cmd)
    search_cmd.add_argument("query", nargs="?", default="")
    search_cmd.add_argument("--path", default=None, help="Optional remote folder prefix.")
    search_cmd.add_argument(
        "--kind",
        choices=["file", "folder", "image", "video", "audio", "document", "archive", "code"],
        help="Optional result type filter.",
    )
    search_cmd.add_argument("--min-size-bytes", type=int, default=None)
    search_cmd.add_argument("--max-size-bytes", type=int, default=None)
    search_cmd.add_argument("--updated-after", default=None, help="ISO timestamp lower bound.")
    search_cmd.add_argument("--updated-before", default=None, help="ISO timestamp upper bound.")
    search_cmd.add_argument("--limit", type=int, default=200)
    search_cmd.add_argument("--offset", type=int, default=0)
    search_cmd.add_argument(
        "--json", action="store_true", help="Deprecated alias for --output json."
    )
    add_output_arg(search_cmd)
    search_cmd.set_defaults(func=command_search)

    mkdir_cmd = sub.add_parser("mkdir", help="Create a remote folder path.")
    add_command_common(mkdir_cmd)
    mkdir_cmd.add_argument("path")
    mkdir_cmd.set_defaults(func=command_mkdir)

    mv_cmd = sub.add_parser("mv", help="Move or rename a remote file/folder.")
    add_command_common(mv_cmd)
    add_remote_arg(mv_cmd, "src")
    add_remote_arg(mv_cmd, "dst")
    mv_cmd.add_argument(
        "--dry-run", action="store_true", help="Show what would move without moving."
    )
    mv_cmd.set_defaults(func=command_mv)

    rm_cmd = sub.add_parser("rm", help="Move a remote file/folder to Trash.")
    add_command_common(rm_cmd)
    add_remote_arg(rm_cmd, "path")
    rm_cmd.add_argument(
        "-y", "--yes", action="store_true", help="Skip the DELETE confirmation prompt."
    )
    rm_cmd.add_argument(
        "--dry-run", action="store_true", help="Show what would be deleted without deleting."
    )
    rm_cmd.set_defaults(func=command_rm)

    upload_cmd = sub.add_parser("upload", aliases=["ul"], help="Upload a file or folder.")
    add_command_common(upload_cmd)
    upload_cmd.add_argument("local")
    add_remote_arg(upload_cmd, "remote", nargs="?")
    upload_cmd.add_argument(
        "--part-size", type=int, default=None, help="Override multipart part size in bytes."
    )
    add_transfer_common(upload_cmd)
    upload_cmd.set_defaults(func=command_upload)

    download_cmd = sub.add_parser(
        "download",
        aliases=["dl"],
        help="Download a file, a folder as ZIP, or a folder recursively.",
    )
    add_command_common(download_cmd)
    add_remote_arg(download_cmd, "remote")
    download_cmd.add_argument("local")
    download_cmd.add_argument(
        "--zip", action="store_true", help="Request a ZIP download for a folder."
    )
    download_cmd.add_argument(
        "--recursive",
        "-r",
        action="store_true",
        help="Download a remote folder as normal files in parallel.",
    )
    download_cmd.add_argument(
        "--expires", type=int, default=3600, help="Signed URL TTL in seconds."
    )
    add_transfer_common(download_cmd)
    download_cmd.set_defaults(func=command_download)

    sync_cmd = sub.add_parser(
        "sync", help="Mirror a local directory to a remote folder (or two-way)."
    )
    add_command_common(sync_cmd)
    sync_cmd.add_argument("local", help="Local directory to sync from.")
    add_remote_arg(sync_cmd, "remote", help="Remote folder to sync with.")
    sync_cmd.add_argument(
        "--two-way", action="store_true", help="Also download remote files missing locally."
    )
    sync_cmd.add_argument(
        "--delete",
        action="store_true",
        help="Delete remote extras (one-way) or both extras (two-way).",
    )
    sync_cmd.add_argument(
        "--expires", type=int, default=3600, help="Signed URL TTL in seconds for downloads."
    )
    add_transfer_common(sync_cmd)
    sync_cmd.set_defaults(func=command_sync)

    watch_cmd = sub.add_parser(
        "watch", help="Continuously upload a local folder's changes to remote."
    )
    add_command_common(watch_cmd)
    watch_cmd.add_argument("local", help="Local directory to watch.")
    add_remote_arg(watch_cmd, "remote", help="Remote folder to upload into.")
    watch_cmd.add_argument(
        "--interval", type=float, default=5.0, help="Polling interval in seconds."
    )
    add_transfer_common(watch_cmd)
    watch_cmd.set_defaults(func=command_watch)

    url_cmd = sub.add_parser("url", help="Print a signed gateway download URL.")
    add_command_common(url_cmd)
    add_remote_arg(url_cmd, "path")
    url_cmd.add_argument("--zip", action="store_true", help="Create a ZIP URL for a folder.")
    url_cmd.add_argument("--expires", type=int, default=3600, help="Signed URL TTL in seconds.")
    url_cmd.set_defaults(func=command_url)

    cp_cmd = sub.add_parser("cp", help="Server-side copy a remote file/folder.")
    add_command_common(cp_cmd)
    add_remote_arg(cp_cmd, "src")
    add_remote_arg(cp_cmd, "dst")
    cp_cmd.add_argument(
        "--dry-run", action="store_true", help="Show what would copy without copying."
    )
    cp_cmd.set_defaults(func=command_cp)

    stat_cmd = sub.add_parser("stat", help="Show metadata for a remote file/folder.")
    add_command_common(stat_cmd)
    add_remote_arg(stat_cmd, "path")
    stat_cmd.add_argument("--json", action="store_true", help="Deprecated alias for --output json.")
    add_output_arg(stat_cmd)
    stat_cmd.set_defaults(func=command_stat)

    du_cmd = sub.add_parser("du", help="Show disk usage under a remote folder.")
    add_command_common(du_cmd)
    add_remote_arg(du_cmd, "path", nargs="?", default="/")
    du_cmd.add_argument("--json", action="store_true", help="Deprecated alias for --output json.")
    add_output_arg(du_cmd)
    du_cmd.set_defaults(func=command_du)

    tree_cmd = sub.add_parser("tree", help="Print the remote directory hierarchy.")
    add_command_common(tree_cmd)
    add_remote_arg(tree_cmd, "path", nargs="?", default="/")
    tree_cmd.add_argument("--json", action="store_true", help="Deprecated alias for --output json.")
    add_output_arg(tree_cmd)
    tree_cmd.set_defaults(func=command_tree)

    trash_cmd = sub.add_parser("trash", help="List files in the trash.")
    add_command_common(trash_cmd)
    trash_cmd.add_argument(
        "--older-than",
        default=None,
        help="Only show trash items deleted more than this duration ago (e.g. 7d, 30m).",
    )
    trash_cmd.add_argument(
        "--json", action="store_true", help="Deprecated alias for --output json."
    )
    add_output_arg(trash_cmd)
    trash_cmd.set_defaults(func=command_trash)

    restore_cmd = sub.add_parser("restore", help="Restore a trashed file/folder.")
    add_command_common(restore_cmd)
    restore_cmd.add_argument("deletion_id", nargs="?", help="Trash entry deletionId.")
    restore_cmd.add_argument("--all", action="store_true", help="Restore every trashed item.")
    restore_cmd.set_defaults(func=command_restore)

    share_cmd = sub.add_parser("share", help="Create a share link for a remote path.")
    add_command_common(share_cmd)
    add_remote_arg(share_cmd, "path")
    share_cmd.add_argument("--password", default=None, help="Protect the link with a password.")
    share_cmd.add_argument("--instructions", default=None, help="Note shown on the share page.")
    share_cmd.add_argument(
        "--expires-hours", type=int, default=None, help="Link lifetime in hours."
    )
    share_cmd.add_argument(
        "--expire-in",
        default=None,
        help="Link lifetime as a duration (e.g. 7d, 12h, 30m). Overrides --expires-hours.",
    )
    share_cmd.add_argument(
        "--json", action="store_true", help="Deprecated alias for --output json."
    )
    add_output_arg(share_cmd)
    share_cmd.set_defaults(func=command_share)

    share_list_cmd = sub.add_parser("shares", help="List share links for a remote path.")
    add_command_common(share_list_cmd)
    add_remote_arg(share_list_cmd, "path")
    share_list_cmd.add_argument(
        "--revoke-all", action="store_true", help="Revoke every share link for the path."
    )
    share_list_cmd.add_argument(
        "--json", action="store_true", help="Deprecated alias for --output json."
    )
    add_output_arg(share_list_cmd)
    share_list_cmd.set_defaults(func=command_share_list)

    revoke_cmd = sub.add_parser("revoke", help="Revoke a share link by id.")
    add_command_common(revoke_cmd)
    revoke_cmd.add_argument("share_id")
    revoke_cmd.set_defaults(func=command_revoke)

    cat_cmd = sub.add_parser("cat", help="Stream a remote file to stdout.")
    add_command_common(cat_cmd)
    add_remote_arg(cat_cmd, "path")
    cat_cmd.add_argument("--expires", type=int, default=3600, help="Signed URL TTL in seconds.")
    cat_cmd.add_argument(
        "--dry-run", action="store_true", help="Print the signed URL without downloading."
    )
    cat_cmd.set_defaults(func=command_cat)

    completion_cmd = sub.add_parser(
        "completion", help="Print shell completion code (bash/zsh/fish/tcsh/powershell)."
    )
    add_command_common(completion_cmd)
    completion_cmd.add_argument(
        "shell",
        choices=["bash", "zsh", "fish", "tcsh", "powershell"],
        help="Shell to generate completion code for.",
    )
    completion_cmd.set_defaults(func=command_completion)

    return parser


def _remote_path_completer(prefix, **kwargs):
    """Suggest remote paths for tab completion (best-effort, requires auth)."""
    partial = (prefix or "").lstrip("/")
    base = "/" + partial.rsplit("/", 1)[0] if "/" in partial else ""
    try:
        client = build_client(
            argparse.Namespace(
                endpoint=None,
                token=None,
                timeout=120.0,
                workers=64,
                file_workers=8,
            )
        )
        names = sorted(node.name for node in client.list_files(base or "/"))
        suggestions = [f"{base}/{name}".lstrip("/") for name in names]
        if not base:
            suggestions = ["/" + s for s in suggestions]
        return [s for s in suggestions if s.startswith(prefix or s)]
    except Exception:
        return []


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        import argcomplete

        argcomplete.autocomplete(parser)
    except ImportError:
        pass
    args = parser.parse_args(argv)
    global VERBOSE
    VERBOSE = bool(getattr(args, "verbose", False))
    try:
        return int(args.func(args))
    except KeyboardInterrupt:
        console.print("\n[yellow]cancelled[/yellow]")
        return 130
    except Exception as exc:
        console.print(f"[red]error:[/red] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

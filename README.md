# DiskBlaze CLI

Command-line client for [DiskBlaze](https://diskblaze.com) storage.

## Install

```bash
pip install git+https://github.com/diskblaze/diskblaze-cli
```

## Log in

```bash
diskblaze login              # prompts for an API token, or pass --token
diskblaze logout
```

The token is saved to `~/.config/diskblaze/credentials.json`. You can also set
`DISKBLAZE_TOKEN` instead of logging in.

## Commands

```bash
diskblaze whoami
diskblaze ls /private
diskblaze search invoice --path /private

diskblaze mkdir /private/backups
diskblaze mv /private/a.mkv /private/b.mkv
diskblaze rm /private/old.bin

diskblaze upload ./movie.mkv /private/movie.mkv     # ul
diskblaze upload ./folder /private/folder
diskblaze download /private/movie.mkv ./movie.mkv   # dl
diskblaze download /private/folder ./folder -r      # recursive
diskblaze download /private/folder ./folder.zip --zip
diskblaze url /private/movie.mkv --expires 604800   # signed link

# Sync a local folder to a remote folder (skips unchanged files via SHA-256)
diskblaze sync ./backups /private/backups
diskblaze sync ./backups /private/backups --two-way --delete   # bidirectional
diskblaze sync ./backups /private/backups --dry-run --verbose  # preview

# Continuously upload changes as they happen
diskblaze watch ./backups /private/backups --interval 5

# Inspect and manage remote files
diskblaze cp /private/a.mkv /private/b.mkv --dry-run  # server-side copy (preview)
diskblaze stat /private/a.mkv --output json           # file metadata
diskblaze du /private                                # disk usage per folder
diskblaze tree /private                              # remote directory tree
diskblaze cat /private/a.mkv                         # stream a file to stdout

# Trash and share links
diskblaze rm /private/a.mkv --yes                     # move a file to Trash
diskblaze trash --older-than 30d                     # list trash older than 30 days
diskblaze restore <deletion-id>                     # restore a trashed item
diskblaze share /private/a.mkv --expire-in 7d         # create a share link (expires in 7 days)
diskblaze shares /private/a.mkv --revoke-all         # revoke every share link for the path
diskblaze revoke <share-id>                          # revoke a share link
```

Run `diskblaze <command> --help` for all options. `--workers` and
`--file-workers` tune upload/download concurrency.

### Shared transfer flags

Most transfer commands accept:

- `--resume` — skip files already present remotely (upload) or locally
  (download) with a matching size and SHA-256, so interrupted runs finish
  without re-transferring bytes.
- `--dry-run` — show what would change without performing any transfers.
- `--no-sha256` — disable content hashing; changes are detected by size only.
- `--no-create-folders` — do not auto-create remote folders.
- `--include` / `--exclude` — comma-separated globs to filter synced files.
- `--verbose` — print per-file planning details.
- `--no-progress` — disable the live progress bar; print one line per file
  (useful when redirecting output to a log you want to read/copy).

All commands also accept global request-tuning flags:

- `--retries N` — number of times to retry a failed request (default 4).
- `--backoff S` — base retry backoff in seconds, applied exponentially (default 0.5).
- `--backoff-cap C` — maximum seconds between retries (default 60).
- `--graphql-concurrency K` — max concurrent control-plane (GraphQL) requests;
  throttles API calls so high `--file-workers` won't trip HTTP 429 rate limits
  (default 4). Data-plane transfers to object storage are unaffected.

Listing commands (`ls`, `search`, `du`, `tree`, `trash`, `shares`, `whoami`,
`stat`, `share`) accept `--output {table,json,csv}`. `--output json` (or the
legacy `--json` flag) prints machine-readable JSON; `--output csv` prints a
CSV table to stdout.

### Shell completion

```bash
pip install diskblaze[shell]
eval "$(register-python-argcomplete diskblaze)"
```

Or generate shell code for your shell directly:

```bash
diskblaze completion bash   # also: zsh, fish, tcsh, powershell
eval "$(diskblaze completion bash)"
```


## Python

```python
from diskblaze import DiskBlazeClient

client = DiskBlazeClient(token="db_...")   # or reads DISKBLAZE_TOKEN
client.upload_file("movie.mkv", "/private/movie.mkv")
client.download("/private/movie.mkv", "movie.mkv")
```

## License

[Apache 2.0](LICENSE)

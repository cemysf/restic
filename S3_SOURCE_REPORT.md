# Remote-Source Backup (S3 / rclone) — Implementation Report

**Date:** 2026-07-03
**Scope:** Implements the feature scoped in [handoff.md](handoff.md): back up data
directly *from* a remote service — an S3-compatible bucket (e.g. Ceph RGW) or any
rclone remote — *to* any restic repository (e.g. Hetzner Object Storage), with no
FUSE mount and no local staging.
This corresponds to upstream requests [restic#2529](https://github.com/restic/restic/issues/2529)
/ [restic#299](https://github.com/restic/restic/issues/299), which upstream declined.

## Usage

```sh
# S3 source (native, no extra binary needed)
export RESTIC_SOURCE_ACCESS_KEY_ID=<source-key>
export RESTIC_SOURCE_SECRET_ACCESS_KEY=<source-secret>
# optional: RESTIC_SOURCE_SESSION_TOKEN, RESTIC_SOURCE_DEFAULT_REGION

restic -r rclone:target_s3:test-backups-ms/test-fuse-restic \
    backup s3:https://<rgw-endpoint>/<tenant-bucket>/<prefix>

# rclone source (any remote rclone supports; uses regular rclone config /
# RCLONE_CONFIG_* env vars — works alongside an rclone repository backend)
restic -r rclone:target:bucket/repo backup rclone:source:bucket/path
```

All repository location formats of the S3 backend are accepted as the S3 source
(`s3:host/bucket/prefix`, `s3:https://host/bucket/prefix`, `s3://host/bucket/prefix`).
The prefix is optional; without it the whole bucket is backed up. For rclone sources,
everything after `rclone:` is passed to the rclone binary verbatim (named remotes,
connection strings, or plain local paths). Everything else (`forget`, `prune`,
`restore`, `check`, retention, encryption, dedup) works unchanged, since the snapshot
is a normal restic snapshot whose paths mirror the object keys.

The `RESTIC_SOURCE_*` variables are deliberately separate from `AWS_*` so source
credentials cannot collide with an S3 repository backend used as the destination. If
unset, the default AWS/MinIO chain (env vars, credential files, IAM) is used.

## What changed

| File | Change |
|---|---|
| [internal/fs/fs_s3.go](internal/fs/fs_s3.go) | New: S3-backed `fs.FS`/`fs.File` implementation; also hosts the shared source-FS core (caching, synthetic inodes) |
| [internal/fs/fs_rclone.go](internal/fs/fs_rclone.go) | New: rclone-backed source, reusing the same core via the `S3API` seam (`rclone lsjson` / `rclone cat`) |
| [cmd/restic/cmd_backup.go](cmd/restic/cmd_backup.go) | Detect `s3:` / `rclone:` source targets, build the source FS, credential/flag handling |
| [internal/fs/fs_s3_test.go](internal/fs/fs_s3_test.go) | New: unit tests against an in-memory S3 fake |
| [internal/fs/fs_rclone_test.go](internal/fs/fs_rclone_test.go) | New: tests against the real rclone binary (local-dir remote) incl. truncation-safety of the stream reader |
| [internal/archiver/archiver_s3_test.go](internal/archiver/archiver_s3_test.go) | New: end-to-end change-detection test through the real archiver |
| [cmd/restic/cmd_backup_s3_integration_test.go](cmd/restic/cmd_backup_s3_integration_test.go) | New: full CLI integration test against a fake S3 HTTP server |
| [cmd/restic/cmd_backup_rclone_integration_test.go](cmd/restic/cmd_backup_rclone_integration_test.go) | New: full CLI integration test with the real rclone binary |
| [doc/040_backup.rst](doc/040_backup.rst) | New sections "Reading data from an S3 bucket" / "… from an rclone remote" |
| [changelog/unreleased/issue-2529](changelog/unreleased/issue-2529) | Changelog entry |

### Design (matches handoff §3–§5, Option A)

The S3 FS is the fourth implementation of restic's existing `fs.FS` seam (next to
`Local`, `LocalVss`, `Reader`) — no archiver changes were needed:

- `Lstat` / metadata-only `OpenFile` → `HeadObject` (or a cached LIST result)
- `Readdirnames` → `ListObjectsV2` with `Delimiter=/` (common prefixes = subdirs)
- `MakeReadable` + `Read` → `GetObject`, streamed into the chunker
- Path ops are plain `/`-string operations on keys

The rclone source plugs into the exact same core through the `S3API` interface (the
seam originally introduced for testing): `Stat`/`List` map to `rclone lsjson
[--stat]`, `Get` to `rclone cat` with the stdout stream fed to the chunker. A
truncation guard converts an rclone process that dies mid-stream into a read error —
without it, a failed transfer would look like a clean EOF and a silently truncated
file would be stored. All caching, synthetic-inode, and change-detection behavior
below is therefore identical for both source types.

Change detection uses restic's own snapshot mechanism, no sidecar index (Option A):

- **Size / mtime** come directly from `Content-Length` / `LastModified`.
- **Inode** is a synthetic FNV-1a 64-bit hash of the object key; **DeviceID** is a
  hash of the bucket name. Stable across runs, so unchanged objects hit restic's
  no-read fast path. A hash collision aborts the snapshot with a fatal error rather
  than risking silent misdetection (handoff §7).
- **ctime** is set equal to mtime (S3 has no ctime), so the default ctime check
  never produces false positives.
- Synthetic directories get a fixed epoch mtime, keeping unchanged tree blobs
  byte-identical across runs.
- Listing a directory caches all entry metadata, so the archiver's per-object stat
  costs **one LIST per prefix instead of one HEAD per object**.
- On `GetObject` the response metadata is taken as authoritative, so if an object
  changes between LIST and GET, the snapshot records the metadata of the bytes
  actually stored.

## Verification

- **Unit tests** (`go test ./internal/fs/ -run TestS3`): listing, reading,
  not-found mapping, metadata caching (0 HEADs after LIST), inode stability across
  process restarts, loud collision failure, object-changed-mid-backup, duplicate
  object/prefix keys. All pass.
- **Archiver e2e** (`go test ./internal/archiver/ -run TestArchiverS3Source`):
  run 1 reads all 3 objects → run 2 with parent snapshot issues **0 GETs** (all
  unchanged) → after changing one object, run 3 issues **exactly 1 GET**; restored
  blob content verified. Passes. This answers the handoff §7 testing-strategy question.
- **CLI integration** (`go test ./cmd/restic/ -run TestBackupS3Source`): full
  init → backup ×3 → restore → check cycle against a fake S3 HTTP server, exercising
  the real minio client. **Skips inside the dev sandbox** (localhost binding is
  blocked) — run it on a normal machine/CI; it is the only path not executed live yet.
- **rclone source, live** (`go test ./internal/fs/ -run TestRclone` and
  `go test ./cmd/restic/ -run TestBackupRcloneSource`): runs against the real
  rclone binary with a local directory as the remote — full init → backup ×3 →
  restore → check cycle, executed and passing, including the reader truncation guard
  and repeated-read-after-EOF behavior the chunker depends on.
- `go build ./...`, `go vet`, and the existing backup/archiver suites pass.

## Limitations

- **backup-only.** Only the `backup` command understands an `s3:` or `rclone:`
  source. Other commands taking filesystem paths (`diff --path`, dump comparisons
  against a live bucket, etc.) do not.
- **One source per invocation.** Exactly one `s3:`/`rclone:` argument; it cannot be
  mixed with local paths or `--files-from`, `--stdin`, `--stdin-from-command`,
  `--use-fs-snapshot`.
- **rclone source: one process per file read.** Listings use one `rclone lsjson` per
  directory, but each *changed/new* file costs an `lsjson --stat` plus an
  `rclone cat` process spawn. Fine for incremental runs (unchanged files spawn
  nothing); initial backups of very many small files are noticeably slower than the
  native S3 source — prefer `s3:` for S3-compatible sources.
- **rclone source: requires the rclone binary** (any version with `lsjson`/`cat`,
  i.e. anything recent; override via `$RESTIC_SOURCE_RCLONE_PROGRAM`). rclone exit
  codes 3/4 are mapped to "not found"; other failures abort the affected file.
- **No atomic bucket snapshot.** S3 has no equivalent of a filesystem snapshot;
  objects created/deleted/modified mid-walk may or may not be included — the same
  consistency model as backing up a live local filesystem, but worth remembering for
  busy tenant buckets.
- **POSIX metadata is synthesized.** Mode is fixed (`0644` files, `0755` dirs),
  uid/gid are 0, no xattrs/ACLs/owner mapping. A restore reproduces object content
  and tree layout, not S3-side metadata (ETags, storage class, object ACLs,
  user-defined `x-amz-meta-*` are not stored).
- **No rename detection.** A renamed key is delete+new (correct for S3 — renames are
  copy+delete server-side — but means a full re-read of renamed objects; dedup still
  avoids re-uploading their chunks).
- **Odd keys are skipped, not backed up.** Keys producing unusable path entries
  (`a//b`, components of `.`/`..`) are skipped with a debug-log entry. A key existing
  both as object and prefix (`dup` *and* `dup/child`) keeps the object and drops the
  subtree.
- **mtime granularity is one second.** S3 listings report millisecond timestamps but
  HEAD/GET `Last-Modified` headers only whole seconds, so the FS normalizes all
  timestamps to whole seconds (otherwise objects with sub-second mtimes would be
  re-read on every run — covered by a dedicated regression test). Consequence: an
  object overwritten within the same second with identical size could theoretically
  be misdetected as unchanged; ETag is not (yet) used as a corroborating signal
  (handoff §7 leaves this open pending Ceph RGW multipart-ETag verification).
- **Memory scales with bucket size.** Per-object metadata and the inode-collision map
  are held in memory for the run (~a few hundred bytes/object; ~1M objects ≈ low
  hundreds of MB). Fine for tenant buckets, relevant for very large ones.
- **Version-pinning burden.** This rides on `internal/*` packages with no upstream
  compatibility guarantee. Upstream merges may require re-checking `fs.FS`/`fs.File`
  against [internal/fs/interface.go](internal/fs/interface.go) (handoff §6).
- **Credential model is static/env-based.** No per-source assume-role support
  (`RESTIC_AWS_ASSUME_ROLE_*` applies to the repository backend only).

## Open items

1. Run `go test ./cmd/restic/ -run TestBackupS3Source` outside the sandbox (only
   live-untested path: the real minio HTTP client wiring).
2. Validate once against a real Ceph RGW tenant bucket and the production rclone
   destination before scheduled use.
3. Optional hardening later: ETag as a secondary change signal; Option B sidecar
   index only if a concrete need appears (per handoff §4 recommendation).

# Handoff: Custom restic S3-as-Source Backend

**Status:** decided — proceeding as a personal/team fork. Source-side S3 support is the
goal; upstream compatibility and whether dedup "pays for itself" vs. Option 1 are no
longer open questions blocking the work — see §7.
**Context:** S3-to-S3 backup strategy for Ceph RGW (Rook-Ceph) tenant buckets → Hetzner Object Storage.

---

## 1. Problem

restic's `backup` command only accepts a local filesystem path as its source. There is
no way to point it at a remote object store directly. Confirmed against three
independent sources, converging on the same answer:

- **[restic/restic#2529 "Rclone as target"](https://github.com/restic/restic/issues/2529)**
  — the closest match to our exact case. Requests precisely what we need:
  `restic backup rclone:target:directory`, i.e. an `rclone:` remote as the backup
  *source*, not just the destination. Real-world motivating cases in the thread
  (hosted Nextcloud over WebDAV, seeding a new repo from an existing rclone remote)
  mirror our S3-source situation closely. **Explicitly merged into #299 by restic
  maintainers** ("looks like a duplicate of #299" / "#2529 closes #299 as a
  side-effect") — so it carries the same 10-year non-resolution, just phrased for the
  rclone/S3-source case specifically rather than #299's more abstract "pull from any
  remote server" framing.
- **[restic/restic#299 "Allow pulling backups from remote servers"](https://github.com/restic/restic/issues/299)**
  — open since 2015. Maintainers have explicitly declined the general feature as too
  large in scope (would require a remote agent process, a new RPC protocol, and
  splitting the archiver across a network boundary). A maintainer confirmed it
  "definitely won't happen this year" — that comment is now years old.
- **[restic forum: "Backup OF an S3 bucket"](https://forum.restic.net/t/backup-of-an-s3-bucket/4852)**
  — an independent user hitting our exact scenario verbatim. A restic regular's
  answer: *"restic backs up from local filesystem, it's not designed to back up from
  remote services... you'll have to somehow mount it locally first."* When the OP
  pushed back asking whether a local clone of the remote changes anything, the answer
  was still no — restic only ever accepts a real filesystem path. The thread ends with
  the OP abandoning restic in favor of the MinIO client's `mirror` command (S3-to-S3
  direct copy, functionally similar to `rclone sync`).

Also confirmed against restic's docs directly: the `rclone:`/`s3:`/`sftp:` URL schemes
are only ever repository (destination) backends, never data sources.

Our two evaluated options so far both work around this:
- **Option 1** — `rclone sync` bucket-to-bucket directly, no mount, no restic.
- **Option 2** — mount source as a filesystem (s3fs/rclone mount via FUSE), then run
  restic against the mount point. Blocked in our environment: no admin privileges to
  grant `SYS_ADMIN`/privileged/hostPath `/dev/fuse` access to workload pods, and no
  cluster-installed CSI driver that abstracts this away (confirmed: only
  `rook-ceph.cephfs.csi.ceph.com` / `rook-ceph.rbd.csi.ceph.com` are installed — CephFS
  and RBD, not S3-as-filesystem).

This doc scopes a **third option**: a custom, narrower alternative to Option 2 — write
a Go program that uses restic's own internal packages as a library, with a hand-built
S3 walker/reader replacing the filesystem walker, so restic backs up S3 objects
directly without ever mounting anything.

## 2. Why this might be worth it

- Keeps restic's chunk-level, content-defined deduplication — Option 1 (`rclone sync`)
  has no dedup; every changed object is a full re-copy.
- Avoids FUSE/mount entirely — no privilege escalation, no CSI driver dependency, no
  `/dev/fuse` device access. Solves the actual blocker on Option 2, not just the
  packaging around it.
- Reuses restic's storage/encryption/retention model (snapshots, `forget`, `prune`)
  which we'd otherwise have to reimplement or do without under Option 1.

## 3. The extension point: restic's `fs.FS` interface

This is not "hack the archiver." restic already has a real, intentional seam for
exactly this: `archiver.Archiver` takes an `FS fs.FS` field, and `internal/fs` already
ships **multiple non-`os`-backed implementations** — `Local` (real disk), `LocalVss`
(Windows shadow-copy snapshots), and `Reader` (a fake single-file "directory" used for
`restic backup --stdin`). A custom S3-backed `FS` is a fourth implementation of an
interface that already varies, not a new architectural layer bolted onto something
rigid.

As of a recent refactor (restic PRs #5143 and #5146, merged into `master`), the
interface is small:

```go
type FS interface {
    OpenFile(name string, flag int, metadataOnly bool) (File, error)
    Lstat(name string) (*ExtendedFileInfo, error)
    Join(elem ...string) string
    Separator() string
    Abs(path string) (string, error)
    Clean(path string) string
    VolumeName(path string) string
    IsAbs(path string) bool
    Dir(path string) string
    Base(path string) string
}

type File interface {
    MakeReadable() error   // reopens a metadata-only handle for actual reading
    io.Reader
    io.Closer
    Readdirnames(n int) ([]string, error)
    Stat() (*ExtendedFileInfo, error)
}
```

The archiver's real call pattern (seen directly in `internal/archiver/archiver.go`):
open metadata-only (`OpenFile(target, fs.O_NOFOLLOW, true)`) → `Stat()`/`Lstat()` to
decide whether the item changed since the parent snapshot → only if it needs reading,
call `MakeReadable()` and stream via `io.Reader`. Directories are walked via
`Readdirnames`. This maps cleanly onto S3:

| FS/File method | S3 implementation |
|---|---|
| `Lstat` / metadata-only `OpenFile` | `HeadObject` (or reuse a cached `ListObjectsV2` result) — no data transfer |
| `Readdirnames` on a "directory" | `ListObjectsV2` with `Delimiter=/` at that prefix; common prefixes = subdirs, keys = files |
| `MakeReadable` + `Read` | `GetObject`, streamed straight into restic's chunker |
| `Join`/`Clean`/`Dir`/`Base`/etc. | plain string ops on `/`-delimited keys — S3 keys are already POSIX-path-shaped |

**Caveat:** this interface shape is current as of a late-2024/2025 refactor. Confirm it
against whatever tag we actually fork from before writing code — `ExtendedFileInfo`'s
exact fields (it replaced raw `os.FileInfo` and folds in a platform-specific `sys`
value) need a direct read of the forked version's source, not this doc.

## 4. Field mapping: getting inode+mtime-equivalent behavior from S3

This is the actual crux. restic's fast-path (skip re-chunking a file that hasn't
changed since the parent snapshot) works by comparing the *previous* snapshot's stored
node fields (device ID, inode, size, mtime) against a fresh `Lstat`. If they match, it
trusts the file is unchanged and reuses the existing blobs — no re-read, no re-hash.
We need our `Lstat` to produce values that are stable across runs for an unchanged S3
object, and different when the object actually changes.

**Size and mtime need no fabrication** — `Content-Length` and `LastModified` from S3
map directly onto restic's `Size`/`ModTime` fields. The only genuinely missing piece is
device+inode, since S3 has no such concept.

### Option A — deterministic hash, no external state (recommended default)

- `DeviceID` = a constant (e.g. a hash of the bucket name) — restic just needs this to
  be consistent, it doesn't need to mean anything.
- `Inode` = a 64-bit hash (FNV-1a or xxhash) of the object key.

That's it — **no metadata.json needed for this part.** The reason: restic's own
snapshot tree already persists exactly the node fields (device, inode, size, mtime) it
compares against next run — that *is* restic's existing "memory" mechanism. We only
need a deterministic mapping function from S3 object identity → those fields; restic's
own repository does the rest, the same way it already does for real filesystems.

Tradeoffs: negligible collision risk at realistic bucket sizes with a 64-bit hash
(should still log loudly if ever detected — a collision would make two different
objects look like the same inode to restic, which needs to fail visibly, not
silently). No rename detection — a renamed S3 key looks like delete+new, which is
actually correct, since S3 has no native rename either (a "rename" is always
copy+delete under the hood).

### Option B — explicit metadata.json sidecar (more control, more moving parts)

Persist an index — `{key: {inode, last_etag, last_modified, size}}` — as a small JSON
(or NDJSON, for easier incremental appends at scale) file. This is what you originally
proposed, and it's the right call if we ever need something Option A can't give us:
explicit collision handling, tombstones for deleted keys, or a debuggable audit trail
of "what does our tool believe about this object" independent of restic's own
snapshot internals.

Recommended location if we go this route: the **destination** bucket
(`test-backups-ms/test-fuse-restic/.source-index.json`), not the source. Ceph RGW
tenant buckets are production data we don't own the lifecycle of — we shouldn't be
writing our own control-plane files into them.

Cost: another piece of state to keep consistent — with the real bucket contents *and*
with restic's own snapshot tree — plus a new failure mode (index drifts out of sync,
e.g. a crash mid-write), plus a scaling concern (one JSON file per tenant bucket gets
large; would need sharding for big buckets).

**Recommendation: start with Option A.** It has no consistency-drift risk and no extra
moving part, and restic's own snapshot mechanism already gives us everything the
sidecar would add. Add Option B later, specifically, only if we hit a concrete problem
it solves (observed hash collisions, or a real need for tombstoning/audit beyond what
A provides) — not preemptively.

## 5. Rough shape of the work

1. **`S3FS` type** implementing `fs.FS`/`fs.File` (§3), backed by an S3 client for the
   Ceph RGW source bucket.
2. **Lstat/metadata-only open** — `HeadObject`, mapped via §4 Option A into
   `ExtendedFileInfo` (size, mtime, synthetic device+inode).
3. **Readdirnames** — `ListObjectsV2` with `Delimiter=/`, common-prefixes → subdirs,
   keys → files.
4. **MakeReadable/Read** — lazily open `GetObject`, stream into restic's chunker.
5. **Wire it up**: link restic's Go packages (`internal/archiver`, `internal/fs`,
   `internal/repository`) as a library in a small standalone binary that constructs an
   `Archiver` with our `S3FS`, pointed at the existing `rclone:target_s3:...` repo we
   already use as the destination. Not a fork of the `restic` CLI itself — we don't
   need to touch `backend`/`repository`/destination code at all, since that path
   already works.

**Estimate:** low single-digit weeks solo, assuming Option A (no sidecar). Add
roughly another week if Option B turns out to be needed. This is a personal/team fork
for our own use — no upstream contribution planned, so no need to match restic's
review bar or backward-compat guarantees beyond what we ourselves require.

## 6. Constraints

- No local staging / no local disk buffering of full bucket contents during backup.
- No FUSE mount, no privileged containers, no cluster admin dependency for the
  workload itself.
- Must work against Ceph RGW (S3-compatible, self-hosted) as source and Hetzner Object
  Storage as destination (unchanged — destination path already works via the existing
  `rclone:` backend).
- This is a fork we build and maintain ourselves against restic's `internal/*`
  packages, which are **not** covered by restic's public API/semver guarantees —
  restic version upgrades may require re-checking/re-patching our `S3FS` against
  interface changes (see §3 caveat).

## 7. Open questions

- **Hash collision handling (Option A):** what should actually happen if two keys ever
  hash to the same synthetic inode? Needs a loud failure, not silent data corruption —
  worth adding an explicit check even if the probability is negligible.
- **ETag reliability**: not used for identity in Option A (mtime+size fill that role
  instead), but still worth confirming Ceph RGW's ETag behavior for multipart uploads
  in case we want ETag as a *secondary* corroborating signal later.
- **Failure/partial-run semantics**: object stores have no atomic directory snapshot —
  what happens if objects are added/modified mid-walk? Does this risk inconsistent
  snapshots in a way real filesystem backups don't, and do we need to care for our
  actual tenant-bucket write patterns?
- **Version pinning**: which restic version/tag do we fork from, and what's our policy
  for pulling in upstream fixes later given we're patched against `internal/*`?
- **Testing strategy**: how do we validate "unchanged object → skipped re-chunk" and
  "changed object → correctly re-chunked" against a real Ceph RGW bucket before
  trusting this for actual tenant backups?

## 8. Decision

Proceeding with the fork. Source-side S3 via a custom `fs.FS` implementation (§3–5,
Option A by default) is the path forward; Option 1 (`rclone sync`) remains documented
as the existing fallback but is no longer the default recommendation now that source
support is being built directly.

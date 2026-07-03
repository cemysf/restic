package fs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/restic/restic/internal/errors"
)

// RcloneOptions collects all parameters needed to open an rclone remote as a
// backup source file system.
type RcloneOptions struct {
	// Remote is the rclone location to back up, e.g. "myremote:bucket/path",
	// a connection string, or a plain local path.
	Remote string
	// Program is the rclone binary to execute, "rclone" if empty.
	Program string
}

// NewRclone returns a read-only FS that exposes the contents of an rclone
// remote as a file tree by shelling out to the rclone binary: listings and
// metadata via "rclone lsjson", file contents via "rclone cat". It is
// intended as a backup source, allowing restic to back up any remote
// supported by rclone without mounting it first.
func NewRclone(ctx context.Context, opts RcloneOptions) (FS, error) {
	if opts.Remote == "" {
		return nil, errors.Fatal("rclone source: remote not specified")
	}
	program := opts.Program
	if program == "" {
		program = "rclone"
	}
	if _, err := exec.LookPath(program); err != nil {
		return nil, errors.Fatalf("rclone source: %v", err)
	}

	api := &rcloneAPI{
		program: program,
		remote:  strings.TrimSuffix(opts.Remote, "/"),
	}
	return newS3FS(ctx, api, opts.Remote), nil
}

// rcloneAPI implements the S3API seam by executing the rclone binary. rclone
// remotes share the S3 object model closely enough: paths below the remote
// map to keys, directories are listed non-recursively.
type rcloneAPI struct {
	program string
	remote  string
}

// statically ensure that rcloneAPI implements S3API.
var _ S3API = &rcloneAPI{}

// rcloneItem is a single entry of "rclone lsjson" output.
type rcloneItem struct {
	Name    string    `json:"Name"`
	Size    int64     `json:"Size"`
	ModTime time.Time `json:"ModTime"`
	IsDir   bool      `json:"IsDir"`
}

func (m rcloneItem) meta() S3ObjectMeta {
	size := m.Size
	if size < 0 {
		// rclone reports -1 for unknown sizes; store 0, such files are then
		// re-read on every run, which is safe
		size = 0
	}
	return S3ObjectMeta{Size: size, ModTime: m.ModTime}
}

// path returns the rclone location for a key below the remote.
func (a *rcloneAPI) path(key string) string {
	if key == "" {
		return a.remote
	}
	if strings.HasSuffix(a.remote, ":") {
		return a.remote + key
	}
	return a.remote + "/" + key
}

// rclone exit codes for missing files and directories, see "rclone help" /
// docs "Exit Code".
const (
	rcloneExitDirNotFound  = 3
	rcloneExitFileNotFound = 4
)

// run executes rclone with the given arguments and returns its stdout.
// Missing paths are reported as os.ErrNotExist.
func (a *rcloneAPI) run(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, a.program, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code := exitErr.ExitCode()
			if code == rcloneExitDirNotFound || code == rcloneExitFileNotFound {
				return nil, fmt.Errorf("rclone %v: %w", args[0], os.ErrNotExist)
			}
		}
		return nil, fmt.Errorf("rclone %v failed: %w: %v", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// stat runs "rclone lsjson --stat" for the given key.
func (a *rcloneAPI) stat(ctx context.Context, key string) (rcloneItem, error) {
	out, err := a.run(ctx, "lsjson", "--stat", a.path(key))
	if err != nil {
		return rcloneItem{}, err
	}
	var item rcloneItem
	if err := json.Unmarshal(out, &item); err != nil {
		return rcloneItem{}, fmt.Errorf("rclone lsjson --stat %q: invalid output: %w", key, err)
	}
	return item, nil
}

func (a *rcloneAPI) Stat(ctx context.Context, key string) (S3ObjectMeta, error) {
	item, err := a.stat(ctx, key)
	if err != nil {
		return S3ObjectMeta{}, err
	}
	if item.IsDir {
		// not a file; the caller detects directories via HasPrefix
		return S3ObjectMeta{}, fmt.Errorf("%q is a directory: %w", key, os.ErrNotExist)
	}
	return item.meta(), nil
}

func (a *rcloneAPI) HasPrefix(ctx context.Context, prefix string) (bool, error) {
	item, err := a.stat(ctx, strings.TrimSuffix(prefix, "/"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return item.IsDir, nil
}

func (a *rcloneAPI) List(ctx context.Context, prefix string) ([]S3DirEntry, error) {
	out, err := a.run(ctx, "lsjson", a.path(strings.TrimSuffix(prefix, "/")))
	if err != nil {
		return nil, err
	}
	var items []rcloneItem
	if err := json.Unmarshal(out, &items); err != nil {
		return nil, fmt.Errorf("rclone lsjson %q: invalid output: %w", prefix, err)
	}

	entries := make([]S3DirEntry, 0, len(items))
	for _, item := range items {
		entries = append(entries, S3DirEntry{
			Name:  item.Name,
			IsDir: item.IsDir,
			Meta:  item.meta(),
		})
	}
	return entries, nil
}

func (a *rcloneAPI) Get(ctx context.Context, key string) (io.ReadCloser, S3ObjectMeta, error) {
	item, err := a.stat(ctx, key)
	if err != nil {
		return nil, S3ObjectMeta{}, err
	}
	if item.IsDir {
		return nil, S3ObjectMeta{}, fmt.Errorf("%q is a directory: %w", key, os.ErrNotExist)
	}

	cmd := exec.CommandContext(ctx, a.program, "cat", a.path(key))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, S3ObjectMeta{}, err
	}
	if err := cmd.Start(); err != nil {
		return nil, S3ObjectMeta{}, fmt.Errorf("rclone cat %q: %w", key, err)
	}
	return &rcloneReader{cmd: cmd, rd: stdout, stderr: &stderr}, item.meta(), nil
}

// rcloneReader streams the stdout of a running "rclone cat". A stream that
// ends because rclone failed must NOT look like a regular EOF, otherwise a
// silently truncated file would be stored; Read only reports EOF after the
// process exited successfully.
type rcloneReader struct {
	cmd    *exec.Cmd
	rd     io.ReadCloser
	stderr *bytes.Buffer
	waited bool
	// terminal result of the stream, returned by all Read calls after the
	// end was reached; Wait() closes the pipe, so reading again would
	// otherwise yield "file already closed" instead of io.EOF
	err error
}

func (r *rcloneReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	n, err := r.rd.Read(p)
	if err == io.EOF {
		r.waited = true
		if werr := r.cmd.Wait(); werr != nil {
			err = fmt.Errorf("rclone cat failed: %w: %v", werr, strings.TrimSpace(r.stderr.String()))
		}
		r.err = err
	}
	return n, err
}

func (r *rcloneReader) Close() error {
	if !r.waited {
		// the file was not read to the end, stop the process
		r.waited = true
		_ = r.cmd.Process.Kill()
		_ = r.rd.Close()
		_ = r.cmd.Wait()
	}
	// cmd.Wait() already closed the stdout pipe
	return nil
}

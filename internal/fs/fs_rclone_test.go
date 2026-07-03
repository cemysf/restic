package fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	rtest "github.com/restic/restic/internal/test"
)

func newTestRcloneFS(t *testing.T) (FS, string) {
	t.Helper()
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skipf("rclone binary not found: %v", err)
	}

	// a plain local directory is a valid rclone remote
	tempdir := t.TempDir()
	rtest.OK(t, os.MkdirAll(filepath.Join(tempdir, "dir", "sub"), 0755))
	rtest.OK(t, os.MkdirAll(filepath.Join(tempdir, "emptydir"), 0755))
	rtest.OK(t, os.WriteFile(filepath.Join(tempdir, "file.txt"), []byte("hello world"), 0644))
	rtest.OK(t, os.WriteFile(filepath.Join(tempdir, "dir", "nested.txt"), []byte("nested content"), 0644))
	rtest.OK(t, os.WriteFile(filepath.Join(tempdir, "dir", "sub", "deep.txt"), []byte("deep content"), 0644))

	fs, err := NewRclone(context.Background(), RcloneOptions{Remote: tempdir})
	rtest.OK(t, err)
	return fs, tempdir
}

func TestRcloneFSLstat(t *testing.T) {
	fs, tempdir := newTestRcloneFS(t)

	fi, err := fs.Lstat("/")
	rtest.OK(t, err)
	rtest.Assert(t, fi.Mode.IsDir(), "root is not a directory: %v", fi.Mode)

	fi, err = fs.Lstat("/file.txt")
	rtest.OK(t, err)
	rtest.Assert(t, fi.Mode.IsRegular(), "expected regular file, got %v", fi.Mode)
	rtest.Equals(t, int64(len("hello world")), fi.Size)
	rtest.Assert(t, fi.Inode != 0, "inode is zero")

	// mtime must match the real file, truncated to whole seconds
	st, err := os.Stat(filepath.Join(tempdir, "file.txt"))
	rtest.OK(t, err)
	rtest.Equals(t, st.ModTime().Truncate(time.Second).UTC(), fi.ModTime.UTC())

	fi, err = fs.Lstat("/dir")
	rtest.OK(t, err)
	rtest.Assert(t, fi.Mode.IsDir(), "expected directory, got %v", fi.Mode)

	_, err = fs.Lstat("/does/not/exist")
	rtest.Assert(t, err != nil && errors.Is(err, os.ErrNotExist), "expected ErrNotExist, got %v", err)
}

func TestRcloneFSReaddirnames(t *testing.T) {
	fs, _ := newTestRcloneFS(t)

	readdir := func(name string) []string {
		f, err := fs.OpenFile(name, O_RDONLY|O_DIRECTORY, false)
		rtest.OK(t, err)
		entries, err := f.Readdirnames(-1)
		rtest.OK(t, err)
		rtest.OK(t, f.Close())
		sort.Strings(entries)
		return entries
	}

	rtest.Equals(t, []string{"dir", "emptydir", "file.txt"}, readdir("/"))
	rtest.Equals(t, []string{"nested.txt", "sub"}, readdir("/dir"))
	// unlike S3 prefixes, rclone reports real empty directories
	rtest.Equals(t, 0, len(readdir("/emptydir")))
}

func TestRcloneFSReadFile(t *testing.T) {
	fs, _ := newTestRcloneFS(t)

	f, err := fs.OpenFile("/dir/nested.txt", O_NOFOLLOW, true)
	rtest.OK(t, err)

	fi, err := f.Stat()
	rtest.OK(t, err)
	rtest.Equals(t, int64(len("nested content")), fi.Size)

	rtest.OK(t, f.MakeReadable())
	content, err := io.ReadAll(f)
	rtest.OK(t, err)
	rtest.Equals(t, "nested content", string(content))
	rtest.OK(t, f.Close())

	node, err := f.ToNode(false, nil)
	rtest.OK(t, err)
	rtest.Equals(t, uint64(fi.Size), node.Size)
	rtest.Equals(t, fi.Inode, node.Inode)
}

func TestRcloneFSInodeStability(t *testing.T) {
	fs1, _ := newTestRcloneFS(t)

	fi1, err := fs1.Lstat("/dir/nested.txt")
	rtest.OK(t, err)

	// the inode depends only on the path, not on the directory contents, so
	// a second FS over a different directory yields the same value
	fs2, _ := newTestRcloneFS(t)
	fi2, err := fs2.Lstat("/dir/nested.txt")
	rtest.OK(t, err)

	rtest.Equals(t, fi1.Inode, fi2.Inode)
}

// TestRcloneReaderFailure ensures that an "rclone cat" process dying before
// the file was fully streamed surfaces as a read error instead of a clean
// EOF; otherwise a truncated file would be silently stored.
func TestRcloneReaderFailure(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skipf("sh not found: %v", err)
	}

	newReader := func(script string) *rcloneReader {
		cmd := exec.Command("sh", "-c", script)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		stdout, err := cmd.StdoutPipe()
		rtest.OK(t, err)
		rtest.OK(t, cmd.Start())
		return &rcloneReader{cmd: cmd, rd: stdout, stderr: &stderr}
	}

	// process fails after partial output: EOF must be converted to an error
	r := newReader("printf partial; exit 1")
	content, err := io.ReadAll(r)
	rtest.Equals(t, "partial", string(content))
	rtest.Assert(t, err != nil, "expected read error from failing process")
	rtest.Assert(t, strings.Contains(err.Error(), "rclone cat failed"), "unexpected error: %v", err)
	rtest.OK(t, r.Close())

	// successful process: regular EOF
	r = newReader("printf complete")
	content, err = io.ReadAll(r)
	rtest.OK(t, err)
	rtest.Equals(t, "complete", string(content))

	// the chunker reads again after EOF; that must stay a clean EOF even
	// though Wait() has closed the pipe by now
	n, err := r.Read(make([]byte, 1))
	rtest.Equals(t, 0, n)
	rtest.Equals(t, io.EOF, err)
	rtest.OK(t, r.Close())

	// closing before EOF must terminate the process
	r = newReader("printf start; sleep 60")
	buf := make([]byte, 5)
	_, err = io.ReadFull(r, buf)
	rtest.OK(t, err)
	rtest.OK(t, r.Close())
}

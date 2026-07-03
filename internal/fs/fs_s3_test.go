package fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	rtest "github.com/restic/restic/internal/test"
)

type fakeS3Object struct {
	data    []byte
	modTime time.Time
}

// fakeS3API is an in-memory S3API implementation with S3 list semantics
// (lexicographic key order, delimiter "/").
type fakeS3API struct {
	mu      sync.Mutex
	objects map[string]fakeS3Object

	statCalls int
	listCalls int
	getCalls  int
}

func (a *fakeS3API) Stat(_ context.Context, key string) (S3ObjectMeta, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.statCalls++

	obj, ok := a.objects[key]
	if !ok {
		return S3ObjectMeta{}, fmt.Errorf("%q: %w", key, os.ErrNotExist)
	}
	return S3ObjectMeta{Size: int64(len(obj.data)), ModTime: obj.modTime}, nil
}

func (a *fakeS3API) HasPrefix(_ context.Context, prefix string) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for key := range a.objects {
		if strings.HasPrefix(key, prefix) {
			return true, nil
		}
	}
	return false, nil
}

func (a *fakeS3API) List(_ context.Context, prefix string) ([]S3DirEntry, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.listCalls++

	keys := make([]string, 0, len(a.objects))
	for key := range a.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var entries []S3DirEntry
	seenDirs := make(map[string]struct{})
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) || key == prefix {
			continue
		}
		rest := strings.TrimPrefix(key, prefix)
		if idx := strings.Index(rest, "/"); idx >= 0 {
			// key below a common prefix, collapsed into a directory entry
			name := rest[:idx]
			if _, ok := seenDirs[name]; !ok {
				seenDirs[name] = struct{}{}
				entries = append(entries, S3DirEntry{Name: name, IsDir: true})
			}
		} else {
			obj := a.objects[key]
			entries = append(entries, S3DirEntry{
				Name: rest,
				Meta: S3ObjectMeta{Size: int64(len(obj.data)), ModTime: obj.modTime},
			})
		}
	}
	return entries, nil
}

func (a *fakeS3API) Get(_ context.Context, key string) (io.ReadCloser, S3ObjectMeta, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.getCalls++

	obj, ok := a.objects[key]
	if !ok {
		return nil, S3ObjectMeta{}, fmt.Errorf("%q: %w", key, os.ErrNotExist)
	}
	return io.NopCloser(bytes.NewReader(obj.data)), S3ObjectMeta{Size: int64(len(obj.data)), ModTime: obj.modTime}, nil
}

// deliberately carries a sub-second component: listings report millisecond
// precision, HEAD/GET only whole seconds, and the FS must normalize both
var testS3ModTime = time.Date(2026, 7, 1, 12, 0, 0, 123e6, time.UTC)

// testS3WantModTime is the timestamp the FS must report for testS3ModTime.
var testS3WantModTime = testS3ModTime.Truncate(time.Second)

func newTestS3FS(t testing.TB, objects map[string]string) (*s3FS, *fakeS3API) {
	t.Helper()
	api := &fakeS3API{objects: make(map[string]fakeS3Object)}
	for key, content := range objects {
		api.objects[key] = fakeS3Object{data: []byte(content), modTime: testS3ModTime}
	}
	return newS3FS(context.Background(), api, "testbucket"), api
}

func testS3Objects() map[string]string {
	return map[string]string{
		"file.txt":              "hello world",
		"dir/nested.txt":        "nested content",
		"dir/sub/deep.txt":      "deep content",
		"emptydir/":             "", // zero-byte directory marker
		"other/data.bin":        "some data",
		"prefix/tenant/obj.txt": "tenant object",
	}
}

func TestS3FSLstat(t *testing.T) {
	fs, _ := newTestS3FS(t, testS3Objects())

	// root is a directory
	fi, err := fs.Lstat("/")
	rtest.OK(t, err)
	rtest.Assert(t, fi.Mode.IsDir(), "root is not a directory: %v", fi.Mode)

	// regular file
	fi, err = fs.Lstat("/file.txt")
	rtest.OK(t, err)
	rtest.Assert(t, fi.Mode.IsRegular(), "expected regular file, got %v", fi.Mode)
	rtest.Equals(t, int64(len("hello world")), fi.Size)
	rtest.Equals(t, testS3WantModTime, fi.ModTime)
	rtest.Equals(t, "file.txt", fi.Name)
	rtest.Assert(t, fi.Inode != 0, "inode is zero")
	rtest.Assert(t, fi.DeviceID != 0, "device ID is zero")

	// implicit directory (prefix without marker object)
	fi, err = fs.Lstat("/dir")
	rtest.OK(t, err)
	rtest.Assert(t, fi.Mode.IsDir(), "expected directory, got %v", fi.Mode)

	// directory from zero-byte marker object
	fi, err = fs.Lstat("/emptydir")
	rtest.OK(t, err)
	rtest.Assert(t, fi.Mode.IsDir(), "expected directory, got %v", fi.Mode)

	// missing path; the archiver relies on ErrNotExist to detect files that
	// disappeared between readdir and open
	_, err = fs.Lstat("/does/not/exist")
	rtest.Assert(t, err != nil, "expected error for missing path")
	rtest.Assert(t, errors.Is(err, os.ErrNotExist), "expected ErrNotExist, got %v", err)
}

func TestS3FSReaddirnames(t *testing.T) {
	fs, _ := newTestS3FS(t, testS3Objects())

	readdir := func(name string) []string {
		f, err := fs.OpenFile(name, O_RDONLY|O_DIRECTORY, false)
		rtest.OK(t, err)
		entries, err := f.Readdirnames(-1)
		rtest.OK(t, err)
		rtest.OK(t, f.Close())
		sort.Strings(entries)
		return entries
	}

	rtest.Equals(t, []string{"dir", "emptydir", "file.txt", "other", "prefix"}, readdir("/"))
	rtest.Equals(t, []string{"nested.txt", "sub"}, readdir("/dir"))
	rtest.Equals(t, []string{"deep.txt"}, readdir("/dir/sub"))
	rtest.Equals(t, 0, len(readdir("/emptydir")))
}

func TestS3FSReadFile(t *testing.T) {
	fs, _ := newTestS3FS(t, testS3Objects())

	// open metadata-only first, like the archiver does
	f, err := fs.OpenFile("/dir/nested.txt", O_NOFOLLOW, true)
	rtest.OK(t, err)

	fi, err := f.Stat()
	rtest.OK(t, err)
	rtest.Equals(t, int64(len("nested content")), fi.Size)

	// reading a metadata-only file must fail
	_, err = f.Read(make([]byte, 1))
	rtest.Assert(t, err != nil, "expected error reading metadata-only file")

	rtest.OK(t, f.MakeReadable())
	content, err := io.ReadAll(f)
	rtest.OK(t, err)
	rtest.Equals(t, "nested content", string(content))
	rtest.OK(t, f.Close())

	// node metadata must be consistent with Stat
	node, err := f.ToNode(false, nil)
	rtest.OK(t, err)
	rtest.Equals(t, uint64(fi.Size), node.Size)
	rtest.Equals(t, fi.ModTime, node.ModTime)
	rtest.Equals(t, fi.Inode, node.Inode)
	rtest.Equals(t, "nested.txt", node.Name)
}

func TestS3FSMetadataCache(t *testing.T) {
	fs, api := newTestS3FS(t, testS3Objects())

	// listing a directory must cache the metadata of its entries, so that
	// the per-entry Lstat/OpenFile calls do not cause HEAD requests
	f, err := fs.OpenFile("/dir", O_RDONLY|O_DIRECTORY, false)
	rtest.OK(t, err)
	_, err = f.Readdirnames(-1)
	rtest.OK(t, err)
	rtest.OK(t, f.Close())

	before := api.statCalls
	fi, err := fs.Lstat("/dir/nested.txt")
	rtest.OK(t, err)
	rtest.Equals(t, int64(len("nested content")), fi.Size)

	child, err := fs.OpenFile("/dir/nested.txt", O_NOFOLLOW, true)
	rtest.OK(t, err)
	rtest.OK(t, child.Close())

	rtest.Equals(t, before, api.statCalls)
}

func TestS3FSInodeStability(t *testing.T) {
	// inodes must be identical across independent FS instances, otherwise
	// restic would consider every object changed on the next run
	fs1, _ := newTestS3FS(t, testS3Objects())
	fs2, _ := newTestS3FS(t, testS3Objects())

	fi1, err := fs1.Lstat("/dir/nested.txt")
	rtest.OK(t, err)
	fi2, err := fs2.Lstat("/dir/nested.txt")
	rtest.OK(t, err)

	rtest.Equals(t, fi1.Inode, fi2.Inode)
	rtest.Equals(t, fi1.DeviceID, fi2.DeviceID)

	other, err := fs1.Lstat("/file.txt")
	rtest.OK(t, err)
	rtest.Assert(t, other.Inode != fi1.Inode, "different objects share an inode")
}

func TestS3FSInodeCollision(t *testing.T) {
	fs, _ := newTestS3FS(t, testS3Objects())

	// simulate a hash collision: another path already claimed the inode
	// value of /file.txt
	fs.mu.Lock()
	fs.inodes[s3PathHash("/file.txt")] = "/colliding/other"
	fs.mu.Unlock()

	_, err := fs.Lstat("/file.txt")
	rtest.Assert(t, err != nil, "expected inode collision to fail loudly")
	rtest.Assert(t, strings.Contains(err.Error(), "collision"), "unexpected error: %v", err)
}

func TestS3FSChangedObject(t *testing.T) {
	fs, api := newTestS3FS(t, testS3Objects())

	f, err := fs.OpenFile("/file.txt", O_NOFOLLOW, true)
	rtest.OK(t, err)
	fi, err := f.Stat()
	rtest.OK(t, err)
	rtest.Equals(t, int64(len("hello world")), fi.Size)

	// object changes between the metadata fetch and the read
	newModTime := testS3ModTime.Add(time.Hour)
	api.mu.Lock()
	api.objects["file.txt"] = fakeS3Object{data: []byte("changed!"), modTime: newModTime}
	api.mu.Unlock()

	rtest.OK(t, f.MakeReadable())

	// after opening for reading, the metadata must reflect the version
	// that is actually read
	fi, err = f.Stat()
	rtest.OK(t, err)
	rtest.Equals(t, int64(len("changed!")), fi.Size)
	rtest.Equals(t, newModTime.Truncate(time.Second), fi.ModTime)

	content, err := io.ReadAll(f)
	rtest.OK(t, err)
	rtest.Equals(t, "changed!", string(content))
	rtest.OK(t, f.Close())
}

func TestS3FSDuplicateFileAndPrefix(t *testing.T) {
	// a key that exists both as an object and as a prefix of other objects
	fs, _ := newTestS3FS(t, map[string]string{
		"dup":       "object content",
		"dup/child": "child content",
	})

	f, err := fs.OpenFile("/", O_RDONLY|O_DIRECTORY, false)
	rtest.OK(t, err)
	entries, err := f.Readdirnames(-1)
	rtest.OK(t, err)
	rtest.OK(t, f.Close())

	// the object wins, the prefix is skipped
	rtest.Equals(t, []string{"dup"}, entries)

	fi, err := fs.Lstat("/dup")
	rtest.OK(t, err)
	rtest.Assert(t, fi.Mode.IsRegular(), "expected regular file, got %v", fi.Mode)
}

func TestS3FSOpenFileFlags(t *testing.T) {
	fs, _ := newTestS3FS(t, testS3Objects())

	// unsupported flags must be rejected
	_, err := fs.OpenFile("/file.txt", os.O_WRONLY, false)
	rtest.Assert(t, err != nil, "expected error for O_WRONLY")

	// O_DIRECTORY on a regular file must fail
	_, err = fs.OpenFile("/file.txt", O_RDONLY|O_DIRECTORY, false)
	rtest.Assert(t, err != nil, "expected error for O_DIRECTORY on file")
}

func TestS3FSPathOperations(t *testing.T) {
	fs, _ := newTestS3FS(t, nil)

	rtest.Equals(t, "/", fs.Separator())
	rtest.Equals(t, "", fs.VolumeName("/a/b"))
	rtest.Assert(t, fs.IsAbs("anything"), "IsAbs must always be true")
	rtest.Equals(t, "/a/b", fs.Join("/a", "b"))
	rtest.Equals(t, "/a", fs.Dir("/a/b"))
	rtest.Equals(t, "b", fs.Base("/a/b"))
	rtest.Equals(t, "/a/b", fs.Clean("/a//b/."))

	abs, err := fs.Abs("a/b")
	rtest.OK(t, err)
	rtest.Equals(t, "/a/b", abs)
}

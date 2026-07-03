package archiver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

type fakeS3Object struct {
	data    []byte
	modTime time.Time
}

// fakeS3 is an in-memory fs.S3API with S3 list semantics (lexicographic key
// order, delimiter "/"). It counts GetObject calls so tests can assert that
// unchanged objects are not read again.
type fakeS3 struct {
	mu       sync.Mutex
	objects  map[string]fakeS3Object
	getCalls int
}

func (a *fakeS3) put(key, content string, modTime time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.objects[key] = fakeS3Object{data: []byte(content), modTime: modTime}
}

func (a *fakeS3) gets() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.getCalls
}

func (a *fakeS3) Stat(_ context.Context, key string) (fs.S3ObjectMeta, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	obj, ok := a.objects[key]
	if !ok {
		return fs.S3ObjectMeta{}, fmt.Errorf("%q: %w", key, os.ErrNotExist)
	}
	return fs.S3ObjectMeta{Size: int64(len(obj.data)), ModTime: obj.modTime}, nil
}

func (a *fakeS3) HasPrefix(_ context.Context, prefix string) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for key := range a.objects {
		if strings.HasPrefix(key, prefix) {
			return true, nil
		}
	}
	return false, nil
}

func (a *fakeS3) List(_ context.Context, prefix string) ([]fs.S3DirEntry, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	keys := make([]string, 0, len(a.objects))
	for key := range a.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var entries []fs.S3DirEntry
	seenDirs := make(map[string]struct{})
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) || key == prefix {
			continue
		}
		rest := strings.TrimPrefix(key, prefix)
		if idx := strings.Index(rest, "/"); idx >= 0 {
			name := rest[:idx]
			if _, ok := seenDirs[name]; !ok {
				seenDirs[name] = struct{}{}
				entries = append(entries, fs.S3DirEntry{Name: name, IsDir: true})
			}
		} else {
			obj := a.objects[key]
			entries = append(entries, fs.S3DirEntry{
				Name: rest,
				Meta: fs.S3ObjectMeta{Size: int64(len(obj.data)), ModTime: obj.modTime},
			})
		}
	}
	return entries, nil
}

func (a *fakeS3) Get(_ context.Context, key string) (io.ReadCloser, fs.S3ObjectMeta, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.getCalls++

	obj, ok := a.objects[key]
	if !ok {
		return nil, fs.S3ObjectMeta{}, fmt.Errorf("%q: %w", key, os.ErrNotExist)
	}
	// like the real S3 API: the Last-Modified header of a GET response only
	// has whole-second granularity, while listings report milliseconds
	return io.NopCloser(bytes.NewReader(obj.data)), fs.S3ObjectMeta{Size: int64(len(obj.data)), ModTime: obj.modTime.Truncate(time.Second)}, nil
}

// TestArchiverS3Source backs up an S3 bucket end-to-end and verifies restic's
// change detection on the synthetic S3 metadata: unchanged objects must not
// be read again on the next run, changed objects must be re-read.
func TestArchiverS3Source(t *testing.T) {
	// sub-second component on purpose: listings report it, GET responses
	// don't, and change detection must not be confused by the difference
	modTime := time.Date(2026, 7, 1, 12, 0, 0, 123e6, time.UTC)
	api := &fakeS3{objects: map[string]fakeS3Object{}}
	api.put("data/a.txt", "object a", modTime)
	api.put("data/sub/b.txt", "object b", modTime)
	api.put("data/sub/c.txt", "object c", modTime)

	repo := repository.TestRepository(t)
	ctx := context.Background()

	snapshotS3 := func(parent *data.Snapshot) (*data.Snapshot, *Summary) {
		// a fresh FS instance per run, like separate restic invocations
		testFS := fs.NewS3WithAPI(ctx, "testbucket", api)
		arch := New(repo, testFS, Options{})
		sn, _, summary, err := arch.Snapshot(ctx, []string{"/data"}, SnapshotOptions{
			Time:           time.Now(),
			ParentSnapshot: parent,
		})
		rtest.OK(t, err)
		return sn, summary
	}

	// first run reads everything
	sn1, sum1 := snapshotS3(nil)
	rtest.Equals(t, uint(3), sum1.Files.New)
	rtest.Equals(t, 3, api.gets())

	// second run: nothing changed, no object may be read again
	sn2, sum2 := snapshotS3(sn1)
	rtest.Equals(t, uint(3), sum2.Files.Unchanged)
	rtest.Equals(t, uint(0), sum2.Files.New+sum2.Files.Changed)
	rtest.Equals(t, 3, api.gets())
	rtest.Equals(t, *sn1.Tree, *sn2.Tree)

	// third run: one object changed, exactly that object is re-read
	api.put("data/a.txt", "object a, now changed", modTime.Add(time.Hour))
	sn3, sum3 := snapshotS3(sn2)
	rtest.Equals(t, uint(1), sum3.Files.Changed)
	rtest.Equals(t, uint(2), sum3.Files.Unchanged)
	rtest.Equals(t, 4, api.gets())
	rtest.Assert(t, *sn3.Tree != *sn2.Tree, "expected changed tree")

	// verify the stored content of the changed object
	tree, err := data.LoadTree(ctx, repo, *sn3.Tree)
	rtest.OK(t, err)
	dataNode := findNode(t, tree, "data")
	subtree, err := data.LoadTree(ctx, repo, *dataNode.Subtree)
	rtest.OK(t, err)
	fileNode := findNode(t, subtree, "a.txt")

	var content []byte
	for _, id := range fileNode.Content {
		buf, err := repo.LoadBlob(ctx, restic.BlobHandle{Type: restic.DataBlob, ID: id}, nil)
		rtest.OK(t, err)
		content = append(content, buf...)
	}
	rtest.Equals(t, "object a, now changed", string(content))
}

func findNode(t testing.TB, tree data.TreeNodeIterator, name string) *data.Node {
	t.Helper()
	finder := data.NewTreeFinder(tree)
	defer finder.Close()
	node, err := finder.Find(name)
	rtest.OK(t, err)
	rtest.Assert(t, node != nil, "node %q not found", name)
	return node
}

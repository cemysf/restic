package main

import (
	"encoding/xml"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/restic/restic/internal/data"
	rtest "github.com/restic/restic/internal/test"
)

type fakeS3Object struct {
	data    []byte
	modTime time.Time
}

// fakeS3Server implements the read-only subset of the S3 HTTP API used when
// backing up from an S3 source: GetBucketLocation, ListObjectsV2, HeadObject
// and GetObject. Authentication is not checked.
type fakeS3Server struct {
	mu      sync.Mutex
	bucket  string
	objects map[string]fakeS3Object
}

func (s *fakeS3Server) put(key, content string, modTime time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = fakeS3Object{data: []byte(content), modTime: modTime}
}

type fakeS3ListEntry struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type fakeS3CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type fakeS3ListResult struct {
	XMLName        xml.Name             `xml:"ListBucketResult"`
	Xmlns          string               `xml:"xmlns,attr"`
	Name           string               `xml:"Name"`
	Prefix         string               `xml:"Prefix"`
	Delimiter      string               `xml:"Delimiter,omitempty"`
	KeyCount       int                  `xml:"KeyCount"`
	MaxKeys        int                  `xml:"MaxKeys"`
	IsTruncated    bool                 `xml:"IsTruncated"`
	Contents       []fakeS3ListEntry    `xml:"Contents"`
	CommonPrefixes []fakeS3CommonPrefix `xml:"CommonPrefixes"`
}

func (s *fakeS3Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := strings.TrimPrefix(r.URL.Path, "/")
	q := r.URL.Query()

	// GetBucketLocation
	if _, ok := q["location"]; ok {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
		return
	}

	// ListObjectsV2
	if strings.TrimSuffix(path, "/") == s.bucket && q.Get("list-type") == "2" {
		prefix := q.Get("prefix")
		delimiter := q.Get("delimiter")
		res := fakeS3ListResult{
			Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
			Name:  s.bucket, Prefix: prefix, Delimiter: delimiter, MaxKeys: 1000,
		}
		keys := make([]string, 0, len(s.objects))
		for k := range s.objects {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		seen := map[string]bool{}
		for _, k := range keys {
			if !strings.HasPrefix(k, prefix) {
				continue
			}
			rest := strings.TrimPrefix(k, prefix)
			if delimiter != "" {
				if idx := strings.Index(rest, delimiter); idx >= 0 {
					cp := prefix + rest[:idx+len(delimiter)]
					if !seen[cp] {
						seen[cp] = true
						res.CommonPrefixes = append(res.CommonPrefixes, fakeS3CommonPrefix{Prefix: cp})
					}
					continue
				}
			}
			obj := s.objects[k]
			res.Contents = append(res.Contents, fakeS3ListEntry{
				Key: k, LastModified: obj.modTime.UTC().Format("2006-01-02T15:04:05.000Z"),
				ETag: `"etag"`, Size: int64(len(obj.data)), StorageClass: "STANDARD",
			})
		}
		res.KeyCount = len(res.Contents) + len(res.CommonPrefixes)
		w.Header().Set("Content-Type", "application/xml")
		out, err := xml.Marshal(res)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(xml.Header))
		_, _ = w.Write(out)
		return
	}

	// HeadObject / GetObject
	key := strings.TrimPrefix(path, s.bucket+"/")
	obj, ok := s.objects[key]
	if !ok {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>not found</Message><Key>%s</Key></Error>`, key)
		return
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(obj.data)))
	w.Header().Set("Last-Modified", obj.modTime.UTC().Format(http.TimeFormat))
	w.Header().Set("ETag", `"etag"`)
	w.Header().Set("Content-Type", "application/octet-stream")
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(obj.data)
}

func startFakeS3Server(t *testing.T, srv *fakeS3Server) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot listen on localhost: %v", err)
	}
	httpServer := &http.Server{Handler: srv}
	go func() { _ = httpServer.Serve(ln) }()
	t.Cleanup(func() { _ = httpServer.Close() })
	return ln.Addr().String()
}

func TestBackupS3Source(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, env.gopts)

	modTime := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	srv := &fakeS3Server{bucket: "srcbucket", objects: map[string]fakeS3Object{}}
	srv.put("tenant/a.txt", "object a content", modTime)
	srv.put("tenant/dir/b.txt", "object b content", modTime)
	srv.put("other/ignored.txt", "outside the backup prefix", modTime)
	endpoint := startFakeS3Server(t, srv)

	t.Setenv("RESTIC_SOURCE_ACCESS_KEY_ID", "testkey")
	t.Setenv("RESTIC_SOURCE_SECRET_ACCESS_KEY", "testsecret")
	source := fmt.Sprintf("s3:http://%s/srcbucket/tenant", endpoint)

	backup := func(wantSummary string) {
		t.Helper()
		gopts := env.gopts
		gopts.Quiet = false
		gopts.Verbosity = 1 // the summary line is only printed at normal verbosity
		out, err := testRunBackupOutput(t, BackupOptions{
			GroupBy: data.SnapshotGroupByOptions{Host: true, Path: true},
		}, gopts, []string{source})
		rtest.OK(t, err)
		matched, err := regexp.Match(wantSummary, out)
		rtest.OK(t, err)
		rtest.Assert(t, matched, "summary %q not found in output:\n%s", wantSummary, out)
	}

	// first backup reads everything
	backup(`2 new,\s+0 changed,\s+0 unmodified`)
	testListSnapshots(t, env.gopts, 1)

	// nothing changed: everything is skipped via the parent snapshot
	backup(`0 new,\s+0 changed,\s+2 unmodified`)

	// exactly the changed object is re-read
	srv.put("tenant/a.txt", "object a CHANGED content", modTime.Add(time.Hour))
	backup(`0 new,\s+1 changed,\s+1 unmodified`)
	testListSnapshots(t, env.gopts, 3)

	// restore the last snapshot and verify the contents
	restoreDir := filepath.Join(env.base, "restore")
	testRunRestore(t, env.gopts, restoreDir, "latest")

	for name, want := range map[string]string{
		"a.txt":     "object a CHANGED content",
		"dir/b.txt": "object b content",
	} {
		content, err := os.ReadFile(filepath.Join(restoreDir, "tenant", filepath.FromSlash(name)))
		rtest.OK(t, err)
		rtest.Equals(t, want, string(content))
	}

	// the object outside the prefix must not be part of the snapshot
	_, err := os.Stat(filepath.Join(restoreDir, "other"))
	rtest.Assert(t, os.IsNotExist(err), "object outside prefix was backed up")

	testRunCheck(t, env.gopts)
}

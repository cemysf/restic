package fs

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
)

// S3Options collects all parameters needed to open an S3 bucket as a backup
// source file system.
type S3Options struct {
	Endpoint string
	Bucket   string
	UseHTTP  bool
	Region   string

	// static credentials; if unset, the default credential chain
	// (environment, credential files, IAM) is used
	KeyID        string
	Secret       string
	SessionToken string

	Transport http.RoundTripper
}

// NewS3 returns a read-only FS that exposes the contents of an S3 bucket as a
// file tree. Object keys are interpreted as /-separated paths, key prefixes
// as directories. It is intended as a backup source, allowing restic to back
// up objects from an S3-compatible server without mounting it first.
func NewS3(ctx context.Context, opts S3Options) (FS, error) {
	if opts.Endpoint == "" {
		return nil, errors.Fatal("s3 source: endpoint not specified")
	}
	if opts.Bucket == "" {
		return nil, errors.Fatal("s3 source: bucket name not specified")
	}

	creds, err := s3SourceCredentials(opts)
	if err != nil {
		return nil, err
	}

	client, err := minio.New(opts.Endpoint, &minio.Options{
		Creds:     creds,
		Secure:    !opts.UseHTTP,
		Region:    opts.Region,
		Transport: opts.Transport,
	})
	if err != nil {
		return nil, errors.Wrap(err, "s3 source: minio.New")
	}

	return newS3FS(ctx, &minioS3API{client: client, bucket: opts.Bucket}, opts.Bucket), nil
}

// NewS3WithAPI returns an FS reading from the given S3API implementation. It
// is intended for tests that substitute an in-memory implementation for a
// real S3 server.
func NewS3WithAPI(ctx context.Context, bucket string, api S3API) FS {
	return newS3FS(ctx, api, bucket)
}

func s3SourceCredentials(opts S3Options) (*credentials.Credentials, error) {
	if opts.KeyID != "" || opts.Secret != "" {
		if opts.KeyID == "" || opts.Secret == "" {
			return nil, errors.Fatal("s3 source: either both or none of access key ID and secret access key must be set")
		}
		return credentials.NewStaticV4(opts.KeyID, opts.Secret, opts.SessionToken), nil
	}

	chain := credentials.NewChainCredentials([]credentials.Provider{
		&credentials.EnvAWS{},
		&credentials.EnvMinio{},
		&credentials.FileAWSCredentials{},
		&credentials.FileMinioClient{},
		&credentials.IAM{},
	})
	c, err := chain.GetWithContext(&credentials.CredContext{Client: &http.Client{Transport: opts.Transport}})
	if err != nil {
		return nil, errors.Wrap(err, "s3 source: creds.Get")
	}
	if c.SignerType == credentials.SignatureAnonymous {
		return nil, errors.Fatal("s3 source: no credentials found, set $RESTIC_SOURCE_ACCESS_KEY_ID and $RESTIC_SOURCE_SECRET_ACCESS_KEY (or $AWS_ACCESS_KEY_ID and $AWS_SECRET_ACCESS_KEY)")
	}
	return chain, nil
}

// S3ObjectMeta is the metadata subset of an S3 object needed for backup.
type S3ObjectMeta struct {
	Size    int64
	ModTime time.Time
}

// S3DirEntry is a single result of listing an S3 prefix non-recursively.
type S3DirEntry struct {
	Name  string // entry name relative to the listed prefix, without trailing slash
	IsDir bool
	Meta  S3ObjectMeta // zero value for directories
}

// S3API is the subset of S3 operations used by s3FS. It exists to allow
// testing with an in-memory implementation.
type S3API interface {
	// Stat returns the metadata of the object at key. The error is
	// os.ErrNotExist (possibly wrapped) if there is no such object.
	Stat(ctx context.Context, key string) (S3ObjectMeta, error)
	// HasPrefix reports whether at least one object exists below prefix.
	HasPrefix(ctx context.Context, prefix string) (bool, error)
	// List returns the entries directly below prefix, i.e. a non-recursive
	// (delimiter "/") listing. prefix is either empty or ends with a slash.
	List(ctx context.Context, prefix string) ([]S3DirEntry, error)
	// Get opens the object at key for reading and returns its metadata.
	Get(ctx context.Context, key string) (io.ReadCloser, S3ObjectMeta, error)
}

type minioS3API struct {
	client *minio.Client
	bucket string
}

func isMinioNotFound(err error) bool {
	var e minio.ErrorResponse
	return errors.As(err, &e) && (e.StatusCode == http.StatusNotFound || e.Code == "NoSuchKey")
}

func (a *minioS3API) Stat(ctx context.Context, key string) (S3ObjectMeta, error) {
	info, err := a.client.StatObject(ctx, a.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if isMinioNotFound(err) {
			return S3ObjectMeta{}, fmt.Errorf("%q: %w", key, os.ErrNotExist)
		}
		return S3ObjectMeta{}, err
	}
	return S3ObjectMeta{Size: info.Size, ModTime: info.LastModified}, nil
}

func (a *minioS3API) HasPrefix(ctx context.Context, prefix string) (bool, error) {
	listCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for obj := range a.client.ListObjects(listCtx, a.bucket, minio.ListObjectsOptions{Prefix: prefix, MaxKeys: 1}) {
		if obj.Err != nil {
			return false, obj.Err
		}
		return true, nil
	}
	return false, nil
}

func (a *minioS3API) List(ctx context.Context, prefix string) ([]S3DirEntry, error) {
	var entries []S3DirEntry
	// non-recursive listing uses the delimiter "/", so keys below a common
	// prefix are returned collapsed into a single entry with a trailing slash
	for obj := range a.client.ListObjects(ctx, a.bucket, minio.ListObjectsOptions{Prefix: prefix}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if obj.Key == prefix {
			// zero-byte directory marker object for the listed prefix itself
			continue
		}
		name := strings.TrimPrefix(obj.Key, prefix)
		if strings.HasSuffix(name, "/") {
			entries = append(entries, S3DirEntry{Name: strings.TrimSuffix(name, "/"), IsDir: true})
		} else {
			entries = append(entries, S3DirEntry{
				Name: name,
				Meta: S3ObjectMeta{Size: obj.Size, ModTime: obj.LastModified},
			})
		}
	}
	return entries, nil
}

func (a *minioS3API) Get(ctx context.Context, key string) (io.ReadCloser, S3ObjectMeta, error) {
	obj, err := a.client.GetObject(ctx, a.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, S3ObjectMeta{}, err
	}
	// Stat performs the actual request and returns the authoritative
	// metadata of the object version that will be read
	info, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		if isMinioNotFound(err) {
			err = fmt.Errorf("%q: %w", key, os.ErrNotExist)
		}
		return nil, S3ObjectMeta{}, err
	}
	return obj, S3ObjectMeta{Size: info.Size, ModTime: info.LastModified}, nil
}

const (
	s3DirMode  = os.ModeDir | 0755
	s3FileMode = os.FileMode(0644)
)

// s3DirModTime is used for all directories: prefixes have no metadata of
// their own in S3 and a fixed timestamp keeps unchanged directory nodes
// identical across runs.
var s3DirModTime = time.Unix(0, 0)

type s3FS struct {
	ctx      context.Context
	api      S3API
	bucket   string
	deviceID uint64

	mu        sync.Mutex
	metaCache map[string]*ExtendedFileInfo
	inodes    map[uint64]string
}

// statically ensure that s3FS implements FS.
var _ FS = &s3FS{}

func newS3FS(ctx context.Context, api S3API, bucket string) *s3FS {
	return &s3FS{
		ctx:       ctx,
		api:       api,
		bucket:    bucket,
		deviceID:  s3PathHash("bucket:" + bucket),
		metaCache: make(map[string]*ExtendedFileInfo),
		inodes:    make(map[uint64]string),
	}
}

func s3PathHash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func s3CleanPath(p string) string {
	return path.Clean("/" + p)
}

// s3Key returns the object key for a cleaned path, "" for the root.
func s3Key(cleanPath string) string {
	return strings.TrimPrefix(cleanPath, "/")
}

// inodeForPath derives a synthetic inode number from the path. S3 has no
// inode concept, but restic's change detection needs a value that is stable
// across runs for the same key. A collision would make two different objects
// indistinguishable to restic and must fail loudly instead of silently
// corrupting change detection.
func (fs *s3FS) inodeForPath(cleanPath string) (uint64, error) {
	inode := s3PathHash(cleanPath)
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if existing, ok := fs.inodes[inode]; ok && existing != cleanPath {
		return 0, errors.Fatalf("backup source: synthetic inode collision between %q and %q, aborting to avoid inconsistent change detection", existing, cleanPath)
	}
	fs.inodes[inode] = cleanPath
	return inode, nil
}

func (fs *s3FS) newFileInfo(cleanPath string, isDir bool, meta S3ObjectMeta) (*ExtendedFileInfo, error) {
	inode, err := fs.inodeForPath(cleanPath)
	if err != nil {
		return nil, err
	}
	fi := &ExtendedFileInfo{
		Name:     path.Base(cleanPath),
		DeviceID: fs.deviceID,
		Inode:    inode,
		Links:    1,
	}
	if isDir {
		fi.Mode = s3DirMode
		fi.ModTime = s3DirModTime
	} else {
		fi.Mode = s3FileMode
		fi.Size = meta.Size
		// S3 reports sub-second timestamps in listings, but only whole
		// seconds in the Last-Modified header of HEAD/GET responses.
		// Truncate so that the value is identical no matter which request
		// produced it, otherwise change detection would consider objects
		// with sub-second timestamps modified on every run.
		fi.ModTime = meta.ModTime.Truncate(time.Second)
	}
	fi.AccessTime = fi.ModTime
	fi.ChangeTime = fi.ModTime
	return fi, nil
}

func (fs *s3FS) cachedFileInfo(cleanPath string) (*ExtendedFileInfo, bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fi, ok := fs.metaCache[cleanPath]
	return fi, ok
}

func (fs *s3FS) cacheFileInfo(cleanPath string, fi *ExtendedFileInfo) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.metaCache[cleanPath] = fi
}

func (fs *s3FS) lstat(op, name string) (*ExtendedFileInfo, error) {
	p := s3CleanPath(name)
	if fi, ok := fs.cachedFileInfo(p); ok {
		return fi, nil
	}

	if p == "/" {
		fi, err := fs.newFileInfo(p, true, S3ObjectMeta{})
		if err != nil {
			return nil, err
		}
		fs.cacheFileInfo(p, fi)
		return fi, nil
	}

	key := s3Key(p)
	meta, err := fs.api.Stat(fs.ctx, key)
	if err == nil {
		fi, err := fs.newFileInfo(p, false, meta)
		if err != nil {
			return nil, err
		}
		fs.cacheFileInfo(p, fi)
		return fi, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, pathError(op, name, err)
	}

	// not an object, check whether the path is a "directory", i.e. a prefix
	// below which objects exist
	isDir, err := fs.api.HasPrefix(fs.ctx, key+"/")
	if err != nil {
		return nil, pathError(op, name, err)
	}
	if !isDir {
		return nil, pathError(op, name, os.ErrNotExist)
	}
	fi, err := fs.newFileInfo(p, true, S3ObjectMeta{})
	if err != nil {
		return nil, err
	}
	fs.cacheFileInfo(p, fi)
	return fi, nil
}

// Lstat returns the FileInfo structure describing the named file.
func (fs *s3FS) Lstat(name string) (*ExtendedFileInfo, error) {
	return fs.lstat("lstat", name)
}

func (fs *s3FS) OpenFile(name string, flag int, metadataOnly bool) (File, error) {
	if flag & ^(O_RDONLY|O_NOFOLLOW|O_DIRECTORY) != 0 {
		return nil, pathError("open", name,
			fmt.Errorf("invalid combination of flags 0x%x", flag))
	}

	fi, err := fs.lstat("open", name)
	if err != nil {
		return nil, err
	}
	if flag&O_DIRECTORY != 0 && !fi.Mode.IsDir() {
		return nil, pathError("open", name, syscall.ENOTDIR)
	}

	f := &s3File{fs: fs, path: s3CleanPath(name), fi: fi}
	if !metadataOnly {
		if err := f.MakeReadable(); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// readDir lists the directory at the given cleaned path and caches the
// metadata of all entries, so that the subsequent per-entry Lstat/OpenFile
// calls of the archiver do not cause one HEAD request per object.
func (fs *s3FS) readDir(cleanPath string) ([]string, error) {
	prefix := ""
	if key := s3Key(cleanPath); key != "" {
		prefix = key + "/"
	}
	entries, err := fs.api.List(fs.ctx, prefix)
	if err != nil {
		return nil, pathError("readdirnames", cleanPath, err)
	}

	seen := make(map[string]struct{}, len(entries))
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.Name == "" || e.Name == "." || e.Name == ".." || strings.Contains(e.Name, "/") {
			debug.Log("backup source: skipping entry with unusable name %q below %q", e.Name, prefix)
			continue
		}
		if _, ok := seen[e.Name]; ok {
			// key exists both as an object and as a prefix; objects sort
			// first, so the object wins and the prefix is skipped
			debug.Log("backup source: %q exists both as object and as prefix below %q, using the object", e.Name, prefix)
			continue
		}
		seen[e.Name] = struct{}{}

		childPath := path.Join(cleanPath, e.Name)
		fi, err := fs.newFileInfo(childPath, e.IsDir, e.Meta)
		if err != nil {
			return nil, err
		}
		fs.cacheFileInfo(childPath, fi)
		names = append(names, e.Name)
	}
	return names, nil
}

// Join joins any number of path elements into a single path.
func (fs *s3FS) Join(elem ...string) string {
	return path.Join(elem...)
}

// Separator returns the path separator, always "/" for S3 keys.
func (fs *s3FS) Separator() string {
	return "/"
}

// IsAbs reports whether the path is absolute. For s3FS, this is always the case.
func (fs *s3FS) IsAbs(_ string) bool {
	return true
}

// Abs returns an absolute representation of path. For s3FS, all paths are absolute.
func (fs *s3FS) Abs(p string) (string, error) {
	return s3CleanPath(p), nil
}

// Clean returns the cleaned path. For details, see filepath.Clean.
func (fs *s3FS) Clean(p string) string {
	return path.Clean(p)
}

// VolumeName returns the leading volume name, for s3FS it's always the empty string.
func (fs *s3FS) VolumeName(_ string) string {
	return ""
}

// Base returns the last element of p.
func (fs *s3FS) Base(p string) string {
	return path.Base(p)
}

// Dir returns p without the last element.
func (fs *s3FS) Dir(p string) string {
	return path.Dir(p)
}

type s3File struct {
	fs   *s3FS
	path string
	fi   *ExtendedFileInfo

	readable bool
	rd       io.ReadCloser // set for regular files once readable
	entries  []string      // set for directories once readable
}

// statically ensure that s3File implements File.
var _ File = &s3File{}

func (f *s3File) MakeReadable() error {
	if f.readable {
		return nil
	}

	if f.fi.Mode.IsDir() {
		entries, err := f.fs.readDir(f.path)
		if err != nil {
			return err
		}
		f.entries = entries
	} else {
		rd, meta, err := f.fs.api.Get(f.fs.ctx, s3Key(f.path))
		if err != nil {
			return pathError("open", f.path, err)
		}
		// the GET response is authoritative for size and modification time,
		// the object may have changed since its metadata was fetched
		modTime := meta.ModTime.Truncate(time.Second)
		fi := *f.fi
		fi.Size = meta.Size
		fi.ModTime = modTime
		fi.AccessTime = modTime
		fi.ChangeTime = modTime
		f.fi = &fi
		f.rd = rd
	}
	f.readable = true
	return nil
}

func (f *s3File) Read(p []byte) (int, error) {
	if f.rd == nil {
		return 0, pathError("read", f.path, os.ErrInvalid)
	}
	return f.rd.Read(p)
}

func (f *s3File) Close() error {
	if f.rd != nil {
		rd := f.rd
		f.rd = nil
		return rd.Close()
	}
	return nil
}

func (f *s3File) Readdirnames(n int) ([]string, error) {
	if !f.readable || !f.fi.Mode.IsDir() {
		return nil, pathError("readdirnames", f.path, os.ErrInvalid)
	}
	if n > 0 {
		return nil, pathError("readdirnames", f.path, errors.New("not implemented"))
	}
	return slices.Clone(f.entries), nil
}

func (f *s3File) Stat() (*ExtendedFileInfo, error) {
	return f.fi, nil
}

func (f *s3File) ToNode(_ bool, _ func(format string, args ...any)) (*data.Node, error) {
	node := buildBasicNode(f.path, f.fi)

	// uid/gid are always 0, S3 has no owner concept that maps onto them
	node.Inode = f.fi.Inode
	node.DeviceID = f.fi.DeviceID
	node.Links = f.fi.Links
	node.AccessTime = f.fi.AccessTime
	node.ChangeTime = f.fi.ChangeTime

	return node, nil
}

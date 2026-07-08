// Package gcs is a storage.Backend over Google Cloud Storage. Each
// File.ReadAt issues a GCS Range GET — the cloud.google.com/go/storage
// client handles transient retry internally.
//
// The cluster's RFiles live at gs://<bucket>/<accumulo-instance-id>/...
// (Accumulo's own naming on top of whatever volume root was configured).
// Path inputs accept either form:
//
//	gs://bucket/path/to/object.rf  → bucket="bucket", object="path/to/object.rf"
//	bucket/path/to/object.rf       → first segment = bucket, rest = object
//
// Auth uses Application Default Credentials — same as everything else
// in the cluster, so a Workload-Identity-bound service account just works.
//
// Why no Alluxio? See internal/storage/storage.go's package comment.
// shoal goes direct-to-GCS; caching (if/when we want it) lives in
// shoal's own block cache layer alongside the prefetcher.
package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	shstorage "github.com/phrocker/shoal/internal/storage"
)

// Backend opens GCS objects via a shared *storage.Client. Safe for
// concurrent Open and concurrent ReadAt across many Files.
type Backend struct {
	client *storage.Client
}

// Option customizes Backend construction. Use WithClient if you've
// already built a *storage.Client (e.g. in a long-lived service that
// wires its own credentials), or WithClientOptions to forward standard
// GCS-client options at construction.
type Option func(*config)

type config struct {
	client     *storage.Client
	clientOpts []option.ClientOption
}

// WithClient supplies a pre-built GCS client. If set, New skips its
// own client construction and ignores any WithClientOptions.
func WithClient(c *storage.Client) Option {
	return func(cfg *config) { cfg.client = c }
}

// WithClientOptions forwards options to storage.NewClient when New
// builds its own client. Common uses: option.WithCredentialsFile,
// option.WithEndpoint (for GCS emulators).
func WithClientOptions(opts ...option.ClientOption) Option {
	return func(cfg *config) { cfg.clientOpts = append(cfg.clientOpts, opts...) }
}

// New constructs a Backend. Without options, builds a default GCS
// client using Application Default Credentials.
func New(ctx context.Context, opts ...Option) (*Backend, error) {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.client == nil {
		c, err := storage.NewClient(ctx, cfg.clientOpts...)
		if err != nil {
			return nil, fmt.Errorf("gcs: NewClient: %w", err)
		}
		cfg.client = c
	}
	return &Backend{client: cfg.client}, nil
}

// Close releases the underlying GCS client. Idempotent.
func (b *Backend) Close() error {
	if b.client == nil {
		return nil
	}
	err := b.client.Close()
	b.client = nil
	return err
}

// Open resolves path to a (bucket, object) pair, fetches the object's
// size via Attrs (one HEAD-equivalent round trip), and returns a File
// that issues a Range GET per ReadAt.
//
// path forms accepted:
//   - "gs://bucket/object/path"
//   - "bucket/object/path"  (no scheme — first segment is bucket)
func (b *Backend) Open(ctx context.Context, path string) (shstorage.File, error) {
	bucket, object, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	obj := b.client.Bucket(bucket).Object(object)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, fmt.Errorf("%w: gs://%s/%s", shstorage.ErrNotFound, bucket, object)
		}
		return nil, fmt.Errorf("gcs: stat gs://%s/%s: %w", bucket, object, err)
	}
	return &file{
		obj:  obj,
		size: attrs.Size,
		ctx:  ctx,
	}, nil
}

// Create opens a GCS object writer. The object is visible after Close.
func (b *Backend) Create(ctx context.Context, path string) (shstorage.Writer, error) {
	bucket, object, err := ParsePath(path)
	if err != nil {
		return nil, err
	}
	return b.client.Bucket(bucket).Object(object).NewWriter(ctx), nil
}

// List returns objects directly under prefix. The prefix may be gs://bucket/dir
// or bucket/dir and is treated like a tablet directory.
func (b *Backend) List(ctx context.Context, prefix string) ([]string, error) {
	bucket, objectPrefix, err := ParsePath(prefix)
	if err != nil {
		return nil, err
	}
	objectPrefix = strings.TrimRight(objectPrefix, "/\\") + "/"
	it := b.client.Bucket(bucket).Objects(ctx, &storage.Query{
		Prefix:    objectPrefix,
		Delimiter: "/",
	})
	var out []string
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gcs: list gs://%s/%s: %w", bucket, objectPrefix, err)
		}
		if attrs == nil || attrs.Name == "" || strings.HasSuffix(attrs.Name, "/") {
			continue
		}
		out = append(out, "gs://"+bucket+"/"+attrs.Name)
	}
	return out, nil
}

// ParsePath splits a path string into (bucket, object). Exposed so
// callers (tests, diagnostics) can validate paths without opening.
func ParsePath(path string) (bucket, object string, err error) {
	trimmed := strings.TrimPrefix(path, "gs://")
	idx := strings.IndexByte(trimmed, '/')
	if idx < 0 || idx == len(trimmed)-1 {
		return "", "", fmt.Errorf("gcs: invalid path %q (want gs://bucket/object or bucket/object)", path)
	}
	bucket = trimmed[:idx]
	object = trimmed[idx+1:]
	if bucket == "" {
		return "", "", fmt.Errorf("gcs: empty bucket in %q", path)
	}
	return bucket, object, nil
}

// file is the GCS File implementation. Each ReadAt opens a fresh
// RangeReader — stateless per call so concurrent ReadAts don't share
// reader state.
type file struct {
	obj  *storage.ObjectHandle
	size int64
	ctx  context.Context
}

func (g *file) Size() int64 { return g.size }

// Close on a GCS file is a no-op — we don't hold per-file resources.
// The underlying client persists until Backend.Close.
func (g *file) Close() error { return nil }

// ReadAt issues a single Range GET covering [off, off+len(p)). On
// short reads at EOF returns (n, io.EOF) per io.ReaderAt's contract.
func (g *file) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("gcs: negative offset %d", off)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= g.size {
		return 0, io.EOF
	}
	want := int64(len(p))
	if off+want > g.size {
		want = g.size - off
	}
	rdr, err := g.obj.NewRangeReader(g.ctx, off, want)
	if err != nil {
		return 0, fmt.Errorf("gcs: NewRangeReader off=%d len=%d: %w", off, want, err)
	}
	defer rdr.Close()
	n, err := io.ReadFull(rdr, p[:want])
	if err != nil {
		// io.ReadFull returns io.ErrUnexpectedEOF for short reads; map
		// that to a regular EOF if we're at the file's end so callers
		// don't have to handle two flavors.
		if errors.Is(err, io.ErrUnexpectedEOF) && off+int64(n) >= g.size {
			err = io.EOF
		} else {
			return n, fmt.Errorf("gcs: read body off=%d: %w", off, err)
		}
	}
	if int64(n) < int64(len(p)) {
		// Caller asked for more than we had at this offset.
		return n, io.EOF
	}
	return n, nil
}

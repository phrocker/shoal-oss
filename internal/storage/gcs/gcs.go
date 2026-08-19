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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	shstorage "github.com/phrocker/shoal/internal/storage"
)

// Backend opens GCS objects via a shared *storage.Client. Safe for
// concurrent Open and concurrent ReadAt across many Files.
type Backend struct {
	client        *storage.Client
	bucketFactory func(string) bucketHandle
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
	return &Backend{
		client: cfg.client,
		bucketFactory: func(name string) bucketHandle {
			return storageBucketHandle{bucket: cfg.client.Bucket(name)}
		},
	}, nil
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

// Create stages writes through a temporary GCS object and promotes it into the
// destination on Close. An unsuccessful Close preserves any pre-existing
// destination object.
func (b *Backend) Create(ctx context.Context, path string) (shstorage.Writer, error) {
	bucket, object, err := ParsePath(path)
	if err != nil {
		return nil, err
	}
	ctx = contextOrBackground(ctx)
	bucketHandle := b.bucket(bucket)
	target := bucketHandle.Object(object)
	targetConditions, err := loadPromotionConditions(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("gcs: inspect destination gs://%s/%s: %w", bucket, object, err)
	}

	writeCtx, cancel := context.WithCancel(ctx)
	tempObject := object + ".shoal-tmp-" + uuid.NewString()
	temp := bucketHandle.Object(tempObject)
	return &writer{
		ctx:                ctx,
		cancel:             cancel,
		inner:              temp.If(storage.Conditions{DoesNotExist: true}).NewWriter(writeCtx),
		target:             target,
		targetPath:         "gs://" + bucket + "/" + object,
		targetConditions:   targetConditions,
		temp:               temp,
		tempPath:           "gs://" + bucket + "/" + tempObject,
		tempCleanupTimeout: tempCleanupTimeout,
	}, nil
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

type objectWriter interface {
	Write([]byte) (int, error)
	Close() error
	CloseWithError(error) error
	Attrs() *storage.ObjectAttrs
}

type writer struct {
	ctx                context.Context
	cancel             context.CancelFunc
	inner              objectWriter
	target             objectHandle
	targetPath         string
	targetConditions   storage.Conditions
	temp               objectHandle
	tempPath           string
	tempGeneration     int64
	tempAttrs          *storage.ObjectAttrs
	tempClosed         bool
	tempCleanupTimeout time.Duration
	closed             bool
	abortRequested     bool
	aborted            bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.abortRequested {
		return 0, fmt.Errorf("gcs: writer already aborted")
	}
	if w.closed || w.tempClosed {
		return 0, fmt.Errorf("gcs: writer already closed")
	}
	return w.inner.Write(p)
}

func (w *writer) Close() error {
	if w.abortRequested {
		return fmt.Errorf("gcs: writer already aborted")
	}
	if w.closed {
		return nil
	}
	if !w.tempClosed {
		w.tempClosed = true
		if err := w.inner.Close(); err != nil {
			w.stopWriter()
			return errors.Join(
				fmt.Errorf("gcs: close temporary object %s: %w", w.tempPath, err),
				w.cleanupTempObject(),
			)
		}
		attrs := w.inner.Attrs()
		if attrs == nil || attrs.Generation == 0 {
			w.stopWriter()
			return errors.Join(
				fmt.Errorf("gcs: close temporary object %s: missing generation after successful close", w.tempPath),
				w.cleanupTempObject(),
			)
		}
		w.tempGeneration = attrs.Generation
		w.tempAttrs = attrs
	}

	if w.tempGeneration == 0 {
		return fmt.Errorf("gcs: temporary object %s is not promotable", w.tempPath)
	}
	if err := w.promoteTempObject(); err != nil {
		w.stopWriter()
		return errors.Join(err, w.cleanupTempObject())
	}
	w.closed = true
	w.stopWriter()
	w.cleanupTempObjectBestEffort()
	return nil
}

func (w *writer) Abort() error {
	if w.aborted {
		return nil
	}
	if w.closed {
		return fmt.Errorf("gcs: writer already closed")
	}
	w.abortRequested = true

	var abortErr error
	if !w.tempClosed {
		w.tempClosed = true
		if err := w.inner.CloseWithError(errors.New("gcs: write aborted")); err != nil {
			abortErr = fmt.Errorf("gcs: abort temporary object %s: %w", w.tempPath, err)
		}
	}
	w.stopWriter()
	if cleanupErr := w.cleanupTempObject(); cleanupErr != nil {
		abortErr = errors.Join(abortErr, cleanupErr)
	}
	if abortErr != nil {
		return abortErr
	}
	w.aborted = true
	return nil
}

type bucketHandle interface {
	Object(name string) objectHandle
}

type objectHandle interface {
	NewWriter(context.Context) objectWriter
	Attrs(context.Context) (*storage.ObjectAttrs, error)
	If(storage.Conditions) objectHandle
	Generation(int64) objectHandle
	CopierFrom(objectHandle) objectCopier
	Delete(context.Context) error
}

type objectCopier interface {
	Run(context.Context) (*storage.ObjectAttrs, error)
}

type storageBucketHandle struct {
	bucket *storage.BucketHandle
}

func (b storageBucketHandle) Object(name string) objectHandle {
	return storageObjectHandle{object: b.bucket.Object(name)}
}

type storageObjectHandle struct {
	object *storage.ObjectHandle
}

func (o storageObjectHandle) NewWriter(ctx context.Context) objectWriter {
	return o.object.NewWriter(ctx)
}

func (o storageObjectHandle) Attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	return o.object.Attrs(ctx)
}

func (o storageObjectHandle) If(conds storage.Conditions) objectHandle {
	return storageObjectHandle{object: o.object.If(conds)}
}

func (o storageObjectHandle) Generation(generation int64) objectHandle {
	return storageObjectHandle{object: o.object.Generation(generation)}
}

func (o storageObjectHandle) CopierFrom(src objectHandle) objectCopier {
	source, ok := src.(storageObjectHandle)
	if !ok {
		panic(fmt.Sprintf("gcs: unsupported storage object source type %T", src))
	}
	return storageObjectCopier{copier: o.object.CopierFrom(source.object)}
}

func (o storageObjectHandle) Delete(ctx context.Context) error {
	return o.object.Delete(ctx)
}

type storageObjectCopier struct {
	copier *storage.Copier
}

func (c storageObjectCopier) Run(ctx context.Context) (*storage.ObjectAttrs, error) {
	return c.copier.Run(ctx)
}

var tempCleanupTimeout = 10 * time.Second

func (b *Backend) bucket(name string) bucketHandle {
	if b.bucketFactory != nil {
		return b.bucketFactory(name)
	}
	return storageBucketHandle{bucket: b.client.Bucket(name)}
}

func loadPromotionConditions(ctx context.Context, target objectHandle) (storage.Conditions, error) {
	attrs, err := target.Attrs(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return storage.Conditions{DoesNotExist: true}, nil
	}
	if err != nil {
		return storage.Conditions{}, err
	}
	return storage.Conditions{
		GenerationMatch:     attrs.Generation,
		MetagenerationMatch: attrs.Metageneration,
	}, nil
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (w *writer) stopWriter() {
	if w.cancel != nil {
		w.cancel()
		w.cancel = nil
	}
}

func (w *writer) promoteTempObject() error {
	_, err := w.target.
		If(w.targetConditions).
		CopierFrom(w.temp.Generation(w.tempGeneration)).
		Run(w.ctx)
	if err != nil {
		if w.destinationMatchesTemp() {
			return nil
		}
		return fmt.Errorf("gcs: promote temporary object %s to %s: %w", w.tempPath, w.targetPath, err)
	}
	return nil
}

func (w *writer) destinationMatchesTemp() bool {
	if w.tempAttrs == nil {
		return false
	}
	verifyCtx, cancel := context.WithTimeout(context.Background(), w.tempCleanupTimeout)
	defer cancel()
	attrs, err := w.target.Attrs(verifyCtx)
	if err != nil {
		return false
	}
	if w.targetConditions.GenerationMatch != 0 &&
		attrs.Generation == w.targetConditions.GenerationMatch {
		return false
	}
	return attrs.Size == w.tempAttrs.Size &&
		attrs.CRC32C == w.tempAttrs.CRC32C &&
		bytes.Equal(attrs.MD5, w.tempAttrs.MD5)
}

func (w *writer) cleanupTempObject() error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), w.tempCleanupTimeout)
	defer cancel()
	if err := w.tempForCleanup().Delete(cleanupCtx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("gcs: remove temporary object %s: %w", w.tempPath, err)
	}
	return nil
}

func (w *writer) cleanupTempObjectBestEffort() {
	_ = w.cleanupTempObject()
}

func (w *writer) tempForCleanup() objectHandle {
	if w.tempGeneration != 0 {
		return w.temp.Generation(w.tempGeneration)
	}
	return w.temp
}

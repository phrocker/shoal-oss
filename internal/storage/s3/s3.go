// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package s3 is a storage.Backend over Amazon S3 (or S3-compatible endpoints).
// Each file.ReadAt issues a GetObject with a Range header — no state is shared
// between concurrent ReadAt calls. Auth uses the AWS SDK v2 default credential
// chain (environment variables, ~/.aws/credentials, EC2 instance profile, EKS
// IRSA, etc.).
//
// Path forms accepted:
//
//	s3://bucket/key/path/object.rf   → bucket="bucket", key="key/path/object.rf"
//	bucket/key/path/object.rf        → first segment = bucket, rest = key
//
// For S3-compatible endpoints (MinIO, LocalStack) supply a custom endpoint and
// path-style addressing via WithClientOptions:
//
//	s3.New(ctx,
//	    s3.WithClientOptions(func(o *s3sdk.Options) {
//	        o.BaseEndpoint = aws.String("http://localhost:9000")
//	        o.UsePathStyle = true
//	    }))
package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"

	shstorage "github.com/phrocker/shoal/internal/storage"
)

// Backend opens S3 objects via a shared *s3sdk.Client. Safe for
// concurrent Open and concurrent ReadAt across many Files.
type Backend struct {
	client *s3sdk.Client
	ops    s3WriteOperations
}

// Option customizes Backend construction. Use WithClient if you've already
// built a *s3sdk.Client, or WithClientOptions to forward S3 client option
// functions at construction (e.g. custom endpoint, region, UsePathStyle).
type Option func(*cfg)

type cfg struct {
	client     *s3sdk.Client
	clientOpts []func(*s3sdk.Options)
}

// WithClient supplies a pre-built S3 client. If set, New skips its own client
// construction and ignores any WithClientOptions.
func WithClient(c *s3sdk.Client) Option {
	return func(c2 *cfg) { c2.client = c }
}

// WithClientOptions forwards option functions to s3sdk.NewFromConfig when New
// builds its own client. Common uses: custom endpoint/region, UsePathStyle=true
// for MinIO or other S3-compatible stores.
func WithClientOptions(opts ...func(*s3sdk.Options)) Option {
	return func(c *cfg) { c.clientOpts = append(c.clientOpts, opts...) }
}

// New constructs a Backend. Without options, builds a default S3 client using
// the AWS SDK v2 default credential chain (env vars, ~/.aws/credentials,
// EC2 instance profile, EKS IRSA, etc.).
func New(ctx context.Context, opts ...Option) (*Backend, error) {
	c := &cfg{}
	for _, o := range opts {
		o(c)
	}
	if c.client == nil {
		awsCfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3: LoadDefaultConfig: %w", err)
		}
		c.client = s3sdk.NewFromConfig(awsCfg, c.clientOpts...)
	}
	return &Backend{client: c.client, ops: sdkS3WriteOperations{client: c.client}}, nil
}

// Close is a no-op: the v2 S3 client holds no persistent connection.
// Kept for symmetry with the GCS backend.
func (b *Backend) Close() error { return nil }

// Open resolves path to a (bucket, key) pair, calls HeadObject for size, and
// returns a File that issues a Range GET per ReadAt.
//
// Path forms accepted:
//   - "s3://bucket/key/path"
//   - "bucket/key/path"  (no scheme — first segment is bucket)
func (b *Backend) Open(ctx context.Context, path string) (shstorage.File, error) {
	bucket, key, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	out, err := b.client.HeadObject(ctx, &s3sdk.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("%w: s3://%s/%s", shstorage.ErrNotFound, bucket, key)
		}
		return nil, fmt.Errorf("s3: HeadObject s3://%s/%s: %w", bucket, key, err)
	}

	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	return &file{
		client: b.client,
		bucket: bucket,
		key:    key,
		size:   size,
		ctx:    ctx,
	}, nil
}

// Create opens an S3 object writer. Close stages the bytes under an internal
// key, then conditionally copies that exact object into the destination.
func (b *Backend) Create(ctx context.Context, path string) (shstorage.Writer, error) {
	bucket, key, err := ParsePath(path)
	if err != nil {
		return nil, err
	}
	ctx = contextOrBackground(ctx)
	ops := b.writeOperations()
	target, err := ops.head(ctx, bucket, key)
	if err != nil && !isNotFound(err) {
		return nil, fmt.Errorf("s3: inspect destination s3://%s/%s: %w", bucket, key, err)
	}
	targetExists := err == nil
	if targetExists && target.etag == nil {
		return nil, fmt.Errorf("s3: inspect destination s3://%s/%s: missing ETag", bucket, key)
	}
	stageKey, err := nextTemporaryStageKey(key)
	if err != nil {
		return nil, fmt.Errorf("s3: stage temporary object for s3://%s/%s: %w", bucket, key, err)
	}
	return &writer{
		ops:            ops,
		bucket:         bucket,
		key:            key,
		stageKey:       stageKey,
		writeID:        uuid.NewString(),
		ctx:            ctx,
		targetExists:   targetExists,
		target:         target,
		cleanupTimeout: s3CleanupTimeout,
	}, nil
}

// List returns paths of objects directly under prefix (using delimiter="/").
// Returned paths are in s3://bucket/key form. "Directory" keys (ending with
// "/") are skipped — only leaf objects are returned, mirroring gcs.List.
func (b *Backend) List(ctx context.Context, prefix string) ([]string, error) {
	bucket, objectPrefix, err := ParsePath(prefix)
	if err != nil {
		return nil, err
	}
	objectPrefix = strings.TrimRight(objectPrefix, "/\\") + "/"

	var out []string
	paginator := s3sdk.NewListObjectsV2Paginator(b.client, &s3sdk.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(objectPrefix),
		Delimiter: aws.String("/"),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3: ListObjectsV2 s3://%s/%s: %w", bucket, objectPrefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil || strings.HasSuffix(*obj.Key, "/") || isTemporaryStageKey(*obj.Key) {
				continue
			}
			out = append(out, "s3://"+bucket+"/"+*obj.Key)
		}
	}
	return out, nil
}

// ParsePath splits a path string into (bucket, key). Exposed so callers
// (tests, diagnostics) can validate paths without opening.
//
// Accepted forms:
//
//	s3://bucket/key/path   → bucket="bucket", key="key/path"
//	bucket/key/path        → bucket="bucket", key="key/path"
func ParsePath(path string) (bucket, key string, err error) {
	trimmed := strings.TrimPrefix(path, "s3://")
	idx := strings.IndexByte(trimmed, '/')
	if idx < 0 || idx == len(trimmed)-1 {
		return "", "", fmt.Errorf("s3: invalid path %q (want s3://bucket/key or bucket/key)", path)
	}
	bucket = trimmed[:idx]
	key = trimmed[idx+1:]
	if bucket == "" {
		return "", "", fmt.Errorf("s3: empty bucket in %q", path)
	}
	return bucket, key, nil
}

const (
	maxObjectKeyBytes     = 1024
	tempStageKeyPrefix    = ".shoal-tmp-"
	tempStageHashHexLen   = 4
	tempStageRandomHexLen = 10
	tempStageComponentLen = len(tempStageKeyPrefix) + tempStageHashHexLen + tempStageRandomHexLen
	legacyStageDirPrefix  = ".shoal-tmp/"
)

var randomStageKeyToken = func() (string, error) {
	buf := make([]byte, tempStageRandomHexLen/2)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate random staging token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func nextTemporaryStageKey(key string) (string, error) {
	prefix, err := temporaryStageKeyPrefixFor(key)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256([]byte(key))
	token, err := randomStageKeyToken()
	if err != nil {
		return "", err
	}
	hashHex := hex.EncodeToString(hash[:tempStageHashHexLen/2])
	if len(hashHex) < tempStageHashHexLen || len(token) < tempStageRandomHexLen {
		return "", fmt.Errorf("temporary key token material too short")
	}
	component := tempStageKeyPrefix + token[:tempStageRandomHexLen] + hashHex[:tempStageHashHexLen]
	available := maxObjectKeyBytes - len(prefix)
	if available < tempStageComponentLen {
		return "", fmt.Errorf(
			"key prefix %q leaves %d bytes for a temporary object; need at least %d",
			prefix, available, tempStageComponentLen,
		)
	}
	return prefix + component, nil
}

func temporaryStageKeyPrefixFor(key string) (string, error) {
	prefix := stageKeyParentPrefix(key)
	for prefix != "" && maxObjectKeyBytes-len(prefix) < tempStageComponentLen {
		trimmed := strings.TrimSuffix(prefix, "/")
		next := stageKeyParentPrefix(trimmed)
		if next == "" {
			available := maxObjectKeyBytes - len(prefix)
			return "", fmt.Errorf(
				"key prefix %q leaves %d bytes for a temporary object; need at least %d",
				prefix, available, tempStageComponentLen,
			)
		}
		prefix = next
	}
	return prefix, nil
}

func stageKeyParentPrefix(key string) string {
	if idx := strings.LastIndexByte(key, '/'); idx >= 0 {
		return key[:idx+1]
	}
	return ""
}

func isTemporaryStageKey(key string) bool {
	name := key
	if idx := strings.LastIndexByte(name, '/'); idx >= 0 {
		name = name[idx+1:]
	}
	return isLegacyTemporaryStageKey(key) || isGeneratedTemporaryStageComponent(name)
}

func isLegacyTemporaryStageKey(key string) bool {
	if !strings.HasPrefix(key, legacyStageDirPrefix) {
		return false
	}
	remainder := key[len(legacyStageDirPrefix):]
	if remainder == "" || strings.ContainsRune(remainder, '/') {
		return false
	}
	_, err := uuid.Parse(remainder)
	return err == nil
}

func isGeneratedTemporaryStageComponent(name string) bool {
	if len(name) != tempStageComponentLen || !strings.HasPrefix(name, tempStageKeyPrefix) {
		return false
	}
	token := name[len(tempStageKeyPrefix) : len(tempStageKeyPrefix)+tempStageRandomHexLen]
	hash := name[len(tempStageKeyPrefix)+tempStageRandomHexLen:]
	return isLowerHex(token) && isLowerHex(hash)
}

func isLowerHex(value string) bool {
	if value == "" {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded)*2 == len(value)
}

// file is the S3 File implementation. Each ReadAt issues a fresh Range GET —
// stateless per call so concurrent ReadAts don't share reader state.
type file struct {
	client *s3sdk.Client
	bucket string
	key    string
	size   int64
	ctx    context.Context //nolint:containedctx
}

func (f *file) Size() int64 { return f.size }

// Close is a no-op — no per-file resources are held.
func (f *file) Close() error { return nil }

// ReadAt issues a single Range GET covering [off, off+len(p)). On short reads
// at EOF returns (n, io.EOF) per io.ReaderAt's contract.
func (f *file) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("s3: negative offset %d", off)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= f.size {
		return 0, io.EOF
	}
	want := int64(len(p))
	if off+want > f.size {
		want = f.size - off
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, off+want-1)
	out, err := f.client.GetObject(f.ctx, &s3sdk.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return 0, fmt.Errorf("s3: GetObject s3://%s/%s off=%d: %w", f.bucket, f.key, off, err)
	}
	defer out.Body.Close()

	n, err := io.ReadFull(out.Body, p[:want])
	if err != nil {
		// io.ReadFull returns io.ErrUnexpectedEOF for short reads; map that to
		// a regular EOF if we're at the file's end so callers don't have to
		// handle two flavors.
		if errors.Is(err, io.ErrUnexpectedEOF) && off+int64(n) >= f.size {
			err = io.EOF
		} else {
			return n, fmt.Errorf("s3: read body s3://%s/%s off=%d: %w", f.bucket, f.key, off, err)
		}
	}
	if int64(n) < int64(len(p)) {
		// Caller asked for more bytes than the object contains at this offset.
		return n, io.EOF
	}
	return n, nil
}

type s3ObjectState struct {
	etag      *string
	versionID *string
	size      int64
	metadata  map[string]string
}

type s3WriteOperations interface {
	head(context.Context, string, string) (s3ObjectState, error)
	putStage(context.Context, string, string, []byte, string) (s3ObjectState, error)
	promote(context.Context, string, string, string, s3ObjectState, bool, s3ObjectState) error
	deleteStage(context.Context, string, string, s3ObjectState) error
}

type sdkS3WriteOperations struct {
	client *s3sdk.Client
}

func (o sdkS3WriteOperations) head(ctx context.Context, bucket, key string) (s3ObjectState, error) {
	out, err := o.client.HeadObject(ctx, &s3sdk.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return s3ObjectState{}, err
	}
	state := s3ObjectState{etag: out.ETag, versionID: out.VersionId, metadata: out.Metadata}
	if out.ContentLength != nil {
		state.size = *out.ContentLength
	}
	return state, nil
}

func (o sdkS3WriteOperations) putStage(
	ctx context.Context, bucket, key string, data []byte, writeID string,
) (s3ObjectState, error) {
	out, err := o.client.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		IfNoneMatch:   aws.String("*"),
		Metadata:      map[string]string{"shoal-write-id": writeID},
	})
	if err != nil {
		return s3ObjectState{}, err
	}
	return s3ObjectState{
		etag:      out.ETag,
		versionID: out.VersionId,
		size:      int64(len(data)),
		metadata:  map[string]string{"shoal-write-id": writeID},
	}, nil
}

func (o sdkS3WriteOperations) promote(
	ctx context.Context,
	bucket, stageKey, key string,
	stage s3ObjectState,
	targetExists bool,
	target s3ObjectState,
) error {
	in := &s3sdk.CopyObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		CopySource:        aws.String(url.PathEscape(bucket + "/" + stageKey)),
		CopySourceIfMatch: stage.etag,
	}
	if targetExists {
		in.IfMatch = target.etag
	} else {
		in.IfNoneMatch = aws.String("*")
	}
	_, err := o.client.CopyObject(ctx, in)
	return err
}

func (o sdkS3WriteOperations) deleteStage(
	ctx context.Context, bucket, key string, stage s3ObjectState,
) error {
	_, err := o.client.DeleteObject(ctx, &s3sdk.DeleteObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		IfMatch:   stage.etag,
		VersionId: stage.versionID,
	})
	return err
}

var s3CleanupTimeout = 10 * time.Second

func (b *Backend) writeOperations() s3WriteOperations {
	if b.ops != nil {
		return b.ops
	}
	return sdkS3WriteOperations{client: b.client}
}

// writer buffers bytes in memory, stages them, and conditionally publishes the
// staged object on Close.
type writer struct {
	ops            s3WriteOperations
	bucket         string
	key            string
	stageKey       string
	writeID        string
	ctx            context.Context //nolint:containedctx
	targetExists   bool
	target         s3ObjectState
	stage          s3ObjectState
	stageCreated   bool
	cleanupTimeout time.Duration
	buf            bytes.Buffer
	closed         bool
	aborted        bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.aborted {
		return 0, fmt.Errorf("s3: writer already aborted")
	}
	if w.closed {
		return 0, fmt.Errorf("s3: writer already closed")
	}
	return w.buf.Write(p)
}

func (w *writer) Close() error {
	if w.aborted {
		return fmt.Errorf("s3: writer already aborted")
	}
	if w.closed {
		_ = w.cleanupStage()
		return nil
	}
	if !w.stageCreated {
		stage, err := w.ops.putStage(w.ctx, w.bucket, w.stageKey, w.buf.Bytes(), w.writeID)
		if err != nil {
			if verified, verifyErr := w.verifyStage(); verifyErr != nil || !verified {
				return errors.Join(
					fmt.Errorf("s3: stage upload s3://%s/%s: %w", w.bucket, w.stageKey, err),
					verifyErr,
					w.cleanupStage(),
				)
			}
			stage = w.stage
		}
		w.stage = stage
		w.stageCreated = true
	}
	if w.stage.etag == nil {
		if verified, err := w.verifyStage(); err != nil || !verified || w.stage.etag == nil {
			return errors.Join(
				fmt.Errorf("s3: temporary object s3://%s/%s is missing an ETag", w.bucket, w.stageKey),
				err,
				w.cleanupStage(),
			)
		}
	}
	if err := w.ops.promote(
		w.ctx, w.bucket, w.stageKey, w.key, w.stage, w.targetExists, w.target,
	); err != nil {
		if verified, verifyErr := w.verifyDestination(); verifyErr != nil || !verified {
			return errors.Join(
				fmt.Errorf("s3: promote s3://%s/%s to s3://%s/%s: %w",
					w.bucket, w.stageKey, w.bucket, w.key, err),
				verifyErr,
				w.cleanupStage(),
			)
		}
	}
	w.closed = true
	_ = w.cleanupStage()
	return nil
}

func (w *writer) Abort() error {
	if w.aborted {
		return nil
	}
	if w.closed {
		return fmt.Errorf("s3: writer already closed")
	}
	if err := w.cleanupStage(); err != nil {
		return err
	}
	w.aborted = true
	w.buf.Reset()
	return nil
}

func (w *writer) verifyStage() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), w.cleanupTimeout)
	defer cancel()
	state, err := w.ops.head(ctx, w.bucket, w.stageKey)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("s3: verify temporary object s3://%s/%s: %w", w.bucket, w.stageKey, err)
	}
	if state.size != int64(w.buf.Len()) || state.metadata["shoal-write-id"] != w.writeID {
		return false, nil
	}
	w.stage = state
	w.stageCreated = true
	return true, nil
}

func (w *writer) verifyDestination() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), w.cleanupTimeout)
	defer cancel()
	state, err := w.ops.head(ctx, w.bucket, w.key)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("s3: verify destination s3://%s/%s: %w", w.bucket, w.key, err)
	}
	if w.targetExists && equalStringPointers(state.etag, w.target.etag) {
		return false, nil
	}
	return state.size == int64(w.buf.Len()) &&
		state.metadata["shoal-write-id"] == w.writeID, nil
}

func (w *writer) cleanupStage() error {
	if !w.stageCreated {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.cleanupTimeout)
	defer cancel()
	if err := w.ops.deleteStage(ctx, w.bucket, w.stageKey, w.stage); err != nil && !isNotFound(err) {
		return fmt.Errorf("s3: remove temporary object s3://%s/%s: %w", w.bucket, w.stageKey, err)
	}
	w.stageCreated = false
	return nil
}

func equalStringPointers(a, b *string) bool {
	return a != nil && b != nil && *a == *b
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// isNotFound reports whether err is an S3 "key does not exist" error.
// HeadObject returns *types.NotFound (404); GetObject returns *types.NoSuchKey.
func isNotFound(err error) bool {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var notFound *types.NotFound
	return errors.As(err, &notFound)
}

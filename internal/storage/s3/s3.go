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
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	shstorage "github.com/phrocker/shoal/internal/storage"
)

// Backend opens S3 objects via a shared *s3sdk.Client. Safe for
// concurrent Open and concurrent ReadAt across many Files.
type Backend struct {
	client *s3sdk.Client
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
	return &Backend{client: c.client}, nil
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

// Create opens an S3 object writer. Bytes are buffered in memory and uploaded
// as a single PutObject on Close. Replaces any existing object at path.
func (b *Backend) Create(ctx context.Context, path string) (shstorage.Writer, error) {
	bucket, key, err := ParsePath(path)
	if err != nil {
		return nil, err
	}
	return &writer{client: b.client, bucket: bucket, key: key, ctx: ctx}, nil
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
			if obj.Key == nil || strings.HasSuffix(*obj.Key, "/") {
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

// writer buffers bytes in memory and uploads them as a single PutObject on
// Close. S3 has no streaming-append API; buffering on Close matches the
// one-shot usage pattern of storage.WriteAll.
type writer struct {
	client *s3sdk.Client
	bucket string
	key    string
	ctx    context.Context //nolint:containedctx
	buf    bytes.Buffer
}

func (w *writer) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *writer) Close() error {
	_, err := w.client.PutObject(w.ctx, &s3sdk.PutObjectInput{
		Bucket:        aws.String(w.bucket),
		Key:           aws.String(w.key),
		Body:          bytes.NewReader(w.buf.Bytes()),
		ContentLength: aws.Int64(int64(w.buf.Len())),
	})
	if err != nil {
		return fmt.Errorf("s3: PutObject s3://%s/%s: %w", w.bucket, w.key, err)
	}
	return nil
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

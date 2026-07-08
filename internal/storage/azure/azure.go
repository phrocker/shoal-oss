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

// Package azure is a storage.Backend over Azure Blob Storage. Each
// file.ReadAt issues a ranged DownloadStream — no state is shared between
// concurrent ReadAt calls.
//
// Azure addresses objects as account → container → blob. The account is fixed
// at construction (via AZURE_STORAGE_ACCOUNT, an explicit service URL, or a
// connection string); the container and blob come from the path:
//
//	az://container/path/to/object.rf → container="container", blob="path/to/object.rf"
//	container/path/to/object.rf      → first segment = container, rest = blob
//
// Auth (in precedence order):
//  1. WithServiceClient — a pre-built *service.Client.
//  2. WithConnectionString / AZURE_STORAGE_CONNECTION_STRING — shared-key or SAS.
//  3. AZURE_STORAGE_ACCOUNT (or WithAccount / WithServiceURL) + the default
//     Azure credential chain (env vars, managed identity, workload identity,
//     Azure CLI, etc.).
//
// For the Azurite emulator, supply the emulator's connection string via
// WithConnectionString.
package azure

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	shstorage "github.com/phrocker/shoal/internal/storage"
)

// Backend opens Azure blobs via a shared *service.Client. Safe for concurrent
// Open and concurrent ReadAt across many Files.
type Backend struct {
	svc *service.Client
}

// Option customizes Backend construction.
type Option func(*config)

type config struct {
	svc        *service.Client
	connString string
	account    string
	serviceURL string
}

// WithServiceClient supplies a pre-built service client. If set, New ignores
// all other credential options.
func WithServiceClient(svc *service.Client) Option {
	return func(c *config) { c.svc = svc }
}

// WithConnectionString authenticates with an Azure Storage connection string
// (shared-key or SAS). Also honored via AZURE_STORAGE_CONNECTION_STRING.
func WithConnectionString(cs string) Option {
	return func(c *config) { c.connString = cs }
}

// WithAccount sets the storage account name; the service URL is derived as
// https://<account>.blob.core.windows.net/. Also honored via
// AZURE_STORAGE_ACCOUNT.
func WithAccount(account string) Option {
	return func(c *config) { c.account = account }
}

// WithServiceURL sets an explicit blob service URL (e.g. for sovereign clouds
// or a custom endpoint). Takes precedence over WithAccount.
func WithServiceURL(url string) Option {
	return func(c *config) { c.serviceURL = url }
}

// New constructs a Backend. With no credential options it uses
// AZURE_STORAGE_CONNECTION_STRING when set, otherwise AZURE_STORAGE_ACCOUNT
// plus the default Azure credential chain.
func New(_ context.Context, opts ...Option) (*Backend, error) {
	c := &config{}
	for _, o := range opts {
		o(c)
	}
	if c.svc != nil {
		return &Backend{svc: c.svc}, nil
	}

	connString := c.connString
	if connString == "" {
		connString = os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	}
	if connString != "" {
		svc, err := service.NewClientFromConnectionString(connString, nil)
		if err != nil {
			return nil, fmt.Errorf("azure: NewClientFromConnectionString: %w", err)
		}
		return &Backend{svc: svc}, nil
	}

	serviceURL := c.serviceURL
	if serviceURL == "" {
		account := c.account
		if account == "" {
			account = os.Getenv("AZURE_STORAGE_ACCOUNT")
		}
		if account == "" {
			return nil, fmt.Errorf("azure: no credentials: set AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT, or pass WithServiceClient/WithConnectionString/WithAccount/WithServiceURL")
		}
		serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net/", account)
	}
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("azure: NewDefaultAzureCredential: %w", err)
	}
	svc, err := service.NewClient(serviceURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("azure: NewClient: %w", err)
	}
	return &Backend{svc: svc}, nil
}

// Close is a no-op: the service client holds no persistent connection.
// Kept for symmetry with the GCS/S3 backends.
func (b *Backend) Close() error { return nil }

// Open resolves path to a (container, blob) pair, calls GetProperties for size,
// and returns a File that issues a ranged DownloadStream per ReadAt.
func (b *Backend) Open(ctx context.Context, path string) (shstorage.File, error) {
	cont, name, err := ParsePath(path)
	if err != nil {
		return nil, err
	}
	bc := b.svc.NewContainerClient(cont).NewBlobClient(name)
	props, err := bc.GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
			return nil, fmt.Errorf("%w: az://%s/%s", shstorage.ErrNotFound, cont, name)
		}
		return nil, fmt.Errorf("azure: stat az://%s/%s: %w", cont, name, err)
	}
	var size int64
	if props.ContentLength != nil {
		size = *props.ContentLength
	}
	return &file{
		blob:      bc,
		container: cont,
		name:      name,
		size:      size,
		ctx:       ctx,
	}, nil
}

// Create opens a block-blob writer. Bytes are buffered in memory and uploaded
// as a single block blob on Close. Replaces any existing blob at path.
func (b *Backend) Create(ctx context.Context, path string) (shstorage.Writer, error) {
	cont, name, err := ParsePath(path)
	if err != nil {
		return nil, err
	}
	return &writer{
		blob:      b.svc.NewContainerClient(cont).NewBlockBlobClient(name),
		container: cont,
		name:      name,
		ctx:       ctx,
	}, nil
}

// List returns paths of blobs directly under prefix (using delimiter "/").
// Returned paths are in az://container/blob form. Virtual "directory" prefixes
// are skipped — only leaf blobs are returned, mirroring gcs/s3 List.
func (b *Backend) List(ctx context.Context, prefix string) ([]string, error) {
	cont, blobPrefix, err := ParsePath(prefix)
	if err != nil {
		return nil, err
	}
	blobPrefix = strings.TrimRight(blobPrefix, "/\\") + "/"

	var out []string
	pager := b.svc.NewContainerClient(cont).NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
		Prefix: &blobPrefix,
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("azure: list az://%s/%s: %w", cont, blobPrefix, err)
		}
		if page.Segment == nil {
			continue
		}
		for _, item := range page.Segment.BlobItems {
			if item == nil || item.Name == nil || strings.HasSuffix(*item.Name, "/") {
				continue
			}
			out = append(out, "az://"+cont+"/"+*item.Name)
		}
	}
	return out, nil
}

// ParsePath splits a path string into (container, blob). Exposed so callers
// (tests, diagnostics) can validate paths without opening.
//
// Accepted forms:
//
//	az://container/blob/path → container="container", blob="blob/path"
//	container/blob/path      → container="container", blob="blob/path"
func ParsePath(path string) (containerName, blobName string, err error) {
	trimmed := strings.TrimPrefix(path, "az://")
	idx := strings.IndexByte(trimmed, '/')
	if idx < 0 || idx == len(trimmed)-1 {
		return "", "", fmt.Errorf("azure: invalid path %q (want az://container/blob or container/blob)", path)
	}
	containerName = trimmed[:idx]
	blobName = trimmed[idx+1:]
	if containerName == "" {
		return "", "", fmt.Errorf("azure: empty container in %q", path)
	}
	return containerName, blobName, nil
}

// file is the Azure File implementation. Each ReadAt issues a fresh ranged
// DownloadStream — stateless per call so concurrent ReadAts don't share reader
// state.
type file struct {
	blob      *blob.Client
	container string
	name      string
	size      int64
	ctx       context.Context //nolint:containedctx
}

func (f *file) Size() int64 { return f.size }

// Close is a no-op — no per-file resources are held.
func (f *file) Close() error { return nil }

// ReadAt issues a single ranged DownloadStream covering [off, off+len(p)). On
// short reads at EOF returns (n, io.EOF) per io.ReaderAt's contract.
func (f *file) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("azure: negative offset %d", off)
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
	resp, err := f.blob.DownloadStream(f.ctx, &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{Offset: off, Count: want},
	})
	if err != nil {
		return 0, fmt.Errorf("azure: download az://%s/%s off=%d: %w", f.container, f.name, off, err)
	}
	defer resp.Body.Close()

	n, err := io.ReadFull(resp.Body, p[:want])
	if err != nil {
		// io.ReadFull returns io.ErrUnexpectedEOF for short reads; map that to
		// a regular EOF if we're at the blob's end so callers don't have to
		// handle two flavors.
		if err == io.ErrUnexpectedEOF && off+int64(n) >= f.size {
			err = io.EOF
		} else {
			return n, fmt.Errorf("azure: read body az://%s/%s off=%d: %w", f.container, f.name, off, err)
		}
	}
	if int64(n) < int64(len(p)) {
		// Caller asked for more bytes than the blob contains at this offset.
		return n, io.EOF
	}
	return n, nil
}

// writer buffers bytes in memory and uploads them as a single block blob on
// Close. Azure block blobs have no streaming-append API; buffering on Close
// matches the one-shot usage pattern of storage.WriteAll.
type writer struct {
	blob      *blockblob.Client
	container string
	name      string
	ctx       context.Context //nolint:containedctx
	buf       bytes.Buffer
}

func (w *writer) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *writer) Close() error {
	if _, err := w.blob.UploadBuffer(w.ctx, w.buf.Bytes(), nil); err != nil {
		return fmt.Errorf("azure: upload az://%s/%s: %w", w.container, w.name, err)
	}
	return nil
}

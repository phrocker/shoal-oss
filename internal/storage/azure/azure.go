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
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	azblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/google/uuid"

	shstorage "github.com/phrocker/shoal/internal/storage"
)

// Backend opens Azure blobs via a shared *service.Client. Safe for concurrent
// Open and concurrent ReadAt across many Files.
type Backend struct {
	svc            *service.Client
	ops            azureWriteOperations
	sourceProvider azureCopySourceProvider
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
		return &Backend{
			svc:            c.svc,
			ops:            sdkAzureWriteOperations{svc: c.svc},
			sourceProvider: rawAzureCopySourceProvider{svc: c.svc},
		}, nil
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
		return &Backend{
			svc:            svc,
			ops:            sdkAzureWriteOperations{svc: svc},
			sourceProvider: sourceProviderFromConnectionString(svc, connString),
		}, nil
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
	return &Backend{
		svc: svc,
		ops: sdkAzureWriteOperations{svc: svc},
		sourceProvider: tokenAzureCopySourceProvider{
			rawAzureCopySourceProvider: rawAzureCopySourceProvider{svc: svc},
			cred:                       cred,
		},
	}, nil
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

// Create opens a block-blob writer. Close first stages the bytes under a
// hidden sibling blob name, then conditionally promotes that exact blob into
// the destination.
func (b *Backend) Create(ctx context.Context, path string) (shstorage.Writer, error) {
	cont, name, err := ParsePath(path)
	if err != nil {
		return nil, err
	}
	ctx = contextOrBackground(ctx)
	ops := b.writeOperations()
	target, err := ops.head(ctx, cont, name)
	if err != nil && !isBlobNotFound(err) {
		return nil, fmt.Errorf("azure: inspect destination az://%s/%s: %w", cont, name, err)
	}
	if err == nil && target.etag == nil {
		return nil, fmt.Errorf("azure: inspect destination az://%s/%s: missing ETag", cont, name)
	}
	stageName, err := nextTemporaryStageName(name)
	if err != nil {
		return nil, fmt.Errorf("azure: stage temporary blob for az://%s/%s: %w", cont, name, err)
	}
	return &writer{
		ops:            ops,
		container:      cont,
		name:           name,
		stageName:      stageName,
		writeID:        uuid.NewString(),
		ctx:            ctx,
		targetExists:   err == nil,
		target:         target,
		sourceProvider: b.sourceProvider,
		cleanupTimeout: azureCleanupTimeout,
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
			if item == nil || item.Name == nil || strings.HasSuffix(*item.Name, "/") ||
				strings.HasPrefix(*item.Name, legacyStageDirPrefix) || isTemporaryStageName(*item.Name) {
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

const (
	maxBlobNameBytes      = 1024
	tempStageNamePrefix   = ".shl-"
	tempStageHashHexLen   = 4
	tempStageRandomHexLen = 10
	tempStageComponentLen = len(tempStageNamePrefix) + tempStageHashHexLen + tempStageRandomHexLen
	legacyStageDirPrefix  = ".shoal-tmp/"
	azureCopyBlockSize    = 100 << 20
	azureSourceSASExpiry  = 5 * time.Minute
)

var randomStageNameToken = func() (string, error) {
	buf := make([]byte, tempStageRandomHexLen/2)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate random staging token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func nextTemporaryStageName(name string) (string, error) {
	prefix := temporaryStageNamePrefixFor(name)
	hash := sha256.Sum256([]byte(name))
	token, err := randomStageNameToken()
	if err != nil {
		return "", err
	}
	hashHex := hex.EncodeToString(hash[:tempStageHashHexLen/2])
	if len(hashHex) < tempStageHashHexLen || len(token) < tempStageRandomHexLen {
		return "", fmt.Errorf("temporary blob token material too short")
	}
	component := tempStageNamePrefix + hashHex[:tempStageHashHexLen] + token[:tempStageRandomHexLen]
	if len(prefix)+len(component) > maxBlobNameBytes {
		return "", fmt.Errorf("temporary blob name exceeds %d-byte limit", maxBlobNameBytes)
	}
	return prefix + component, nil
}

func temporaryStageNamePrefixFor(name string) string {
	prefix := stageNameParentPrefix(name)
	for prefix != "" && maxBlobNameBytes-len(prefix) < tempStageComponentLen {
		trimmed := strings.TrimSuffix(prefix, "/")
		prefix = stageNameParentPrefix(trimmed)
	}
	return prefix
}

func stageNameParentPrefix(name string) string {
	if idx := strings.LastIndexByte(name, '/'); idx >= 0 {
		return name[:idx+1]
	}
	return ""
}

func isTemporaryStageName(name string) bool {
	base := name
	if idx := strings.LastIndexByte(base, '/'); idx >= 0 {
		base = base[idx+1:]
	}
	return strings.HasPrefix(name, legacyStageDirPrefix) ||
		(len(base) == tempStageComponentLen && strings.HasPrefix(base, tempStageNamePrefix))
}

type azureCopySource struct {
	url           string
	authorization *string
}

type azureCopySourceProvider interface {
	source(context.Context, string, string) (azureCopySource, error)
}

type rawAzureCopySourceProvider struct {
	svc *service.Client
}

func (p rawAzureCopySourceProvider) source(_ context.Context, containerName, blobName string) (azureCopySource, error) {
	return azureCopySource{url: p.blobURL(containerName, blobName)}, nil
}

func (p rawAzureCopySourceProvider) blobURL(containerName, blobName string) string {
	return p.svc.NewContainerClient(containerName).NewBlobClient(blobName).URL()
}

type sasAzureCopySourceProvider struct {
	rawAzureCopySourceProvider
	sasQuery string
}

func (p sasAzureCopySourceProvider) source(_ context.Context, containerName, blobName string) (azureCopySource, error) {
	sourceURL, err := url.Parse(p.blobURL(containerName, blobName))
	if err != nil {
		return azureCopySource{}, fmt.Errorf("azure: parse source blob URL: %w", err)
	}
	sourceURL.RawQuery = strings.TrimPrefix(p.sasQuery, "?")
	return azureCopySource{url: sourceURL.String()}, nil
}

type sharedKeyAzureCopySourceProvider struct {
	rawAzureCopySourceProvider
	cred *azblob.SharedKeyCredential
}

func (p sharedKeyAzureCopySourceProvider) source(_ context.Context, containerName, blobName string) (azureCopySource, error) {
	blobURL := p.blobURL(containerName, blobName)
	client, err := blob.NewClientWithSharedKeyCredential(blobURL, p.cred, nil)
	if err != nil {
		return azureCopySource{}, fmt.Errorf("azure: build shared-key source blob client: %w", err)
	}
	sourceURL, err := client.GetSASURL(sas.BlobPermissions{Read: true}, time.Now().Add(azureSourceSASExpiry), nil)
	if err != nil {
		return azureCopySource{}, fmt.Errorf("azure: sign source blob URL: %w", err)
	}
	return azureCopySource{url: sourceURL}, nil
}

type tokenAzureCopySourceProvider struct {
	rawAzureCopySourceProvider
	cred azcore.TokenCredential
}

func (p tokenAzureCopySourceProvider) source(ctx context.Context, containerName, blobName string) (azureCopySource, error) {
	token, err := p.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{"https://storage.azure.com/.default"}})
	if err != nil {
		return azureCopySource{}, fmt.Errorf("azure: acquire source copy token: %w", err)
	}
	authorization := "Bearer " + token.Token
	return azureCopySource{
		url:           p.blobURL(containerName, blobName),
		authorization: &authorization,
	}, nil
}

func sourceProviderFromConnectionString(svc *service.Client, connString string) azureCopySourceProvider {
	values := parseConnectionString(connString)
	base := rawAzureCopySourceProvider{svc: svc}
	if sasQuery := values["SharedAccessSignature"]; sasQuery != "" {
		return sasAzureCopySourceProvider{rawAzureCopySourceProvider: base, sasQuery: sasQuery}
	}
	accountName := values["AccountName"]
	accountKey := values["AccountKey"]
	if accountName == "" || accountKey == "" {
		return base
	}
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return base
	}
	return sharedKeyAzureCopySourceProvider{rawAzureCopySourceProvider: base, cred: cred}
}

func parseConnectionString(connString string) map[string]string {
	values := make(map[string]string)
	for _, part := range strings.Split(connString, ";") {
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		values[key] = value
	}
	return values
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

type azureObjectState struct {
	etag     *azcore.ETag
	size     int64
	metadata map[string]*string
}

type azureWriteOperations interface {
	head(context.Context, string, string) (azureObjectState, error)
	uploadStage(context.Context, string, string, []byte, string) (azureObjectState, error)
	promote(context.Context, string, string, string, azureCopySource, azureObjectState, bool, azureObjectState) error
	deleteStage(context.Context, string, string, azureObjectState) error
}

type sdkAzureWriteOperations struct {
	svc *service.Client
}

func (o sdkAzureWriteOperations) head(
	ctx context.Context, containerName, name string,
) (azureObjectState, error) {
	out, err := o.svc.NewContainerClient(containerName).NewBlobClient(name).GetProperties(ctx, nil)
	if err != nil {
		return azureObjectState{}, err
	}
	state := azureObjectState{etag: out.ETag, metadata: out.Metadata}
	if out.ContentLength != nil {
		state.size = *out.ContentLength
	}
	return state, nil
}

func (o sdkAzureWriteOperations) uploadStage(
	ctx context.Context,
	containerName, name string,
	data []byte,
	writeID string,
) (azureObjectState, error) {
	out, err := o.svc.NewContainerClient(containerName).NewBlockBlobClient(name).UploadBuffer(
		ctx,
		data,
		&blockblob.UploadBufferOptions{
			Metadata: map[string]*string{"shoal-write-id": to.Ptr(writeID)},
			AccessConditions: &blob.AccessConditions{
				ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)},
			},
		},
	)
	if err != nil {
		return azureObjectState{}, err
	}
	return azureObjectState{
		etag:     out.ETag,
		size:     int64(len(data)),
		metadata: map[string]*string{"shoal-write-id": to.Ptr(writeID)},
	}, nil
}

func (o sdkAzureWriteOperations) promote(
	ctx context.Context,
	containerName, stageName, name string,
	source azureCopySource,
	stage azureObjectState,
	targetExists bool,
	target azureObjectState,
) error {
	bb := o.svc.NewContainerClient(containerName).NewBlockBlobClient(name)
	conditions := &blob.ModifiedAccessConditions{}
	if targetExists {
		conditions.IfMatch = target.etag
	} else {
		conditions.IfNoneMatch = to.Ptr(azcore.ETagAny)
	}

	blockIDs := make([]string, 0, max(1, int((stage.size+azureCopyBlockSize-1)/azureCopyBlockSize)))
	for offset, index := int64(0), 0; offset < stage.size; offset, index = offset+azureCopyBlockSize, index+1 {
		count := min(stage.size-offset, int64(azureCopyBlockSize))
		blockID := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%08d", index)))
		options := &blockblob.StageBlockFromURLOptions{
			CopySourceAuthorization: source.authorization,
			SourceModifiedAccessConditions: &blob.SourceModifiedAccessConditions{
				SourceIfMatch: stage.etag,
			},
			Range: blob.HTTPRange{Offset: offset, Count: count},
		}
		if _, err := bb.StageBlockFromURL(ctx, blockID, source.url, options); err != nil {
			return err
		}
		blockIDs = append(blockIDs, blockID)
	}

	if _, err := bb.CommitBlockList(ctx, blockIDs, &blockblob.CommitBlockListOptions{
		Metadata: stage.metadata,
		AccessConditions: &blob.AccessConditions{
			ModifiedAccessConditions: conditions,
		},
	}); err != nil {
		return err
	}
	return nil
}

func (o sdkAzureWriteOperations) deleteStage(
	ctx context.Context, containerName, name string, stage azureObjectState,
) error {
	_, err := o.svc.NewContainerClient(containerName).NewBlobClient(name).Delete(
		ctx,
		&blob.DeleteOptions{
			AccessConditions: &blob.AccessConditions{
				ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: stage.etag},
			},
		},
	)
	return err
}

var azureCleanupTimeout = 10 * time.Second

func (b *Backend) writeOperations() azureWriteOperations {
	if b.ops != nil {
		return b.ops
	}
	return sdkAzureWriteOperations{svc: b.svc}
}

// writer buffers bytes in memory, stages them, and conditionally publishes
// them on Close.
type writer struct {
	ops            azureWriteOperations
	container      string
	name           string
	stageName      string
	writeID        string
	ctx            context.Context //nolint:containedctx
	targetExists   bool
	target         azureObjectState
	sourceProvider azureCopySourceProvider
	stage          azureObjectState
	stageCreated   bool
	cleanupTimeout time.Duration
	buf            bytes.Buffer
	closed         bool
	aborted        bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.aborted {
		return 0, fmt.Errorf("azure: writer already aborted")
	}
	if w.closed {
		return 0, fmt.Errorf("azure: writer already closed")
	}
	return w.buf.Write(p)
}

func (w *writer) Close() error {
	if w.aborted {
		return fmt.Errorf("azure: writer already aborted")
	}
	if w.closed {
		_ = w.cleanupStage()
		return nil
	}
	if !w.stageCreated {
		stage, err := w.ops.uploadStage(w.ctx, w.container, w.stageName, w.buf.Bytes(), w.writeID)
		if err != nil {
			if verified, verifyErr := w.verifyStage(); verifyErr != nil || !verified {
				return errors.Join(
					fmt.Errorf("azure: stage upload az://%s/%s: %w", w.container, w.stageName, err),
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
				fmt.Errorf("azure: temporary blob az://%s/%s is missing an ETag", w.container, w.stageName),
				err,
				w.cleanupStage(),
			)
		}
	}
	source, err := w.promotionSource()
	if err != nil {
		return errors.Join(err, w.cleanupStage())
	}
	if err := w.ops.promote(
		w.ctx, w.container, w.stageName, w.name, source, w.stage, w.targetExists, w.target,
	); err != nil {
		if verified, verifyErr := w.verifyDestination(); verifyErr != nil || !verified {
			return errors.Join(
				fmt.Errorf("azure: promote az://%s/%s to az://%s/%s: %w",
					w.container, w.stageName, w.container, w.name, err),
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
		return fmt.Errorf("azure: writer already closed")
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
	state, err := w.ops.head(ctx, w.container, w.stageName)
	if err != nil {
		if isBlobNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("azure: verify temporary blob az://%s/%s: %w", w.container, w.stageName, err)
	}
	if state.size != int64(w.buf.Len()) || metadataValue(state.metadata, "shoal-write-id") != w.writeID {
		return false, nil
	}
	w.stage = state
	w.stageCreated = true
	return true, nil
}

func (w *writer) verifyDestination() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), w.cleanupTimeout)
	defer cancel()
	state, err := w.ops.head(ctx, w.container, w.name)
	if err != nil {
		if isBlobNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("azure: verify destination az://%s/%s: %w", w.container, w.name, err)
	}
	if w.targetExists && equalETags(state.etag, w.target.etag) {
		return false, nil
	}
	return state.size == int64(w.buf.Len()) &&
		metadataValue(state.metadata, "shoal-write-id") == w.writeID, nil
}

func (w *writer) cleanupStage() error {
	if !w.stageCreated {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.cleanupTimeout)
	defer cancel()
	if err := w.ops.deleteStage(ctx, w.container, w.stageName, w.stage); err != nil && !isBlobNotFound(err) {
		return fmt.Errorf("azure: remove temporary blob az://%s/%s: %w", w.container, w.stageName, err)
	}
	w.stageCreated = false
	return nil
}

func (w *writer) promotionSource() (azureCopySource, error) {
	if w.sourceProvider == nil {
		return azureCopySource{}, fmt.Errorf("azure: no source authorization configured for az://%s/%s", w.container, w.stageName)
	}
	source, err := w.sourceProvider.source(w.ctx, w.container, w.stageName)
	if err != nil {
		return azureCopySource{}, fmt.Errorf("azure: authorize staged source az://%s/%s: %w", w.container, w.stageName, err)
	}
	return source, nil
}

func metadataValue(metadata map[string]*string, key string) string {
	value := metadata[key]
	if value == nil {
		return ""
	}
	return *value
}

func equalETags(a, b *azcore.ETag) bool {
	return a != nil && b != nil && a.Equals(*b)
}

func isBlobNotFound(err error) bool {
	return bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound)
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

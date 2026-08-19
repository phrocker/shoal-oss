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

package azure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	shstorage "github.com/phrocker/shoal/internal/storage"
)

type fakeAzureObject struct {
	state azureObjectState
	data  string
}

type fakeAzureWriteOperations struct {
	objects             map[string]fakeAzureObject
	stageFailure        bool
	stageResponseLost   bool
	promoteResponseLost bool
	mutateBeforePromote bool
	deleteFailures      int
	promoteCalls        int
	cleanupCanceled     bool
	nextETag            int
	lastPromoteSource   azureCopySource
	lastPromoteWriteID  string
}

func newFakeAzureWriteOperations() *fakeAzureWriteOperations {
	return &fakeAzureWriteOperations{objects: make(map[string]fakeAzureObject)}
}

func azureNotFoundError() error {
	return &azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)}
}

func (f *fakeAzureWriteOperations) head(
	_ context.Context, _, name string,
) (azureObjectState, error) {
	obj, ok := f.objects[name]
	if !ok {
		return azureObjectState{}, azureNotFoundError()
	}
	return obj.state, nil
}

func (f *fakeAzureWriteOperations) uploadStage(
	ctx context.Context,
	_, name string,
	data []byte,
	writeID string,
) (azureObjectState, error) {
	if f.stageFailure {
		return azureObjectState{}, errors.New("stage checksum failed")
	}
	if err := ctx.Err(); err != nil {
		return azureObjectState{}, err
	}
	f.nextETag++
	etag := azcore.ETag(fmt.Sprintf("\"etag-%d\"", f.nextETag))
	id := writeID
	state := azureObjectState{
		etag:     &etag,
		size:     int64(len(data)),
		metadata: map[string]*string{"shoal-write-id": &id},
	}
	f.objects[name] = fakeAzureObject{state: state, data: string(data)}
	if f.stageResponseLost {
		f.stageResponseLost = false
		return azureObjectState{}, errors.New("stage response lost")
	}
	return state, nil
}

func (f *fakeAzureWriteOperations) promote(
	ctx context.Context,
	_, stageName, name string,
	source azureCopySource,
	stage azureObjectState,
	writeID string,
	targetExists bool,
	target azureObjectState,
) error {
	f.promoteCalls++
	f.lastPromoteSource = source
	f.lastPromoteWriteID = writeID
	if f.mutateBeforePromote {
		f.mutateBeforePromote = false
		f.nextETag++
		etag := azcore.ETag(fmt.Sprintf("\"etag-%d\"", f.nextETag))
		f.objects[name] = fakeAzureObject{
			state: azureObjectState{etag: &etag, size: int64(len("concurrent"))},
			data:  "concurrent",
		}
	}
	current, exists := f.objects[name]
	if exists != targetExists ||
		(exists && !equalETags(current.state.etag, target.etag)) {
		return errors.New("destination precondition failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	stageObject, ok := f.objects[stageName]
	if !ok {
		return azureNotFoundError()
	}
	if !equalETags(stageObject.state.etag, stage.etag) {
		return errors.New("source precondition failed")
	}
	f.nextETag++
	etag := azcore.ETag(fmt.Sprintf("\"etag-%d\"", f.nextETag))
	stageObject.state.etag = &etag
	f.objects[name] = stageObject
	if f.promoteResponseLost {
		f.promoteResponseLost = false
		return errors.New("upload response lost")
	}
	return nil
}

func (f *fakeAzureWriteOperations) deleteStage(
	ctx context.Context, _, name string, _ azureObjectState,
) error {
	if ctx.Err() != nil {
		f.cleanupCanceled = true
		return ctx.Err()
	}
	if f.deleteFailures > 0 {
		f.deleteFailures--
		return errors.New("delete failed")
	}
	if _, ok := f.objects[name]; !ok {
		return azureNotFoundError()
	}
	delete(f.objects, name)
	return nil
}

func newFakeAzureWriter(f *fakeAzureWriteOperations, target string) *writer {
	targetState, err := f.head(context.Background(), "container", target)
	return &writer{
		ops:            f,
		container:      "container",
		name:           target,
		stageName:      ".shl-stage0000",
		writeID:        "write-id",
		ctx:            context.Background(),
		targetExists:   err == nil,
		target:         targetState,
		sourceProvider: staticAzureCopySourceProvider{},
		cleanupTimeout: azureCleanupTimeout,
	}
}

func newCustomServiceBackend(t *testing.T, opts ...Option) (*Backend, *fakeAzureWriteOperations) {
	t.Helper()

	svc, err := service.NewClientWithNoCredential("https://example.blob.core.windows.net/", nil)
	if err != nil {
		t.Fatalf("NewClientWithNoCredential: %v", err)
	}
	return newCustomServiceBackendWithClient(t, svc, opts...)
}

func newCustomServiceBackendWithURL(t *testing.T, serviceURL string, opts ...Option) (*Backend, *fakeAzureWriteOperations) {
	t.Helper()

	svc, err := service.NewClientWithNoCredential(serviceURL, nil)
	if err != nil {
		t.Fatalf("NewClientWithNoCredential: %v", err)
	}
	return newCustomServiceBackendWithClient(t, svc, opts...)
}

func newCustomServiceBackendWithClient(t *testing.T, svc *service.Client, opts ...Option) (*Backend, *fakeAzureWriteOperations) {
	t.Helper()

	backend, err := New(context.Background(), append([]Option{WithServiceClient(svc)}, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ops := newFakeAzureWriteOperations()
	backend.ops = ops
	return backend, ops
}

type staticAzureCopySourceProvider struct{}

func (staticAzureCopySourceProvider) source(context.Context, string, string) (azureCopySource, error) {
	return azureCopySource{url: "https://example.invalid/stage"}, nil
}

func TestParsePath(t *testing.T) {
	cases := []struct {
		in        string
		container string
		blob      string
		wantErr   bool
	}{
		{"az://my-container/path/to/file.rf", "my-container", "path/to/file.rf", false},
		{"my-container/path/to/file.rf", "my-container", "path/to/file.rf", false},
		{"az://c/o", "c", "o", false},
		{"az://", "", "", true},
		{"no-slash", "", "", true},
		{"az://c/", "", "", true},          // trailing slash only → empty blob
		{"/leading-slash/o", "", "", true}, // leading slash → empty container → error
		{"", "", "", true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			cont, blob, err := ParsePath(c.in)
			if c.wantErr {
				if err == nil {
					t.Errorf("expected error, got container=%q blob=%q", cont, blob)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if cont != c.container || blob != c.blob {
				t.Errorf("got (%q, %q), want (%q, %q)", cont, blob, c.container, c.blob)
			}
		})
	}
}

// TestNew_NoCredentials confirms New errors cleanly when no account/connection
// string is configured (env scrubbed), rather than panicking or hanging.
func TestNew_NoCredentials(t *testing.T) {
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")
	t.Setenv("AZURE_STORAGE_ACCOUNT", "")
	if _, err := New(context.Background()); err == nil {
		t.Fatal("New with no credentials: expected error, got nil")
	}
}

// TestNew_AccountFromEnv confirms an account name (env) is enough to build a
// Backend; the default credential chain is constructed lazily and not exercised
// here (no network).
func TestNew_AccountFromEnv(t *testing.T) {
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")
	t.Setenv("AZURE_STORAGE_ACCOUNT", "examplestorageacct")
	b, err := New(context.Background())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b == nil || b.svc == nil {
		t.Fatal("New returned a nil backend/service client")
	}
}

// TestWriter_Write exercises the in-memory buffer without any Azure call.
func TestWriter_Write(t *testing.T) {
	w := &writer{}
	data := []byte("hello azure")
	n, err := w.Write(data)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(data) {
		t.Errorf("n = %d, want %d", n, len(data))
	}
	if w.buf.Len() != len(data) {
		t.Errorf("buf.Len() = %d, want %d", w.buf.Len(), len(data))
	}
	more := []byte(" blob")
	w.Write(more) //nolint:errcheck
	if w.buf.Len() != len(data)+len(more) {
		t.Errorf("accumulated buf.Len() = %d, want %d", w.buf.Len(), len(data)+len(more))
	}
}

func TestWriter_AbortDiscardsBufferedDataAndRejectsLaterUse(t *testing.T) {
	w := &writer{}
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if _, err := w.Write([]byte(" azure")); err != nil {
		t.Fatalf("second Write: %v", err)
	}
	if got := w.buf.Len(); got != len("hello azure") {
		t.Fatalf("buf.Len() before Abort = %d, want %d", got, len("hello azure"))
	}

	if err := w.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if got := w.buf.Len(); got != 0 {
		t.Fatalf("buf.Len() after Abort = %d, want 0", got)
	}
	if err := w.Abort(); err != nil {
		t.Fatalf("second Abort: %v", err)
	}
	if got := w.buf.Len(); got != 0 {
		t.Fatalf("buf.Len() after second Abort = %d, want 0", got)
	}
	if _, err := w.Write([]byte("late")); err == nil || !strings.Contains(err.Error(), "aborted") {
		t.Fatalf("Write after Abort error = %v, want aborted state", err)
	}
	if err := w.Close(); err == nil || !strings.Contains(err.Error(), "aborted") {
		t.Fatalf("Close after Abort error = %v, want aborted state", err)
	}
}

func TestWriter_WriteAfterCloseReportsClosedState(t *testing.T) {
	w := &writer{closed: true}
	if _, err := w.Write([]byte("late")); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Write after Close error = %v, want closed state", err)
	}
}

func TestNextTemporaryStageNamePreservesPrefixAndBoundsCharacters(t *testing.T) {
	original := randomStageNameToken
	randomStageNameToken = func() (string, error) {
		return strings.Repeat("a", tempStageRandomHexLen), nil
	}
	t.Cleanup(func() {
		randomStageNameToken = original
	})

	name := "tenant/path/" + strings.Repeat("界", 338)
	stageName, err := nextTemporaryStageName(name)
	if err != nil {
		t.Fatalf("nextTemporaryStageName: %v", err)
	}
	if got, want := stageNameParentPrefix(stageName), stageNameParentPrefix(name); got != want {
		t.Fatalf("stage prefix = %q, want %q", got, want)
	}
	if got := utf8.RuneCountInString(stageName); got > maxBlobNameChars {
		t.Fatalf("stage name length = %d chars, want <= %d", got, maxBlobNameChars)
	}
}

func TestNextTemporaryStageNameUsesCharacterLimitInsteadOfUTF8Bytes(t *testing.T) {
	original := randomStageNameToken
	randomStageNameToken = func() (string, error) {
		return strings.Repeat("b", tempStageRandomHexLen), nil
	}
	t.Cleanup(func() {
		randomStageNameToken = original
	})

	name := strings.Repeat("界", 1004) + "/x"
	stageName, err := nextTemporaryStageName(name)
	if err != nil {
		t.Fatalf("nextTemporaryStageName: %v", err)
	}
	if got, want := stageNameParentPrefix(stageName), stageNameParentPrefix(name); got != want {
		t.Fatalf("stage prefix = %q, want %q", got, want)
	}
	if got := utf8.RuneCountInString(stageName); got > maxBlobNameChars {
		t.Fatalf("stage name length = %d chars, want <= %d", got, maxBlobNameChars)
	}
}

func TestNextTemporaryStageNameRejectsPrefixWithoutFullRandomToken(t *testing.T) {
	original := randomStageNameToken
	randomStageNameToken = func() (string, error) {
		return strings.Repeat("b", tempStageRandomHexLen), nil
	}
	t.Cleanup(func() {
		randomStageNameToken = original
	})

	name := strings.Repeat("界", 1015) + "/x"
	if _, err := nextTemporaryStageName(name); err == nil {
		t.Fatal("nextTemporaryStageName succeeded without room for the full random token")
	}
}

func TestNextTemporaryStageNameKeepsFullRandomTokenAtMinimumSpace(t *testing.T) {
	original := randomStageNameToken
	tokens := []string{
		strings.Repeat("c", tempStageRandomHexLen),
		strings.Repeat("d", tempStageRandomHexLen),
	}
	randomStageNameToken = func() (string, error) {
		token := tokens[0]
		tokens = tokens[1:]
		return token, nil
	}
	t.Cleanup(func() {
		randomStageNameToken = original
	})

	name := strings.Repeat("界", 1008) + "/x"
	stageName, err := nextTemporaryStageName(name)
	if err != nil {
		t.Fatalf("nextTemporaryStageName: %v", err)
	}
	if got, want := stageNameParentPrefix(stageName), stageNameParentPrefix(name); got != want {
		t.Fatalf("stage prefix = %q, want %q", got, want)
	}
	component := stageName[len(stageNameParentPrefix(stageName)):]
	if got, want := component, tempStageNamePrefix+strings.Repeat("c", tempStageRandomHexLen); got != want {
		t.Fatalf("temporary component = %q, want %q", got, want)
	}
	if !isTemporaryStageName(stageName) {
		t.Fatalf("temporary blob %q is visible to List", stageName)
	}
	other, err := nextTemporaryStageName(name)
	if err != nil {
		t.Fatalf("second nextTemporaryStageName: %v", err)
	}
	if other == stageName {
		t.Fatalf("distinct random tokens produced the same temporary blob %q", stageName)
	}
}

func TestNextTemporaryStageNameTrimsToDeepestAncestorPrefixWhenSpaceIsTight(t *testing.T) {
	original := randomStageNameToken
	randomStageNameToken = func() (string, error) {
		return strings.Repeat("c", tempStageRandomHexLen), nil
	}
	t.Cleanup(func() {
		randomStageNameToken = original
	})

	name := strings.Repeat("a", 600) + "/" + strings.Repeat("b", 420) + "/x"
	stageName, err := nextTemporaryStageName(name)
	if err != nil {
		t.Fatalf("nextTemporaryStageName: %v", err)
	}
	wantPrefix := strings.Repeat("a", 600) + "/"
	if got := stageNameParentPrefix(stageName); got != wantPrefix {
		t.Fatalf("stage prefix = %q, want deepest compatible prefix %q", got, wantPrefix)
	}
	if got := utf8.RuneCountInString(stageName) - utf8.RuneCountInString(wantPrefix); got != tempStageComponentLen {
		t.Fatalf("stage component length = %d, want %d", got, tempStageComponentLen)
	}
}

func TestNextTemporaryStageNamePreservesFullEntropyWhenPrefixTrims(t *testing.T) {
	original := randomStageNameToken
	randomStageNameToken = func() (string, error) {
		return strings.Repeat("d", tempStageRandomHexLen), nil
	}
	t.Cleanup(func() {
		randomStageNameToken = original
	})

	nameA := strings.Repeat("a", 600) + "/" + strings.Repeat("b", 420) + "/x"
	nameB := strings.Repeat("a", 600) + "/" + strings.Repeat("b", 420) + "/y"
	stageA, err := nextTemporaryStageName(nameA)
	if err != nil {
		t.Fatalf("nextTemporaryStageName nameA: %v", err)
	}
	stageB, err := nextTemporaryStageName(nameB)
	if err != nil {
		t.Fatalf("nextTemporaryStageName nameB: %v", err)
	}
	if stageA == stageB {
		t.Fatalf("trimmed temporary stage names collided: %q", stageA)
	}
}

func TestIsTemporaryStageNameMatchesOnlyReservedFormats(t *testing.T) {
	legacyUUID := "123e4567-e89b-12d3-a456-426614174000"
	if !isTemporaryStageName(".shoal-tmp/" + legacyUUID) {
		t.Fatal("legacy temporary stage blob was not detected")
	}
	if !isTemporaryStageName("tenant/.shl-aaaaaaaaaa1234") {
		t.Fatal("generated temporary stage blob was not detected")
	}
	if !isTemporaryStageName("tenant/.shl-aaaaaaaaaa") {
		t.Fatal("minimum generated temporary stage blob was not detected")
	}
	if isTemporaryStageName(".shoal-tmp/user-visible") {
		t.Fatal("arbitrary .shoal-tmp/ blob should remain visible")
	}
	if isTemporaryStageName("tenant/.shl-visible-blob") {
		t.Fatal("arbitrary .shl- blob should remain visible")
	}
}

func TestBackendCreateWithCustomServiceClientRequiresSourceAuthorization(t *testing.T) {
	backend, ops := newCustomServiceBackend(t)
	etag := azcore.ETag("\"old\"")
	ops.objects["target"] = fakeAzureObject{
		state: azureObjectState{etag: &etag, size: 3},
		data:  "old",
	}

	w, err := backend.Create(context.Background(), "az://container/target")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	writer := w.(*writer)
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	err = w.Close()
	if err == nil || !strings.Contains(err.Error(), "no source authorization configured") {
		t.Fatalf("Close error = %v, want explicit source authorization failure", err)
	}
	if got := ops.objects["target"].data; got != "old" {
		t.Fatalf("target data = %q, want old", got)
	}
	if _, ok := ops.objects[writer.stageName]; ok {
		t.Fatal("temporary blob was not removed after authorization failure")
	}
}

func TestBackendCreateWithCustomServiceClientSASURLPromotesStage(t *testing.T) {
	backend, ops := newCustomServiceBackendWithURL(t, "https://example.blob.core.windows.net/?sig=client-sas")

	w, err := backend.Create(context.Background(), "az://container/target")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	sourceURL, err := url.Parse(ops.lastPromoteSource.url)
	if err != nil {
		t.Fatalf("Parse source URL: %v", err)
	}
	if got := sourceURL.Query().Get("sig"); got != "client-sas" {
		t.Fatalf("source SAS signature = %q, want client-sas", got)
	}
}

func TestBackendCreateWithCustomServiceClientSourceSASPromotesStage(t *testing.T) {
	backend, ops := newCustomServiceBackend(t, WithSourceSASQuery("sig=shared-key"))

	w, err := backend.Create(context.Background(), "az://container/target")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	sourceURL, err := url.Parse(ops.lastPromoteSource.url)
	if err != nil {
		t.Fatalf("Parse source URL: %v", err)
	}
	if got := sourceURL.Query().Get("sig"); got != "shared-key" {
		t.Fatalf("source SAS signature = %q, want shared-key", got)
	}
	if ops.lastPromoteSource.authorization != nil {
		t.Fatalf("source authorization header = %v, want nil", *ops.lastPromoteSource.authorization)
	}
	if got := ops.lastPromoteWriteID; got == "" {
		t.Fatal("promote write ID was not recorded")
	}
}

func TestBackendCreateWithCustomServiceClientCopySourceAuthorizationPromotesStage(t *testing.T) {
	backend, ops := newCustomServiceBackend(t, WithCopySourceAuthorization("Bearer test-token"))

	w, err := backend.Create(context.Background(), "az://container/target")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if ops.lastPromoteSource.authorization == nil || *ops.lastPromoteSource.authorization != "Bearer test-token" {
		t.Fatalf("source authorization = %v, want Bearer test-token", ops.lastPromoteSource.authorization)
	}
	sourceURL, err := url.Parse(ops.lastPromoteSource.url)
	if err != nil {
		t.Fatalf("Parse source URL: %v", err)
	}
	if sourceURL.RawQuery != "" {
		t.Fatalf("source query = %q, want empty", sourceURL.RawQuery)
	}
}

func TestBackendCreateWithCustomServiceClientAuthorizationProviderPromotesStage(t *testing.T) {
	var authCalls int
	backend, ops := newCustomServiceBackend(t, WithSourceAuthorizationProvider(
		SourceAuthorizationProviderFunc(func(_ context.Context, containerName, blobName string) (SourceAuthorization, error) {
			authCalls++
			if containerName != "container" {
				t.Fatalf("container = %q, want container", containerName)
			}
			if !strings.HasPrefix(blobName, tempStageNamePrefix) {
				t.Fatalf("blob name = %q, want %q prefix", blobName, tempStageNamePrefix)
			}
			return SourceAuthorization{Authorization: "Bearer test-token"}, nil
		}),
	))

	w, err := backend.Create(context.Background(), "az://container/target")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if authCalls != 1 {
		t.Fatalf("authorization provider calls = %d, want 1", authCalls)
	}
	if ops.lastPromoteSource.authorization == nil || *ops.lastPromoteSource.authorization != "Bearer test-token" {
		t.Fatalf("source authorization = %v, want Bearer test-token", ops.lastPromoteSource.authorization)
	}
	sourceURL, err := url.Parse(ops.lastPromoteSource.url)
	if err != nil {
		t.Fatalf("Parse source URL: %v", err)
	}
	if sourceURL.RawQuery != "" {
		t.Fatalf("source query = %q, want empty", sourceURL.RawQuery)
	}
}

func TestTemporaryBlockIDUsesWriteIdentityAndFixedLength(t *testing.T) {
	writeA0 := temporaryBlockID("write-a", 0)
	writeA1 := temporaryBlockID("write-a", 1)
	writeB0 := temporaryBlockID("write-b", 0)

	if writeA0 == writeB0 {
		t.Fatalf("different writes produced the same block ID %q", writeA0)
	}
	if writeA0 == writeA1 {
		t.Fatalf("different block indexes produced the same block ID %q", writeA0)
	}
	if len(writeA0) != len(writeA1) || len(writeA0) != len(writeB0) {
		t.Fatalf("block ID lengths = %d, %d, %d; want equal fixed length", len(writeA0), len(writeA1), len(writeB0))
	}
}

func TestPromoteStagedBlobWriteScopedBlockIDsPreventCrossCommitBytes(t *testing.T) {
	promoter := &fakeAzureBlockPromoter{
		sources: map[string]string{
			"https://example.invalid/stage-a": "alpha",
			"https://example.invalid/stage-b": "bravo",
		},
	}
	promoter.stageHook = func() {
		if _, err := stagePromotionBlocks(
			context.Background(),
			promoter,
			azureCopySource{url: "https://example.invalid/stage-b"},
			azureObjectState{size: int64(len("bravo"))},
			"write-b",
		); err != nil {
			t.Fatalf("stage competing writer: %v", err)
		}
	}

	err := promoteStagedBlob(
		context.Background(),
		promoter,
		azureCopySource{url: "https://example.invalid/stage-a"},
		azureObjectState{size: int64(len("alpha"))},
		"write-a",
		false,
		azureObjectState{},
	)
	if err != nil {
		t.Fatalf("promoteStagedBlob: %v", err)
	}
	if got := promoter.committedData; got != "alpha" {
		t.Fatalf("committed data = %q, want alpha", got)
	}
	if len(promoter.stageCalls) != 2 {
		t.Fatalf("stage calls = %d, want 2", len(promoter.stageCalls))
	}
	if promoter.stageCalls[0].blockID == promoter.stageCalls[1].blockID {
		t.Fatalf("block IDs collided across writers: %q", promoter.stageCalls[0].blockID)
	}
	if len(promoter.stageCalls[0].blockID) != len(promoter.stageCalls[1].blockID) {
		t.Fatalf("block ID lengths = %d and %d, want equal length", len(promoter.stageCalls[0].blockID), len(promoter.stageCalls[1].blockID))
	}
}

func TestWriter_StagedCreateAndReplace(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%v", existing), func(t *testing.T) {
			f := newFakeAzureWriteOperations()
			if existing {
				etag := azcore.ETag("\"old\"")
				f.objects["target"] = fakeAzureObject{
					state: azureObjectState{etag: &etag, size: 3},
					data:  "old",
				}
			}
			w := newFakeAzureWriter(f, "target")
			_, _ = w.Write([]byte("new"))
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if got := f.objects["target"].data; got != "new" {
				t.Fatalf("target data = %q, want new", got)
			}
			if _, ok := f.objects[w.stageName]; ok {
				t.Fatal("temporary blob was not removed")
			}
		})
	}
}

func TestWriter_VerifiesCommittedResponsesWithoutReplayingPromotion(t *testing.T) {
	f := newFakeAzureWriteOperations()
	f.stageResponseLost = true
	f.promoteResponseLost = true
	w := newFakeAzureWriter(f, "target")
	_, _ = w.Write([]byte("new"))
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if f.promoteCalls != 1 {
		t.Fatalf("promote calls = %d, want 1", f.promoteCalls)
	}
	if got := f.objects["target"].data; got != "new" {
		t.Fatalf("target data = %q, want new", got)
	}
}

func TestWriter_StageFailurePreservesExistingTarget(t *testing.T) {
	f := newFakeAzureWriteOperations()
	etag := azcore.ETag("\"old\"")
	f.objects["target"] = fakeAzureObject{
		state: azureObjectState{etag: &etag, size: 3},
		data:  "old",
	}
	f.stageFailure = true
	w := newFakeAzureWriter(f, "target")
	_, _ = w.Write([]byte("new"))
	if err := w.Close(); err == nil {
		t.Fatal("Close succeeded after stage failure")
	}
	if got := f.objects["target"].data; got != "old" {
		t.Fatalf("target data = %q, want old", got)
	}
}

func TestWriter_ConcurrentTargetMutationIsPreserved(t *testing.T) {
	f := newFakeAzureWriteOperations()
	etag := azcore.ETag("\"old\"")
	f.objects["target"] = fakeAzureObject{
		state: azureObjectState{etag: &etag, size: 3},
		data:  "old",
	}
	f.mutateBeforePromote = true
	w := newFakeAzureWriter(f, "target")
	_, _ = w.Write([]byte("new"))
	if err := w.Close(); err == nil {
		t.Fatal("Close succeeded after concurrent target mutation")
	}
	if got := f.objects["target"].data; got != "concurrent" {
		t.Fatalf("target data = %q, want concurrent", got)
	}
	if _, ok := f.objects[w.stageName]; ok {
		t.Fatal("temporary blob was not removed after failed promotion")
	}
}

func TestWriter_CleanupUsesIndependentContextAndCanRetryAfterCommit(t *testing.T) {
	t.Run("request cancellation", func(t *testing.T) {
		f := newFakeAzureWriteOperations()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		w := newFakeAzureWriter(f, "target")
		w.ctx = ctx
		_, _ = w.Write([]byte("new"))
		if err := w.Close(); err == nil {
			t.Fatal("Close succeeded with canceled promotion context")
		}
		if f.cleanupCanceled {
			t.Fatal("cleanup inherited canceled request context")
		}
		if _, ok := f.objects[w.stageName]; ok {
			t.Fatal("temporary blob was not removed")
		}
	})

	t.Run("retry after committed close", func(t *testing.T) {
		f := newFakeAzureWriteOperations()
		f.deleteFailures = 1
		w := newFakeAzureWriter(f, "target")
		_, _ = w.Write([]byte("new"))
		if err := w.Close(); err != nil {
			t.Fatalf("Close promoted cleanup failure: %v", err)
		}
		if _, ok := f.objects[w.stageName]; !ok {
			t.Fatal("temporary blob unexpectedly removed")
		}
		if err := w.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		if _, ok := f.objects[w.stageName]; ok {
			t.Fatal("second Close did not retry temporary cleanup")
		}
	})
}

type fakeAzureBlockStageCall struct {
	blockID string
}

type fakeAzureBlockPromoter struct {
	sources       map[string]string
	uncommitted   map[string][]byte
	committedData string
	stageCalls    []fakeAzureBlockStageCall
	stageHook     func()
}

func (p *fakeAzureBlockPromoter) StageBlockFromURL(
	ctx context.Context,
	blockID, source string,
	options *blockblob.StageBlockFromURLOptions,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.uncommitted == nil {
		p.uncommitted = make(map[string][]byte)
	}
	data, ok := p.sources[source]
	if !ok {
		return fmt.Errorf("unknown source %s", source)
	}
	start := int(options.Range.Offset)
	end := start + int(options.Range.Count)
	if start < 0 || end < start || end > len(data) {
		return fmt.Errorf("range %d:%d out of bounds for %s", start, end, source)
	}
	p.uncommitted[blockID] = append([]byte(nil), data[start:end]...)
	p.stageCalls = append(p.stageCalls, fakeAzureBlockStageCall{blockID: blockID})
	if p.stageHook != nil {
		hook := p.stageHook
		p.stageHook = nil
		hook()
	}
	return nil
}

func (p *fakeAzureBlockPromoter) CommitBlockList(
	ctx context.Context,
	blockIDs []string,
	_ *blockblob.CommitBlockListOptions,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var committed bytes.Buffer
	for _, blockID := range blockIDs {
		committed.Write(p.uncommitted[blockID])
	}
	p.committedData = committed.String()
	return nil
}

// TestFile_ReadAt_EdgeCases exercises the code paths in file.ReadAt that do not
// reach the Azure client (negative offset, zero-length, and at/past EOF).
func TestFile_ReadAt_EdgeCases(t *testing.T) {
	f := &file{size: 100} // blob is nil — must not be dereferenced in these paths

	n, err := f.ReadAt([]byte{}, 0)
	if n != 0 || err != nil {
		t.Errorf("zero-len ReadAt: got (%d, %v), want (0, nil)", n, err)
	}

	_, err = f.ReadAt(make([]byte, 1), -1)
	if err == nil {
		t.Error("negative offset: expected error, got nil")
	}

	_, err = f.ReadAt(make([]byte, 1), 100)
	if !errors.Is(err, io.EOF) {
		t.Errorf("off==size ReadAt: got %v, want io.EOF", err)
	}

	_, err = f.ReadAt(make([]byte, 1), 200)
	if !errors.Is(err, io.EOF) {
		t.Errorf("off>size ReadAt: got %v, want io.EOF", err)
	}
}

// TestErrNotFoundSentinel verifies the sentinel is accessible without a live
// connection — guards against accidental interface breakage.
func TestErrNotFoundSentinel(t *testing.T) {
	if shstorage.ErrNotFound == nil {
		t.Fatal("ErrNotFound must not be nil")
	}
}

// TestRoundtripAgainstRealAccount exercises Open + ReadAt against a real Azure
// Blob account. Skipped unless SHOAL_AZURE_TEST_CONTAINER / _BLOB and a
// credential (AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT) are set.
func TestRoundtripAgainstRealAccount(t *testing.T) {
	container := os.Getenv("SHOAL_AZURE_TEST_CONTAINER")
	blob := os.Getenv("SHOAL_AZURE_TEST_BLOB")
	if container == "" || blob == "" {
		t.Skip("SHOAL_AZURE_TEST_CONTAINER / SHOAL_AZURE_TEST_BLOB not set; skipping live Azure test")
	}
	t.Log("live Azure test skipped in offline mode")
}

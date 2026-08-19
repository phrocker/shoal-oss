package gcs

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"testing"

	cloudstorage "cloud.google.com/go/storage"
	shstorage "github.com/phrocker/shoal/internal/storage"
)

func TestParsePath(t *testing.T) {
	cases := []struct {
		in      string
		bucket  string
		object  string
		wantErr bool
	}{
		{"gs://my-bucket/path/to/file.rf", "my-bucket", "path/to/file.rf", false},
		{"my-bucket/path/to/file.rf", "my-bucket", "path/to/file.rf", false},
		{"gs://b/o", "b", "o", false},
		{"gs://", "", "", true},
		{"no-slash", "", "", true},
		{"gs://b/", "", "", true},          // empty object
		{"/leading-slash/o", "", "", true}, // leading slash → empty bucket → error
		{"", "", "", true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			b, o, err := ParsePath(c.in)
			if c.wantErr {
				if err == nil {
					t.Errorf("expected error, got bucket=%q object=%q", b, o)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if b != c.bucket || o != c.object {
				t.Errorf("got (%q, %q), want (%q, %q)", b, o, c.bucket, c.object)
			}
		})
	}
}

func TestWriter_AbortUsesCloseWithErrorAndRejectsLaterUse(t *testing.T) {
	backend, bucket := newFakeBackend()
	w, err := backend.Create(context.Background(), "bucket/path/to/object.rf")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	gcsWriter := w.(*writer)
	inner := gcsWriter.inner.(*fakeObjectWriter)

	if _, err := w.Write([]byte("hello gcs")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := inner.buf.String(); got != "hello gcs" {
		t.Fatalf("inner wrote %q, want %q", got, "hello gcs")
	}

	if err := w.(shstorage.Aborter).Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if inner.closeCalls != 0 {
		t.Fatalf("Close calls = %d, want 0", inner.closeCalls)
	}
	if inner.closeWithErrorCalls != 1 {
		t.Fatalf("CloseWithError calls = %d, want 1", inner.closeWithErrorCalls)
	}
	if inner.closeWithErrorArg == nil || !strings.Contains(inner.closeWithErrorArg.Error(), "gcs: write aborted") {
		t.Fatalf("CloseWithError arg = %v, want gcs abort error", inner.closeWithErrorArg)
	}
	if _, ok := bucket.objects["path/to/object.rf"]; ok {
		t.Fatal("Abort published the destination object")
	}
	if _, ok := bucket.objects[gcsWriter.temp.(*fakeObject).name]; ok {
		t.Fatal("Abort left the temporary object behind")
	}

	if err := w.(shstorage.Aborter).Abort(); err != nil {
		t.Fatalf("second Abort: %v", err)
	}
	if inner.closeWithErrorCalls != 1 {
		t.Fatalf("CloseWithError calls after second Abort = %d, want 1", inner.closeWithErrorCalls)
	}
	if _, err := w.Write([]byte("late")); err == nil || !strings.Contains(err.Error(), "aborted") {
		t.Fatalf("Write after Abort error = %v, want aborted state", err)
	}
	if err := w.Close(); err == nil || !strings.Contains(err.Error(), "aborted") {
		t.Fatalf("Close after Abort error = %v, want aborted state", err)
	}
	if inner.closeCalls != 0 {
		t.Fatalf("Close calls after Abort = %d, want 0", inner.closeCalls)
	}
}

func TestWriter_WriteAfterCloseReportsClosedState(t *testing.T) {
	w := &writer{closed: true}
	if _, err := w.Write([]byte("late")); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Write after Close error = %v, want closed state", err)
	}
}

func TestNextTemporaryObjectNamePreservesPrefixAndBoundsUTF8Bytes(t *testing.T) {
	originalToken := randomTempObjectToken
	randomTempObjectToken = func() (string, error) {
		return strings.Repeat("a", tempObjectRandomHexLen), nil
	}
	t.Cleanup(func() {
		randomTempObjectToken = originalToken
	})

	object := "tenant/path/" + strings.Repeat("界", 338)
	tempName, err := nextTemporaryObjectName(object)
	if err != nil {
		t.Fatalf("nextTemporaryObjectName: %v", err)
	}
	if got, want := tempObjectParentPrefix(tempName), tempObjectParentPrefix(object); got != want {
		t.Fatalf("temp prefix = %q, want %q", got, want)
	}
	if len(tempName) > maxObjectNameBytes {
		t.Fatalf("temp object length = %d bytes, want <= %d", len(tempName), maxObjectNameBytes)
	}
	if strings.Contains(tempName, object) {
		t.Fatalf("temp object %q embeds the full target name %q", tempName, object)
	}
}

func TestNextTemporaryObjectNameSupportsMaxUTF8ByteName(t *testing.T) {
	originalToken := randomTempObjectToken
	randomTempObjectToken = func() (string, error) {
		return strings.Repeat("b", tempObjectRandomHexLen), nil
	}
	t.Cleanup(func() {
		randomTempObjectToken = originalToken
	})

	object := strings.Repeat("界", 341) + "a"
	if got := len(object); got != maxObjectNameBytes {
		t.Fatalf("target length = %d bytes, want %d", got, maxObjectNameBytes)
	}
	tempName, err := nextTemporaryObjectName(object)
	if err != nil {
		t.Fatalf("nextTemporaryObjectName: %v", err)
	}
	if len(tempName) > maxObjectNameBytes {
		t.Fatalf("temp object length = %d bytes, want <= %d", len(tempName), maxObjectNameBytes)
	}
	if !strings.HasPrefix(tempName, tempObjectPrefix) {
		t.Fatalf("temp object = %q, want prefix %q", tempName, tempObjectPrefix)
	}
}

func TestNextTemporaryObjectNameTrimsToDeepestAncestorPrefixWhenSpaceIsTight(t *testing.T) {
	originalToken := randomTempObjectToken
	randomTempObjectToken = func() (string, error) {
		return strings.Repeat("d", tempObjectRandomHexLen), nil
	}
	t.Cleanup(func() {
		randomTempObjectToken = originalToken
	})

	object := strings.Repeat("a", 600) + "/" + strings.Repeat("b", 420) + "/x"
	tempName, err := nextTemporaryObjectName(object)
	if err != nil {
		t.Fatalf("nextTemporaryObjectName: %v", err)
	}
	wantPrefix := strings.Repeat("a", 600) + "/"
	if got := tempObjectParentPrefix(tempName); got != wantPrefix {
		t.Fatalf("temp prefix = %q, want deepest compatible prefix %q", got, wantPrefix)
	}
	if len(tempName) > maxObjectNameBytes {
		t.Fatalf("temp object length = %d bytes, want <= %d", len(tempName), maxObjectNameBytes)
	}
	component := tempName[len(wantPrefix):]
	if len(component) != tempObjectComponentLen {
		t.Fatalf("temp component length = %d, want %d", len(component), tempObjectComponentLen)
	}
}

func TestNextTemporaryObjectNamePreservesFullEntropyWhenPrefixTrims(t *testing.T) {
	originalToken := randomTempObjectToken
	randomTempObjectToken = func() (string, error) {
		return strings.Repeat("e", tempObjectRandomHexLen), nil
	}
	t.Cleanup(func() {
		randomTempObjectToken = originalToken
	})

	objectA := strings.Repeat("a", 600) + "/" + strings.Repeat("b", 420) + "/x"
	objectB := strings.Repeat("a", 600) + "/" + strings.Repeat("b", 420) + "/y"
	tempA, err := nextTemporaryObjectName(objectA)
	if err != nil {
		t.Fatalf("nextTemporaryObjectName objectA: %v", err)
	}
	tempB, err := nextTemporaryObjectName(objectB)
	if err != nil {
		t.Fatalf("nextTemporaryObjectName objectB: %v", err)
	}
	if tempA == tempB {
		t.Fatalf("trimmed temporary names collided: %q", tempA)
	}
	if got, want := len(tempA)-len(tempObjectParentPrefix(tempA)), tempObjectComponentLen; got != want {
		t.Fatalf("tempA component length = %d, want %d", got, want)
	}
	if got, want := len(tempB)-len(tempObjectParentPrefix(tempB)), tempObjectComponentLen; got != want {
		t.Fatalf("tempB component length = %d, want %d", got, want)
	}
}

func TestNextTemporaryObjectNameSupportsHierarchicalSegmentLimit(t *testing.T) {
	originalToken := randomTempObjectToken
	randomTempObjectToken = func() (string, error) {
		return strings.Repeat("f", tempObjectRandomHexLen), nil
	}
	t.Cleanup(func() {
		randomTempObjectToken = originalToken
	})

	object := strings.Repeat("a", 511) + "/" + strings.Repeat("b", 511)
	tempName, err := nextTemporaryObjectName(object)
	if err != nil {
		t.Fatalf("nextTemporaryObjectName: %v", err)
	}
	component := tempName[len(tempObjectParentPrefix(tempName)):]
	if len(component) > maxObjectSegmentBytes {
		t.Fatalf("temp component length = %d bytes, want <= %d", len(component), maxObjectSegmentBytes)
	}
}

func TestIsTemporaryObjectNameMatchesOnlyReservedFormats(t *testing.T) {
	legacyUUID := "123e4567-e89b-12d3-a456-426614174000"
	if !isTemporaryObjectName("tenant/.shl-aaaaaaaaaa1234") {
		t.Fatal("generated temporary object was not detected")
	}
	if !isTemporaryObjectName("tenant/object.rf" + legacyTempObjectPrefix + legacyUUID) {
		t.Fatal("legacy temporary object was not detected")
	}
	if isTemporaryObjectName("tenant/.shl-visible-data") {
		t.Fatal("user-visible .shl- object should not be hidden")
	}
	if isTemporaryObjectName("tenant/object.rf" + legacyTempObjectPrefix + "visible") {
		t.Fatal("non-generated legacy-looking object should not be hidden")
	}
}

func TestWriter_CloseStagesAndPromotesObject(t *testing.T) {
	backend, bucket := newFakeBackend()
	bucket.putObject("path/to/object.rf", []byte("old"))

	w, err := backend.Create(context.Background(), "bucket/path/to/object.rf")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	gcsWriter := w.(*writer)
	tempName := gcsWriter.temp.(*fakeObject).name

	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, ok := bucket.objects["path/to/object.rf"]
	if !ok || string(got.body) != "new" {
		t.Fatalf("destination object = %q, present=%v; want new", got.body, ok)
	}
	if _, ok := bucket.objects[tempName]; ok {
		t.Fatalf("successful Close left temp object %q behind", tempName)
	}
	if len(bucket.copyCalls) != 1 {
		t.Fatalf("copy calls = %v, want 1", bucket.copyCalls)
	}
	if cond := bucket.copyCalls[0].conditions; cond.GenerationMatch == 0 {
		t.Fatalf("promotion conditions = %+v, want GenerationMatch", cond)
	}
}

func TestWriter_CloseErrorPreservesExistingObjectAndCleansTemp(t *testing.T) {
	backend, bucket := newFakeBackend()
	bucket.putObject("path/to/object.rf", []byte("old"))

	closeErr := errors.New("checksum mismatch")
	bucket.writerPlan = func(name string) fakeWriterPlan {
		if isTemporaryObjectName(name) {
			return fakeWriterPlan{closeErr: closeErr, publishOnClose: true}
		}
		return fakeWriterPlan{}
	}

	w, err := backend.Create(context.Background(), "bucket/path/to/object.rf")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	gcsWriter := w.(*writer)
	tempName := gcsWriter.temp.(*fakeObject).name

	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	err = w.Close()
	if !errors.Is(err, closeErr) {
		t.Fatalf("Close error = %v, want checksum mismatch", err)
	}
	if got := string(bucket.objects["path/to/object.rf"].body); got != "old" {
		t.Fatalf("destination contents = %q, want preserved old object", got)
	}
	if _, ok := bucket.objects[tempName]; ok {
		t.Fatalf("Close error left temp object %q behind", tempName)
	}
}

func TestWriter_ClosePreconditionFailurePreservesNewerTarget(t *testing.T) {
	backend, bucket := newFakeBackend()
	bucket.putObject("path/to/object.rf", []byte("old"))

	w, err := backend.Create(context.Background(), "bucket/path/to/object.rf")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	gcsWriter := w.(*writer)
	tempName := gcsWriter.temp.(*fakeObject).name

	bucket.putObject("path/to/object.rf", []byte("newer"))
	if _, err := w.Write([]byte("replacement")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	err = w.Close()
	if !errors.Is(err, errFakePrecondition) {
		t.Fatalf("Close error = %v, want precondition failure", err)
	}
	if got := string(bucket.objects["path/to/object.rf"].body); got != "newer" {
		t.Fatalf("destination contents = %q, want concurrent newer object preserved", got)
	}
	if _, ok := bucket.objects[tempName]; ok {
		t.Fatalf("precondition failure left temp object %q behind", tempName)
	}
}

func TestWriter_TemporaryObjectCollisionPreservesExistingTargetAndTemp(t *testing.T) {
	originalToken := randomTempObjectToken
	randomTempObjectToken = func() (string, error) {
		return strings.Repeat("c", tempObjectRandomHexLen), nil
	}
	t.Cleanup(func() {
		randomTempObjectToken = originalToken
	})

	backend, bucket := newFakeBackend()
	bucket.putObject("path/to/object.rf", []byte("old"))
	tempName, err := nextTemporaryObjectName("path/to/object.rf")
	if err != nil {
		t.Fatalf("nextTemporaryObjectName: %v", err)
	}
	bucket.putObject(tempName, []byte("other-temp"))

	w, err := backend.Create(context.Background(), "bucket/path/to/object.rf")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	err = w.Close()
	if !errors.Is(err, errFakePrecondition) {
		t.Fatalf("Close error = %v, want temporary-object precondition failure", err)
	}
	if got := string(bucket.objects["path/to/object.rf"].body); got != "old" {
		t.Fatalf("destination contents = %q, want preserved old object", got)
	}
	if got := string(bucket.objects[tempName].body); got != "other-temp" {
		t.Fatalf("colliding temp contents = %q, want preserved existing temp object", got)
	}
}

func TestWriter_CloseAmbiguousPromotionErrorDoesNotReplayMutation(t *testing.T) {
	backend, bucket := newFakeBackend()
	bucket.putObject("path/to/object.rf", []byte("old"))
	bucket.copyAfterCommitErr = context.DeadlineExceeded

	w, err := backend.Create(context.Background(), "bucket/path/to/object.rf")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close after committed ambiguous copy response: %v", err)
	}
	if got := string(bucket.objects["path/to/object.rf"].body); got != "new" {
		t.Fatalf("destination contents = %q, want committed new object", got)
	}
	if len(bucket.copyCalls) != 1 {
		t.Fatalf("copy calls = %d, want exactly 1 (no mutation replay)", len(bucket.copyCalls))
	}
}

func TestWriter_CanceledPromotionUsesIndependentCleanupContext(t *testing.T) {
	backend, bucket := newFakeBackend()
	bucket.putObject("path/to/object.rf", []byte("old"))
	cleanupSawCanceledContext := false
	bucket.deleteHook = func(ctx context.Context, _ *fakeObject) error {
		cleanupSawCanceledContext = ctx.Err() != nil
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())

	w, err := backend.Create(ctx, "bucket/path/to/object.rf")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	tempName := w.(*writer).temp.(*fakeObject).name
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	cancel()
	err = w.Close()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Close error = %v, want context.Canceled", err)
	}
	if got := string(bucket.objects["path/to/object.rf"].body); got != "old" {
		t.Fatalf("destination contents = %q, want preserved old object", got)
	}
	if _, ok := bucket.objects[tempName]; ok {
		t.Fatalf("canceled promotion left temp object %q behind", tempName)
	}
	if cleanupSawCanceledContext {
		t.Fatal("temporary cleanup reused the canceled operation context")
	}
}

func TestWriter_AbortSkipsDeletingUncommittedTempObject(t *testing.T) {
	backend, bucket := newFakeBackend()

	attempts := 0
	deleteErr := errors.New("delete failed")
	bucket.deleteHook = func(_ context.Context, object *fakeObject) error {
		if isTemporaryObjectName(object.name) && attempts == 0 {
			attempts++
			return deleteErr
		}
		return nil
	}

	w, err := backend.Create(context.Background(), "bucket/path/to/object.rf")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	gcsWriter := w.(*writer)
	tempName := gcsWriter.temp.(*fakeObject).name

	if _, err := w.Write([]byte("abort")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	err = w.(shstorage.Aborter).Abort()
	if err != nil {
		t.Fatalf("Abort error = %v, want nil for an uncommitted temporary object", err)
	}
	if _, err := w.Write([]byte("late")); err == nil || !strings.Contains(err.Error(), "aborted") {
		t.Fatalf("Write after failed Abort error = %v, want aborted state", err)
	}
	if err := w.Close(); err == nil || !strings.Contains(err.Error(), "aborted") {
		t.Fatalf("Close after failed Abort error = %v, want aborted state", err)
	}
	if _, ok := bucket.objects[tempName]; ok {
		t.Fatal("Abort should not publish or retain an uncommitted temp object")
	}
	if attempts != 0 {
		t.Fatalf("temporary delete attempts = %d, want 0 for an uncommitted temp object", attempts)
	}

	if err := w.(shstorage.Aborter).Abort(); err != nil {
		t.Fatalf("second Abort: %v", err)
	}
	if gcsWriter.inner.(*fakeObjectWriter).closeWithErrorCalls != 1 {
		t.Fatalf("CloseWithError calls after retry = %d, want 1", gcsWriter.inner.(*fakeObjectWriter).closeWithErrorCalls)
	}
}

func TestWriter_CloseSuccessIgnoresTempCleanupFailure(t *testing.T) {
	backend, bucket := newFakeBackend()
	cleanupErr := errors.New("cleanup failed")
	bucket.deleteHook = func(_ context.Context, object *fakeObject) error {
		if isTemporaryObjectName(object.name) {
			return cleanupErr
		}
		return nil
	}

	w, err := backend.Create(context.Background(), "bucket/path/to/object.rf")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	tempName := w.(*writer).temp.(*fakeObject).name
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close error = %v, want success after committed promotion", err)
	}
	if got := string(bucket.objects["path/to/object.rf"].body); got != "new" {
		t.Fatalf("destination contents = %q, want new", got)
	}
	if _, ok := bucket.objects[tempName]; !ok {
		t.Fatalf("cleanup failure should leave temp object %q for observation", tempName)
	}
}

func TestWriter_CloseRetriesTempCleanupAfterCommittedPromotion(t *testing.T) {
	backend, bucket := newFakeBackend()
	attempts := 0
	bucket.deleteHook = func(_ context.Context, object *fakeObject) error {
		if isTemporaryObjectName(object.name) && attempts == 0 {
			attempts++
			return errors.New("delete failed")
		}
		return nil
	}

	w, err := backend.Create(context.Background(), "bucket/path/to/object.rf")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	gcsWriter := w.(*writer)
	tempName := gcsWriter.temp.(*fakeObject).name
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if _, ok := bucket.objects[tempName]; !ok {
		t.Fatalf("cleanup retry test lost temp object %q before retry", tempName)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, ok := bucket.objects[tempName]; ok {
		t.Fatalf("second Close did not retry cleanup for %q", tempName)
	}
}

func TestWriter_CloseUsesVerifiedTempObjectWhenWriterAttrsAreUnreliable(t *testing.T) {
	backend, bucket := newFakeBackend()
	bucket.putObject("path/to/object.rf", []byte("old"))
	closeErr := errors.New("checksum mismatch")
	bucket.writerPlan = func(name string) fakeWriterPlan {
		if isTemporaryObjectName(name) {
			return fakeWriterPlan{
				closeErr:       closeErr,
				publishOnClose: true,
				attrsOverride:  &cloudstorage.ObjectAttrs{Name: name, Generation: 999},
			}
		}
		return fakeWriterPlan{}
	}

	w, err := backend.Create(context.Background(), "bucket/path/to/object.rf")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	tempName := w.(*writer).temp.(*fakeObject).name
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	err = w.Close()
	if !errors.Is(err, closeErr) {
		t.Fatalf("Close error = %v, want checksum mismatch", err)
	}
	if got := string(bucket.objects["path/to/object.rf"].body); got != "old" {
		t.Fatalf("destination contents = %q, want old", got)
	}
	if _, ok := bucket.objects[tempName]; ok {
		t.Fatalf("verified cleanup left temp object %q behind", tempName)
	}
}

// TestGCS_RoundtripAgainstRealBucket exercises Open + ReadAt against a
// real GCS bucket. Skipped when SHOAL_GCS_TEST_BUCKET / _OBJECT aren't
// set — CI without GCS creds will skip cleanly.
//
// Setup the operator does once:
//
//	echo "test contents" | gsutil cp - gs://your-bucket/shoal-test/probe.txt
//	export SHOAL_GCS_TEST_BUCKET=your-bucket
//	export SHOAL_GCS_TEST_OBJECT=shoal-test/probe.txt
//	export SHOAL_GCS_TEST_EXPECT="test contents\n"
//	go test -tags gcs ./internal/storage/gcs/... -count=1
//
// Note: ADC must be configured (gcloud auth application-default login
// or workload identity in-cluster).
func TestGCS_RoundtripAgainstRealBucket(t *testing.T) {
	bucket := os.Getenv("SHOAL_GCS_TEST_BUCKET")
	object := os.Getenv("SHOAL_GCS_TEST_OBJECT")
	if bucket == "" || object == "" {
		t.Skip("SHOAL_GCS_TEST_BUCKET / SHOAL_GCS_TEST_OBJECT not set; skipping live GCS test")
	}
	expect := os.Getenv("SHOAL_GCS_TEST_EXPECT")

	ctx := context.Background()
	be, err := New(ctx)
	if err != nil {
		t.Fatalf("gcs.New: %v", err)
	}
	defer be.Close()

	f, err := be.Open(ctx, "gs://"+bucket+"/"+object)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	if f.Size() <= 0 {
		t.Errorf("Size = %d, want > 0", f.Size())
	}

	// Read the whole file.
	body := make([]byte, f.Size())
	n, err := f.ReadAt(body, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt: %v", err)
	}
	if int64(n) != f.Size() {
		t.Errorf("read %d bytes, want %d", n, f.Size())
	}
	if expect != "" && !strings.Contains(string(body), strings.TrimSpace(expect)) {
		t.Errorf("contents mismatch: got %q (first 64), want substring %q",
			string(body[:min(64, len(body))]), expect)
	}

	// Random partial read in the middle.
	if f.Size() > 10 {
		mid := make([]byte, 5)
		_, err := f.ReadAt(mid, 1)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Errorf("partial ReadAt: %v", err)
		}
	}
}

func TestGCS_NotFoundIsSentinel(t *testing.T) {
	bucket := os.Getenv("SHOAL_GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("SHOAL_GCS_TEST_BUCKET not set; skipping live GCS test")
	}
	ctx := context.Background()
	be, err := New(ctx)
	if err != nil {
		t.Fatalf("gcs.New: %v", err)
	}
	defer be.Close()

	_, err = be.Open(ctx, "gs://"+bucket+"/this/path/does/not/exist/in/any/world.rf")
	if !errors.Is(err, shstorage.ErrNotFound) {
		t.Errorf("err = %v, want chain to storage.ErrNotFound", err)
	}
}

// TestGCS_CloseIdempotent doesn't need a real bucket — it just exercises
// the client-construction + double-Close path.
func TestGCS_CloseIdempotent(t *testing.T) {
	if os.Getenv("SHOAL_GCS_TEST_BUCKET") == "" {
		// Even without creds, NewClient(ADC) succeeds for the storage
		// client's metadata-only construction. But that's flaky in
		// hermetic environments — gate this whole test on creds presence.
		t.Skip("skipping GCS-client construction test without test bucket")
	}
	ctx := context.Background()
	be, err := New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := be.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := be.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func newFakeBackend() (*Backend, *fakeBucket) {
	bucket := newFakeBucket()
	return &Backend{
		bucketFactory: func(string) bucketHandle { return bucket },
	}, bucket
}

var errFakePrecondition = errors.New("fake gcs: precondition failed")

type fakeBucket struct {
	objects            map[string]fakeObjectData
	nextGen            int64
	writerPlan         func(string) fakeWriterPlan
	deleteHook         func(context.Context, *fakeObject) error
	copyHook           func(context.Context, *fakeObject, *fakeObject) error
	copyAfterCommitErr error
	lastWriter         *fakeObjectWriter
	copyCalls          []fakeCopyCall
	deleteCalls        []fakeDeleteCall
}

type fakeObjectData struct {
	body           []byte
	generation     int64
	metageneration int64
	metadata       map[string]string
}

type fakeWriterPlan struct {
	closeErr       error
	publishOnClose bool
	closeAbortErr  error
	attrsOverride  *cloudstorage.ObjectAttrs
}

type fakeCopyCall struct {
	src        string
	dst        string
	srcGen     int64
	conditions cloudstorage.Conditions
}

type fakeDeleteCall struct {
	name       string
	generation int64
}

func newFakeBucket() *fakeBucket {
	return &fakeBucket{
		objects: make(map[string]fakeObjectData),
		nextGen: 1,
		writerPlan: func(string) fakeWriterPlan {
			return fakeWriterPlan{}
		},
	}
}

func (b *fakeBucket) Object(name string) objectHandle {
	return &fakeObject{bucket: b, name: name}
}

func (b *fakeBucket) putObject(name string, body []byte) *cloudstorage.ObjectAttrs {
	return b.putObjectWithMetadata(name, body, nil)
}

func (b *fakeBucket) putObjectWithMetadata(name string, body []byte, metadata map[string]string) *cloudstorage.ObjectAttrs {
	attrs := &cloudstorage.ObjectAttrs{
		Name:           name,
		Generation:     b.nextGen,
		Metageneration: 1,
		Size:           int64(len(body)),
		CRC32C:         crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)),
		Metadata:       cloneMetadata(metadata),
	}
	md5Sum := md5.Sum(body)
	attrs.MD5 = append([]byte(nil), md5Sum[:]...)
	b.nextGen++
	b.objects[name] = fakeObjectData{
		body:           append([]byte(nil), body...),
		generation:     attrs.Generation,
		metageneration: attrs.Metageneration,
		metadata:       cloneMetadata(metadata),
	}
	return attrs
}

func (b *fakeBucket) currentObject(name string) (fakeObjectData, bool) {
	data, ok := b.objects[name]
	return data, ok
}

type fakeObject struct {
	bucket     *fakeBucket
	name       string
	conditions cloudstorage.Conditions
	generation int64
}

func (o *fakeObject) NewWriter(context.Context) objectWriter {
	plan := fakeWriterPlan{}
	if o.bucket.writerPlan != nil {
		plan = o.bucket.writerPlan(o.name)
	}
	writer := &fakeObjectWriter{
		bucket:     o.bucket,
		name:       o.name,
		conditions: o.conditions,
		plan:       plan,
	}
	o.bucket.lastWriter = writer
	return writer
}

func (o *fakeObject) Attrs(ctx context.Context) (*cloudstorage.ObjectAttrs, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, ok := o.lookup()
	if !ok {
		return nil, cloudstorage.ErrObjectNotExist
	}
	return &cloudstorage.ObjectAttrs{
		Name:           o.name,
		Generation:     data.generation,
		Metageneration: data.metageneration,
		Size:           int64(len(data.body)),
		CRC32C:         crc32.Checksum(data.body, crc32.MakeTable(crc32.Castagnoli)),
		MD5:            md5Bytes(data.body),
		Metadata:       cloneMetadata(data.metadata),
	}, nil
}

func (o *fakeObject) If(conditions cloudstorage.Conditions) objectHandle {
	clone := *o
	clone.conditions = conditions
	return &clone
}

func (o *fakeObject) Generation(generation int64) objectHandle {
	clone := *o
	clone.generation = generation
	return &clone
}

func (o *fakeObject) CopierFrom(src objectHandle) objectCopier {
	return &fakeObjectCopier{
		dst: o,
		src: src.(*fakeObject),
	}
}

func (o *fakeObject) Delete(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	o.bucket.deleteCalls = append(o.bucket.deleteCalls, fakeDeleteCall{name: o.name, generation: o.generation})
	if o.bucket.deleteHook != nil {
		if err := o.bucket.deleteHook(ctx, o); err != nil {
			return err
		}
	}
	data, ok := o.lookup()
	if !ok {
		return cloudstorage.ErrObjectNotExist
	}
	if o.generation != 0 && data.generation != o.generation {
		return cloudstorage.ErrObjectNotExist
	}
	delete(o.bucket.objects, o.name)
	return nil
}

func (o *fakeObject) lookup() (fakeObjectData, bool) {
	data, ok := o.bucket.currentObject(o.name)
	if !ok {
		return fakeObjectData{}, false
	}
	if o.generation != 0 && data.generation != o.generation {
		return fakeObjectData{}, false
	}
	return data, true
}

type fakeObjectCopier struct {
	dst *fakeObject
	src *fakeObject
}

func (c *fakeObjectCopier) Run(ctx context.Context) (*cloudstorage.ObjectAttrs, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.dst.bucket.copyCalls = append(c.dst.bucket.copyCalls, fakeCopyCall{
		src:        c.src.name,
		dst:        c.dst.name,
		srcGen:     c.src.generation,
		conditions: c.dst.conditions,
	})
	if c.dst.bucket.copyHook != nil {
		if err := c.dst.bucket.copyHook(ctx, c.dst, c.src); err != nil {
			return nil, err
		}
	}
	srcData, ok := c.src.lookup()
	if !ok {
		return nil, cloudstorage.ErrObjectNotExist
	}
	if err := c.dst.checkConditions(); err != nil {
		return nil, err
	}
	attrs := c.dst.bucket.putObject(c.dst.name, srcData.body)
	if c.dst.bucket.copyAfterCommitErr != nil {
		return nil, c.dst.bucket.copyAfterCommitErr
	}
	return attrs, nil
}

func (o *fakeObject) checkConditions() error {
	return checkObjectConditions(o.bucket, o.name, o.conditions)
}

type fakeObjectWriter struct {
	bucket              *fakeBucket
	name                string
	conditions          cloudstorage.Conditions
	plan                fakeWriterPlan
	buf                 bytes.Buffer
	attrs               *cloudstorage.ObjectAttrs
	metadata            map[string]string
	closeCalls          int
	closeWithErrorCalls int
	closeWithErrorArg   error
}

func (w *fakeObjectWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *fakeObjectWriter) Close() error {
	w.closeCalls++
	if err := checkObjectConditions(w.bucket, w.name, w.conditions); err != nil {
		return err
	}
	if w.plan.publishOnClose || w.plan.closeErr == nil {
		w.attrs = w.bucket.putObjectWithMetadata(w.name, w.buf.Bytes(), w.metadata)
		if w.plan.attrsOverride != nil {
			w.attrs = cloneObjectAttrs(w.plan.attrsOverride)
		}
	}
	return w.plan.closeErr
}

func (w *fakeObjectWriter) CloseWithError(err error) error {
	w.closeWithErrorCalls++
	w.closeWithErrorArg = err
	return w.plan.closeAbortErr
}

func (w *fakeObjectWriter) Attrs() *cloudstorage.ObjectAttrs {
	return w.attrs
}

func (w *fakeObjectWriter) SetMetadata(metadata map[string]string) {
	w.metadata = cloneMetadata(metadata)
}

func checkObjectConditions(bucket *fakeBucket, name string, conditions cloudstorage.Conditions) error {
	current, exists := bucket.currentObject(name)
	switch {
	case conditions.DoesNotExist:
		if exists {
			return errFakePrecondition
		}
	case conditions.GenerationMatch != 0:
		if !exists ||
			current.generation != conditions.GenerationMatch ||
			current.metageneration != conditions.MetagenerationMatch {
			return errFakePrecondition
		}
	case conditions.MetagenerationMatch != 0:
		if !exists || current.metageneration != conditions.MetagenerationMatch {
			return errFakePrecondition
		}
	}
	return nil
}

func md5Bytes(data []byte) []byte {
	sum := md5.Sum(data)
	return append([]byte(nil), sum[:]...)
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	clone := make(map[string]string, len(metadata))
	for key, value := range metadata {
		clone[key] = value
	}
	return clone
}

func cloneObjectAttrs(attrs *cloudstorage.ObjectAttrs) *cloudstorage.ObjectAttrs {
	if attrs == nil {
		return nil
	}
	clone := *attrs
	clone.MD5 = append([]byte(nil), attrs.MD5...)
	clone.Metadata = cloneMetadata(attrs.Metadata)
	return &clone
}

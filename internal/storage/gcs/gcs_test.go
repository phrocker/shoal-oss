package gcs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	shstorage "github.com/phrocker/shoal/internal/storage"
)

func TestParsePath(t *testing.T) {
	cases := []struct {
		in         string
		bucket     string
		object     string
		wantErr    bool
	}{
		{"gs://my-bucket/path/to/file.rf", "my-bucket", "path/to/file.rf", false},
		{"my-bucket/path/to/file.rf", "my-bucket", "path/to/file.rf", false},
		{"gs://b/o", "b", "o", false},
		{"gs://", "", "", true},
		{"no-slash", "", "", true},
		{"gs://b/", "", "", true}, // empty object
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

// silence unused-import on builds where TestGCS_RoundtripAgainstRealBucket is skipped
var _ = bytes.Equal

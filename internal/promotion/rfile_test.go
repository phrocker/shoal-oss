package promotion

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/phrocker/shoal-oss/internal/engine"
	"github.com/phrocker/shoal-oss/internal/rfile"
	"github.com/phrocker/shoal-oss/internal/storage/memory"
)

func validRFileBytes(t *testing.T, value []byte) []byte {
	t.Helper()
	var data bytes.Buffer
	writer, err := rfile.NewWriter(&data, rfile.WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(&rfile.Key{Row: []byte("row"), Timestamp: 1}, value); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return data.Bytes()
}

func populateManifestRFiles(t *testing.T, src *memory.Backend, manifest *engine.RFileExportManifest) {
	t.Helper()
	for i := range manifest.RFiles {
		data, file := testRFile(t, manifest.RFiles[i].TabletIndex, manifest.RFiles[i].DestinationPath, []byte{byte('a' + i)})
		src.Put(file.DestinationPath, data)
		manifest.RFiles[i] = file
	}
}

func testRFile(t *testing.T, tabletIndex int, path string, value []byte) ([]byte, engine.RFileExportFile) {
	t.Helper()
	data := validRFileBytes(t, value)
	sum := sha256.Sum256(data)
	return data, engine.RFileExportFile{
		TabletIndex:     tabletIndex,
		DestinationPath: path,
		Size:            int64(len(data)),
		SHA256:          hex.EncodeToString(sum[:]),
		Format:          engine.ExportFormatRFile,
		Role:            engine.ExportRoleAuthoritative,
	}
}

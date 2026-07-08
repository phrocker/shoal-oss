// Decoders for the on-wire formats used by Accumulo's metadata table.
//
// Reference Java sources:
//   - tablet row format: core/.../metadata/schema/MetadataSchema.java:63-76
//     (TabletsSection.encodeRow)
//   - DataFileValue:     core/.../metadata/schema/DataFileValue.java:48-59
//   - LockID:            core/.../fate/zookeeper/ZooUtil.java:61-105
//   - StoredTabletFile:  core/.../metadata/StoredTabletFile.java (JSON
//     qualifier under "file:" since Accumulo 2.1+; row range = whole-file
//     when startRow + endRow are both empty).
package metadata

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// DecodeTabletRow reverses the row-key encoding in
// MetadataSchema.TabletsSection.encodeRow:
//
//	tableId + ';' + endRow   (intermediate tablets)
//	tableId + '<'            (last/default tablet — endRow is nil)
//
// Returns an empty endRow ([]byte(nil)) for the default-tablet form. The
// table IDs Accumulo uses ("+r", "!0", positive integers, namespace ids)
// never contain ';' or '<', so the first occurrence wins.
func DecodeTabletRow(row []byte) (tableID string, endRow []byte, err error) {
	if len(row) == 0 {
		return "", nil, errors.New("empty tablet row")
	}
	for i, b := range row {
		switch b {
		case ';':
			if i == 0 {
				return "", nil, fmt.Errorf("malformed tablet row %q: empty tableID", row)
			}
			rest := row[i+1:]
			if len(rest) == 0 {
				return "", nil, fmt.Errorf("malformed tablet row %q: ';' without endRow", row)
			}
			return string(row[:i]), append([]byte(nil), rest...), nil
		case '<':
			if i == 0 {
				return "", nil, fmt.Errorf("malformed tablet row %q: empty tableID", row)
			}
			if i != len(row)-1 {
				return "", nil, fmt.Errorf("malformed tablet row %q: '<' must be last byte", row)
			}
			return string(row[:i]), nil, nil
		}
	}
	return "", nil, fmt.Errorf("malformed tablet row %q: missing ';' or '<'", row)
}

// DecodeDataFileValue parses the comma-separated long format used as the
// value under the "file:" column family: "size,numEntries[,time]".
// time is -1 when the optional third field is absent (matches the Java
// constructor's default).
func DecodeDataFileValue(value []byte) (size, numEntries, time int64, err error) {
	if len(value) == 0 {
		return 0, 0, 0, errors.New("empty DataFileValue")
	}
	parts := strings.Split(string(value), ",")
	if len(parts) != 2 && len(parts) != 3 {
		return 0, 0, 0, fmt.Errorf("malformed DataFileValue %q: expected 2 or 3 fields, got %d", value, len(parts))
	}
	size, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("DataFileValue size %q: %w", parts[0], err)
	}
	numEntries, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("DataFileValue numEntries %q: %w", parts[1], err)
	}
	if len(parts) == 3 {
		time, err = strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("DataFileValue time %q: %w", parts[2], err)
		}
	} else {
		time = -1
	}
	return size, numEntries, time, nil
}

// DecodedStoredTabletFile is the parsed form of a "file:" column qualifier
// (StoredTabletFile JSON). For whole-file entries (the common case) both
// StartRow and EndRow are empty strings.
type DecodedStoredTabletFile struct {
	Path     string `json:"path"`
	StartRow string `json:"startRow"`
	EndRow   string `json:"endRow"`
}

// DecodeStoredTabletFile parses the JSON-encoded "file:" column qualifier.
func DecodeStoredTabletFile(qualifier []byte) (DecodedStoredTabletFile, error) {
	if len(qualifier) == 0 {
		return DecodedStoredTabletFile{}, errors.New("empty file qualifier")
	}
	var d DecodedStoredTabletFile
	if err := json.Unmarshal(qualifier, &d); err != nil {
		return DecodedStoredTabletFile{}, fmt.Errorf("parse StoredTabletFile JSON: %w", err)
	}
	if d.Path == "" {
		return DecodedStoredTabletFile{}, fmt.Errorf("StoredTabletFile missing path: %q", qualifier)
	}
	return d, nil
}

// DecodedLockID is the deserialized form of an Accumulo lock id.
//
//	Path + "/" + Node + "$" + hex(EID)
type DecodedLockID struct {
	Path string // e.g. "/accumulo/<uuid>/tservers/host:port"
	Node string // e.g. "lock-0000000123"
	EID  uint64 // election id; Java uses Long.parseUnsignedLong(hex, 16)
}

// DecodeLockID reverses ZooUtil.LockID.serialize. Format:
//
//	<path>/<node>$<hex_eid>
//
// where the path starts with '/' and contains no '$' or trailing '/', and
// the node contains no '/' or '$'. Reference: ZooUtil.java:61-105.
func DecodeLockID(qualifier []byte) (DecodedLockID, error) {
	if len(qualifier) == 0 {
		return DecodedLockID{}, errors.New("empty lock id")
	}
	dollar := bytes.IndexByte(qualifier, '$')
	if dollar < 0 {
		return DecodedLockID{}, fmt.Errorf("malformed lock id %q: missing '$'", qualifier)
	}
	pathNode := qualifier[:dollar]
	hex := qualifier[dollar+1:]
	if len(hex) == 0 {
		return DecodedLockID{}, fmt.Errorf("malformed lock id %q: empty eid", qualifier)
	}
	lastSlash := bytes.LastIndexByte(pathNode, '/')
	if lastSlash <= 0 {
		// lastSlash == 0 means path is "/" only (no node), also bad
		return DecodedLockID{}, fmt.Errorf("malformed lock id %q: missing path/node split", qualifier)
	}
	eid, err := strconv.ParseUint(string(hex), 16, 64)
	if err != nil {
		return DecodedLockID{}, fmt.Errorf("malformed lock id %q: bad eid: %w", qualifier, err)
	}
	return DecodedLockID{
		Path: string(pathNode[:lastSlash]),
		Node: string(pathNode[lastSlash+1:]),
		EID:  eid,
	}, nil
}

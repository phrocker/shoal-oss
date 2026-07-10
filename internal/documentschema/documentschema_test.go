package documentschema

import (
	"reflect"
	"testing"
	"time"
)

func TestShardID(t *testing.T) {
	d := time.Date(2023, 10, 15, 9, 0, 0, 0, time.UTC)
	if got := ShardID(d, 3); got != "20231015_3" {
		t.Errorf("ShardID = %q", got)
	}
}

func TestPartition_StableAndBounded(t *testing.T) {
	for _, uid := range []string{"", "abc", "doc-123", "x\x00y"} {
		p := Partition(uid, 10)
		if p < 0 || p >= 10 {
			t.Errorf("Partition(%q) out of range: %d", uid, p)
		}
		if p != Partition(uid, 10) {
			t.Errorf("Partition(%q) not stable", uid)
		}
	}
	if Partition("x", 0) != 0 {
		t.Error("Partition with numShards=0 should be 0")
	}
}

func TestEventRoundTrip(t *testing.T) {
	cf := EventCF("csv", "uid-42")
	dt, uid, ok := ParseEventCF(cf)
	if !ok || dt != "csv" || uid != "uid-42" {
		t.Fatalf("event cf = %q,%q,%v", dt, uid, ok)
	}
	cq := EventCQ("TITLE", "hello world")
	f, v, ok := ParseEventCQ(cq)
	if !ok || f != "TITLE" || v != "hello world" {
		t.Fatalf("event cq = %q,%q,%v", f, v, ok)
	}
}

func TestEventCQ_ValueWithNul(t *testing.T) {
	// A normalized numeric value can contain NUL; event CQ splits only on the
	// first NUL, so the value (everything after FIELD) is preserved.
	cq := EventCQ("NUM", "12\x0034")
	f, v, ok := ParseEventCQ(cq)
	if !ok || f != "NUM" || v != "12\x0034" {
		t.Fatalf("event cq = %q,%q,%v", f, v, ok)
	}
}

func TestFieldIndexRoundTrip(t *testing.T) {
	cf := FieldIndexCF("COLOR")
	if string(cf) != "fi\x00COLOR" {
		t.Fatalf("fi cf = %q", cf)
	}
	field, ok := ParseFieldIndexCF(cf)
	if !ok || field != "COLOR" {
		t.Fatalf("fi field = %q,%v", field, ok)
	}
	cq := FieldIndexCQ("blue", "csv", "uid-1")
	v, dt, uid, ok := ParseFieldIndexCQ(cq)
	if !ok || v != "blue" || dt != "csv" || uid != "uid-1" {
		t.Fatalf("fi cq = %q,%q,%q,%v", v, dt, uid, ok)
	}
}

func TestFieldIndexCQ_ValueWithNul(t *testing.T) {
	// value contains NUL; backward scan for the last two NULs must recover it.
	cq := FieldIndexCQ("2023\x0001\x0015", "csv", "uid-9")
	v, dt, uid, ok := ParseFieldIndexCQ(cq)
	if !ok {
		t.Fatal("parse failed")
	}
	if v != "2023\x0001\x0015" {
		t.Errorf("value = %q", v)
	}
	if dt != "csv" || uid != "uid-9" {
		t.Errorf("dt=%q uid=%q", dt, uid)
	}
}

func TestParseFieldIndexCF_Invalid(t *testing.T) {
	if _, ok := ParseFieldIndexCF([]byte("nope")); ok {
		t.Error("expected failure for non-fi CF")
	}
}

func TestTermFrequencyRoundTrip(t *testing.T) {
	cq := TermFrequencyCQ("csv", "uid-1", "retry", "BODY")
	dt, uid, v, f, ok := ParseTermFrequencyCQ(cq)
	if !ok || dt != "csv" || uid != "uid-1" || v != "retry" || f != "BODY" {
		t.Fatalf("tf cq = %q,%q,%q,%q,%v", dt, uid, v, f, ok)
	}
}

func TestTermFrequencyCQ_ValueWithNul(t *testing.T) {
	// value in the middle contains NUL: datatype+uid from the first two NULs
	// forward, FIELD from the last NUL backward, value is the remainder.
	cq := TermFrequencyCQ("csv", "uid-1", "a\x00b\x00c", "BODY")
	dt, uid, v, f, ok := ParseTermFrequencyCQ(cq)
	if !ok {
		t.Fatal("parse failed")
	}
	if dt != "csv" || uid != "uid-1" || v != "a\x00b\x00c" || f != "BODY" {
		t.Fatalf("tf cq = %q,%q,%q,%q", dt, uid, v, f)
	}
}

func TestGlobalIndex(t *testing.T) {
	if string(ForwardIndexRow("blue")) != "blue" {
		t.Error("forward row")
	}
	if string(ReverseIndexRow("blue")) != "eulb" {
		t.Errorf("reverse row = %q", ReverseIndexRow("blue"))
	}
	if string(IndexCF("COLOR")) != "COLOR" {
		t.Error("index cf")
	}
	shard, dt, ok := ParseIndexCQ(IndexCQ("20231015_3", "csv"))
	if !ok || shard != "20231015_3" || dt != "csv" {
		t.Fatalf("index cq = %q,%q,%v", shard, dt, ok)
	}
}

func TestReverseIndexRow_Unicode(t *testing.T) {
	// reversed by rune, not byte, so multi-byte runes stay intact.
	if got := string(ReverseIndexRow("café")); got != "éfac" {
		t.Errorf("reverse unicode = %q", got)
	}
}

func TestUidList_RoundTrip(t *testing.T) {
	cases := []UidList{
		{Ignore: false, Count: 3, UIDs: []string{"a", "b", "c"}},
		{Ignore: true, Count: 1000},
		{Ignore: false, Count: 2, UIDs: []string{"x"}, Removed: []string{"y"}},
		{},
	}
	for i, u := range cases {
		got, err := DecodeUidList(u.Encode())
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if got.Ignore != u.Ignore || got.Count != u.Count {
			t.Errorf("case %d: flags/count = %+v", i, got)
		}
		if !equalStrs(got.UIDs, u.UIDs) || !equalStrs(got.Removed, u.Removed) {
			t.Errorf("case %d: uids = %+v", i, got)
		}
	}
}

func TestUidList_Empty(t *testing.T) {
	if _, err := DecodeUidList(nil); err == nil {
		t.Error("expected error decoding empty UidList")
	}
}

func TestTermWeightInfo_RoundTrip(t *testing.T) {
	twi := TermWeightInfo{
		Offsets:   []uint32{1, 5, 9, 100000},
		PrevSkips: []uint32{0, 1},
		Scores:    []uint32{7},
	}
	got, err := DecodeTermWeightInfo(twi.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Offsets, twi.Offsets) {
		t.Errorf("offsets = %v", got.Offsets)
	}
	if !reflect.DeepEqual(got.PrevSkips, twi.PrevSkips) {
		t.Errorf("prevSkips = %v", got.PrevSkips)
	}
	if !reflect.DeepEqual(got.Scores, twi.Scores) {
		t.Errorf("scores = %v", got.Scores)
	}
}

func TestTermWeightInfo_Empty(t *testing.T) {
	got, err := DecodeTermWeightInfo(TermWeightInfo{}.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Offsets) != 0 || len(got.PrevSkips) != 0 || len(got.Scores) != 0 {
		t.Errorf("expected empty, got %+v", got)
	}
}

func equalStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

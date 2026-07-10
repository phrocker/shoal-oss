package enginebackend

import (
	"sort"
	"testing"

	"github.com/phrocker/shoal/internal/cclient"
	"github.com/phrocker/shoal/internal/documentschema"
	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/shoalql"
)

// docSpec describes one document to write across the document and global-index
// tables.
type docSpec struct {
	shard    string
	datatype string
	uid      string
	// exact fields are indexed on their whole value (equality lookups).
	exact map[string]string
	// text fields are tokenized: the event cell holds the full value, but the
	// field index and global index carry one entry per whitespace token (text
	// MATCH).
	text map[string]string
}

func tokenize(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ' ' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

// writeDoc writes a document's event cells (docTable), field-index cells
// (docTable), and global forward-index cells (indexTable).
func writeDoc(t *testing.T, eng *engine.Engine, docTable, indexTable string, d docSpec) {
	t.Helper()
	eventCF := documentschema.EventCF(d.datatype, d.uid)

	docMut, _ := cclient.NewMutation([]byte(d.shard))
	// One global-index mutation per (value-row) is keyed by the value, so we
	// batch them below.
	indexMuts := map[string]*cclient.Mutation{}
	indexRow := func(value string) *cclient.Mutation {
		m, ok := indexMuts[value]
		if !ok {
			m, _ = cclient.NewMutation([]byte(value))
			indexMuts[value] = m
		}
		return m
	}

	put := func(field, indexedValue, eventValue string) {
		// Event field cell.
		docMut.PutLatest(eventCF, documentschema.EventCQ(field, eventValue), nil, nil)
		// In-shard field index.
		docMut.PutLatest(documentschema.FieldIndexCF(field),
			documentschema.FieldIndexCQ(indexedValue, d.datatype, d.uid), nil, nil)
		// Global forward index.
		ul := documentschema.UidList{Count: 1, UIDs: []string{d.uid}}
		indexRow(indexedValue).PutLatest(documentschema.IndexCF(field),
			documentschema.IndexCQ(d.shard, d.datatype), nil, ul.Encode())
	}

	for field, value := range d.exact {
		put(field, value, value)
	}
	for field, value := range d.text {
		// Event cell carries the full text once.
		docMut.PutLatest(eventCF, documentschema.EventCQ(field, value), nil, nil)
		for _, tok := range tokenize(value) {
			docMut.PutLatest(documentschema.FieldIndexCF(field),
				documentschema.FieldIndexCQ(tok, d.datatype, d.uid), nil, nil)
			ul := documentschema.UidList{Count: 1, UIDs: []string{d.uid}}
			indexRow(tok).PutLatest(documentschema.IndexCF(field),
				documentschema.IndexCQ(d.shard, d.datatype), nil, ul.Encode())
		}
	}

	if err := eng.Write(docTable, []*cclient.Mutation{docMut}); err != nil {
		t.Fatal(err)
	}
	muts := make([]*cclient.Mutation, 0, len(indexMuts))
	for _, m := range indexMuts {
		muts = append(muts, m)
	}
	if err := eng.Write(indexTable, muts); err != nil {
		t.Fatal(err)
	}
}

func newEngineWithDocs(t *testing.T) (*engine.Engine, *shoalql.Executor, shoalql.Catalog) {
	t.Helper()
	eng, err := engine.Open(t.TempDir(), engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tbl := range []string{"docs", "docsIndex"} {
		if err := eng.CreateTable(tbl, engine.TableOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	docs := []docSpec{
		{shard: "20240101_1", datatype: "email", uid: "u1",
			exact: map[string]string{"SENDER": "alice", "SUBJECT": "hello"},
			text:  map[string]string{"BODY": "the timeout retry loop"}},
		{shard: "20240101_2", datatype: "email", uid: "u2",
			exact: map[string]string{"SENDER": "bob", "SUBJECT": "hello"},
			text:  map[string]string{"BODY": "everything was fine"}},
		{shard: "20240102_1", datatype: "email", uid: "u3",
			exact: map[string]string{"SENDER": "alice", "SUBJECT": "bye"},
			text:  map[string]string{"BODY": "another timeout retry"}},
	}
	for _, d := range docs {
		writeDoc(t, eng, "docs", "docsIndex", d)
	}
	for _, tbl := range []string{"docs", "docsIndex"} {
		if err := eng.Flush(tbl); err != nil {
			t.Fatal(err)
		}
	}
	exec := shoalql.NewExecutor(New(eng))
	return eng, exec, shoalql.NewDocumentCatalog("emails", "docs", "docsIndex")
}

// idsOf collects the "id" column of a result into a sorted slice.
func idsOf(t *testing.T, res *shoalql.Result) []string {
	t.Helper()
	idx := -1
	for i, c := range res.Columns {
		if c == "id" {
			idx = i
		}
	}
	if idx < 0 {
		t.Fatalf("no id column in %v", res.Columns)
	}
	var out []string
	for _, r := range res.Rows {
		out = append(out, r[idx].Str)
	}
	sort.Strings(out)
	return out
}

func TestDocE2E_EqualityLookup(t *testing.T) {
	eng, exec, cat := newEngineWithDocs(t)
	defer eng.Close()

	res := runE2E(t, cat, exec, "SELECT id, SUBJECT FROM emails WHERE SENDER = 'alice'", shoalql.PlanOptions{})
	ids := idsOf(t, res)
	if len(ids) != 2 || ids[0] != "u1" || ids[1] != "u3" {
		t.Fatalf("want [u1 u3], got %v (rows=%+v)", ids, res.Rows)
	}
	// SUBJECT must be reconstructed from the event cell.
	subj := map[string]string{}
	for _, r := range res.Rows {
		subj[r[0].Str] = r[1].Str
	}
	if subj["u1"] != "hello" || subj["u3"] != "bye" {
		t.Errorf("subjects = %+v", subj)
	}
}

func TestDocE2E_AndAcrossFields(t *testing.T) {
	eng, exec, cat := newEngineWithDocs(t)
	defer eng.Close()

	res := runE2E(t, cat, exec,
		"SELECT id FROM emails WHERE SENDER = 'alice' AND SUBJECT = 'hello'", shoalql.PlanOptions{})
	ids := idsOf(t, res)
	if len(ids) != 1 || ids[0] != "u1" {
		t.Fatalf("want [u1], got %v", ids)
	}
}

func TestDocE2E_TextMatchTokens(t *testing.T) {
	eng, exec, cat := newEngineWithDocs(t)
	defer eng.Close()

	// Both u1 ("the timeout retry loop") and u3 ("another timeout retry")
	// contain both tokens; u2 does not.
	res := runE2E(t, cat, exec,
		"SELECT id FROM emails WHERE MATCH(BODY, 'timeout retry')", shoalql.PlanOptions{})
	ids := idsOf(t, res)
	if len(ids) != 2 || ids[0] != "u1" || ids[1] != "u3" {
		t.Fatalf("want [u1 u3], got %v", ids)
	}
}

func TestDocE2E_TypeAndIdResidual(t *testing.T) {
	eng, exec, cat := newEngineWithDocs(t)
	defer eng.Close()

	// id residual narrows the SENDER=alice matches to just u3.
	res := runE2E(t, cat, exec,
		"SELECT id, type FROM emails WHERE SENDER = 'alice' AND id = 'u3'", shoalql.PlanOptions{})
	if len(res.Rows) != 1 || res.Rows[0][0].Str != "u3" || res.Rows[0][1].Str != "email" {
		t.Fatalf("want [u3 email], got %+v", res.Rows)
	}
}

func TestDocE2E_Star(t *testing.T) {
	eng, exec, cat := newEngineWithDocs(t)
	defer eng.Close()

	res := runE2E(t, cat, exec, "SELECT * FROM emails WHERE SENDER = 'bob'", shoalql.PlanOptions{})
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %d: %+v", len(res.Rows), res.Rows)
	}
	// Columns are id, type, then the sorted union of fields (BODY, SENDER,
	// SUBJECT).
	want := []string{"id", "type", "BODY", "SENDER", "SUBJECT"}
	if len(res.Columns) != len(want) {
		t.Fatalf("cols = %v", res.Columns)
	}
	for i := range want {
		if res.Columns[i] != want[i] {
			t.Fatalf("cols = %v, want %v", res.Columns, want)
		}
	}
	row := res.Rows[0]
	got := map[string]string{}
	for i, c := range res.Columns {
		got[c] = row[i].Str
	}
	if got["id"] != "u2" || got["type"] != "email" || got["SENDER"] != "bob" ||
		got["SUBJECT"] != "hello" || got["BODY"] != "everything was fine" {
		t.Errorf("row = %+v", got)
	}
}

func TestDocE2E_NoMatch(t *testing.T) {
	eng, exec, cat := newEngineWithDocs(t)
	defer eng.Close()

	res := runE2E(t, cat, exec, "SELECT id FROM emails WHERE SENDER = 'nobody'", shoalql.PlanOptions{})
	if len(res.Rows) != 0 {
		t.Fatalf("want 0 rows, got %+v", res.Rows)
	}
}

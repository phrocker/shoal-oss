// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.

package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/extensions/effects-ssh/sshclient"
)

var journalEpoch = time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)

type fakeNow struct {
	mu  sync.Mutex
	now time.Time
}

func (f *fakeNow) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeNow) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}

func openTestJournal(t *testing.T, dir string, now *fakeNow, maxEntries int) *Journal {
	t.Helper()
	journal, err := OpenJournal(dir, JournalOptions{Retention: time.Hour, MaxEntries: maxEntries, Now: now.Now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	return journal
}

var fp = Fingerprint("restart-web")

func completed() Finished {
	status := 0
	return Finished{Outcome: OutcomeCompleted, Reference: "exit-0", ExitStatus: &status}
}

func runAndFinish(t *testing.T, j *Journal, key string) {
	t.Helper()
	if begin := j.Begin(key, fp, "restart-web", true, 4); begin.Kind != BeginRun {
		t.Fatalf("begin %s: %v", key, begin.Kind)
	}
	if entry := j.Finish(key, completed()); entry.Finished == nil {
		t.Fatalf("finish %s not durable", key)
	}
}

func appendRaw(t *testing.T, dir string, data []byte) {
	t.Helper()
	file, err := os.OpenFile(filepath.Join(dir, JournalFileName), os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		t.Fatal(err)
	}
}

func line(t *testing.T, r record) []byte {
	t.Helper()
	encoded, err := encodeLine(r)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// TestTornStartedTailIsDropped: a started record cut off by a crash was
// never durable, so nothing was dialed; it is dropped and the key is new.
func TestTornStartedTailIsDropped(t *testing.T) {
	dir, now := t.TempDir(), &fakeNow{now: journalEpoch}
	j := openTestJournal(t, dir, now, 10)
	runAndFinish(t, j, testKey("a"))
	_ = j.Close()
	full := line(t, record{Op: recordStarted, Key: testKey("b"), At: journalEpoch, Command: "restart-web", Fingerprint: fp})
	appendRaw(t, dir, full[:len(full)-1]) // everything but the LF
	before, _ := os.Stat(filepath.Join(dir, JournalFileName))

	j = openTestJournal(t, dir, now, 10)
	after, _ := os.Stat(filepath.Join(dir, JournalFileName))
	if after.Size() != before.Size()-int64(len(full)-1) {
		t.Fatalf("the torn tail was not truncated: %d -> %d", before.Size(), after.Size())
	}
	if begin := j.Begin(testKey("b"), fp, "restart-web", true, 4); begin.Kind != BeginRun {
		t.Fatalf("torn started: %v", begin.Kind)
	}
	if begin := j.Begin(testKey("a"), fp, "restart-web", true, 4); begin.Kind != BeginFinished {
		t.Fatalf("finished key: %v", begin.Kind)
	}
}

// TestTornFinishedTailLeavesUnknown: a finished record cut off by a crash
// leaves the key started, which answers unknown and never runs again.
func TestTornFinishedTailLeavesUnknown(t *testing.T) {
	dir, now := t.TempDir(), &fakeNow{now: journalEpoch}
	j := openTestJournal(t, dir, now, 10)
	key := testKey("a")
	if begin := j.Begin(key, fp, "restart-web", true, 4); begin.Kind != BeginRun {
		t.Fatal(begin.Kind)
	}
	_ = j.Close()
	status := 0
	full := line(t, record{Op: recordFinished, Key: key, At: journalEpoch,
		Finished: &Finished{Outcome: OutcomeCompleted, Reference: "exit-0", ExitStatus: &status}})
	for _, cut := range []int{1, len(full) / 2, len(full) - 1} {
		copyDir := t.TempDir()
		raw, _ := os.ReadFile(filepath.Join(dir, JournalFileName))
		_ = os.WriteFile(filepath.Join(copyDir, JournalFileName), append(raw, full[:cut]...), 0o600)
		reopened := openTestJournal(t, copyDir, now, 10)
		if begin := reopened.Begin(key, fp, "restart-web", true, 4); begin.Kind != BeginUnknown {
			t.Fatalf("cut %d: %v", cut, begin.Kind)
		}
	}
}

// TestCorruptionRefusesToOpen: a complete line that fails its checksum, or a
// record contradicting the ones before it, is not trusted.
func TestCorruptionRefusesToOpen(t *testing.T) {
	now := &fakeNow{now: journalEpoch}
	key := testKey("a")
	started := line(t, record{Op: recordStarted, Key: key, At: journalEpoch, Command: "restart-web", Fingerprint: fp})
	status := 0
	finished := line(t, record{Op: recordFinished, Key: key, At: journalEpoch,
		Finished: &Finished{Outcome: OutcomeCompleted, Reference: "exit-0", ExitStatus: &status}})
	flipped := append([]byte(nil), started...)
	flipped[20] ^= 0x01
	// A checksum that does not match a record that still decodes.
	wrongSum := append([]byte(nil), started...)
	if wrongSum[0] == '0' {
		wrongSum[0] = '1'
	} else {
		wrongSum[0] = '0'
	}
	cases := map[string][]byte{
		"flipped byte, last line":  flipped,
		"wrong checksum":           wrongSum,
		"flipped byte, mid file":   append(append([]byte(nil), flipped...), finished...),
		"finished without started": finished,
		"started twice":            append(append([]byte(nil), started...), started...),
		"finished twice":           append(append(append([]byte(nil), started...), finished...), finished...),
		"unknown op":               line(t, record{Op: "deleted", Key: key, At: journalEpoch}),
		"bad key":                  line(t, record{Op: recordStarted, Key: "short", At: journalEpoch, Command: "restart-web", Fingerprint: fp}),
		"no time":                  line(t, record{Op: recordStarted, Key: key, Command: "restart-web", Fingerprint: fp}),
		"completed without reference": append(append([]byte(nil), started...), line(t, record{Op: recordFinished, Key: key,
			At: journalEpoch, Finished: &Finished{Outcome: OutcomeCompleted}})...),
		"unknown with reference": append(append([]byte(nil), started...), line(t, record{Op: recordFinished, Key: key,
			At: journalEpoch, Finished: &Finished{Outcome: OutcomeUnknown, Reference: "exit-0"}})...),
		"unknown field": []byte("00000000 {\"op\":\"started\",\"extra\":1}\n"),
		"no checksum":   []byte("{\"op\":\"started\"}\n"),
		"blank line":    []byte("\n"),
	}
	for name, content := range cases {
		dir := t.TempDir()
		_ = os.WriteFile(filepath.Join(dir, JournalFileName), content, 0o600)
		j, err := OpenJournal(dir, JournalOptions{Retention: time.Hour, MaxEntries: 10, Now: now.Now})
		if err == nil {
			_ = j.Close()
			t.Errorf("%s: opened", name)
			continue
		}
		if !errors.Is(err, ErrJournalCorrupt) {
			t.Errorf("%s: %v", name, err)
		}
		// A refused open leaves the file as it was, and releases the lock.
		if raw, _ := os.ReadFile(filepath.Join(dir, JournalFileName)); string(raw) != string(content) {
			t.Errorf("%s: the file was changed", name)
		}
		if lock, err := lockDir(dir); err != nil {
			t.Errorf("%s: lock not released: %v", name, err)
		} else {
			_ = lock.release()
		}
	}
}

func TestJournalLockIsExclusive(t *testing.T) {
	dir, now := t.TempDir(), &fakeNow{now: journalEpoch}
	j := openTestJournal(t, dir, now, 10)
	if _, err := OpenJournal(dir, JournalOptions{Retention: time.Hour, MaxEntries: 10}); !errors.Is(err, ErrJournalLocked) {
		t.Fatalf("second open: %v", err)
	}
	_ = j.Close()
	again := openTestJournal(t, dir, now, 10)
	_ = again.Close()
}

func TestJournalOpenRefusals(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := OpenJournal(missing, JournalOptions{Retention: time.Hour, MaxEntries: 10}); err == nil {
		t.Fatal("opened a missing directory")
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the missing directory was created")
	}
	for name, options := range map[string]JournalOptions{
		"short retention": {Retention: time.Second, MaxEntries: 10},
		"no entries":      {Retention: time.Hour},
		"too many":        {Retention: time.Hour, MaxEntries: MaxJournalEntries + 1},
	} {
		if _, err := OpenJournal(t.TempDir(), options); err == nil {
			t.Errorf("%s: opened", name)
		}
	}
}

// TestRetentionPrunesOnlyExpiredIdleKeys: a key is forgotten only after its
// last record is older than retention, and never while it runs.
func TestRetentionPrunesOnlyExpiredIdleKeys(t *testing.T) {
	dir, now := t.TempDir(), &fakeNow{now: journalEpoch}
	j := openTestJournal(t, dir, now, 10)
	runAndFinish(t, j, testKey("old"))
	// Still running when its started record passes retention.
	if begin := j.Begin(testKey("long"), fp, "restart-web", true, 4); begin.Kind != BeginRun {
		t.Fatal(begin.Kind)
	}
	now.Advance(30 * time.Minute)
	runAndFinish(t, j, testKey("young"))
	if begin := j.Begin(testKey("running"), fp, "restart-web", true, 4); begin.Kind != BeginRun {
		t.Fatal(begin.Kind)
	}
	now.Advance(31 * time.Minute)
	j.mu.Lock()
	j.prune()
	j.mu.Unlock()
	if _, ok := j.Lookup(testKey("old")); ok {
		t.Fatal("an expired key was kept")
	}
	if _, ok := j.Lookup(testKey("young")); !ok {
		t.Fatal("a key within retention was pruned")
	}
	if _, ok := j.Lookup(testKey("running")); !ok {
		t.Fatal("a running key was pruned")
	}
	if _, ok := j.Lookup(testKey("long")); !ok {
		t.Fatal("a running key past retention was pruned")
	}
	_ = j.Close()
	// On reopen the same rule applies to what is on disk: the running key's
	// started record is 31 minutes old, within retention, and answers unknown.
	j = openTestJournal(t, dir, now, 10)
	if j.Len() != 2 {
		t.Fatalf("%d keys after reopen", j.Len())
	}
	if begin := j.Begin(testKey("running"), fp, "restart-web", true, 4); begin.Kind != BeginUnknown {
		t.Fatalf("running key after reopen: %v", begin.Kind)
	}
}

// TestKeyReusedAfterRetentionStillOpens: a key the running process forgot
// (past retention) and then saw again has two started records on disk until
// a compaction; the journal must still open, holding the later one.
func TestKeyReusedAfterRetentionStillOpens(t *testing.T) {
	dir, now := t.TempDir(), &fakeNow{now: journalEpoch}
	j := openTestJournal(t, dir, now, 10)
	key := testKey("reused")
	runAndFinish(t, j, key)
	now.Advance(2 * time.Hour)
	// Another key's finish prunes the expired one in memory; its records
	// stay on disk (too few dead records to compact).
	runAndFinish(t, j, testKey("other"))
	if _, known := j.Lookup(key); known {
		t.Fatal("the expired key was not pruned")
	}
	runAndFinish(t, j, key)
	_ = j.Close()
	j = openTestJournal(t, dir, now, 10)
	if begin := j.Begin(key, fp, "restart-web", true, 4); begin.Kind != BeginFinished {
		t.Fatalf("reused key after reopen: %v", begin.Kind)
	}
}

// TestCompactionKeepsEveryLiveRecordByteForByte: after compaction the file
// holds exactly the live records, and a reopen answers every key as before.
func TestCompactionKeepsEveryLiveRecordByteForByte(t *testing.T) {
	dir, now := t.TempDir(), &fakeNow{now: journalEpoch}
	j := openTestJournal(t, dir, now, 5000)
	for i := 0; i < compactSlack+200; i++ {
		runAndFinish(t, j, testKey("dead"+string(rune(i))))
	}
	now.Advance(2 * time.Hour)
	unfinished := testKey("unfinished")
	if begin := j.Begin(unfinished, fp, "restart-web", true, 4); begin.Kind != BeginRun {
		t.Fatal(begin.Kind)
	}
	for i := 0; i < 5; i++ {
		runAndFinish(t, j, testKey("live"+string(rune(i))))
	}
	records, torn, err := ReadJournal(dir)
	if err != nil || torn {
		t.Fatal(err, torn)
	}
	// The dead keys expired, so the finishes after them compacted the file.
	if len(records) != 1+10 {
		t.Fatalf("%d records after compaction", len(records))
	}
	if _, err := os.Stat(filepath.Join(dir, JournalFileName+".compact")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the compaction file was left behind")
	}
	// The running key finishes after the compaction moved its record.
	if entry := j.Finish(unfinished, completed()); entry.Finished == nil {
		t.Fatal("finish after compaction")
	}
	_ = j.Close()
	j = openTestJournal(t, dir, now, 5000)
	for i := 0; i < 5; i++ {
		if begin := j.Begin(testKey("live"+string(rune(i))), fp, "restart-web", true, 4); begin.Kind != BeginFinished {
			t.Fatalf("live %d: %v", i, begin.Kind)
		}
	}
	if begin := j.Begin(unfinished, fp, "restart-web", true, 4); begin.Kind != BeginFinished {
		t.Fatalf("unfinished: %v", begin.Kind)
	}
}

// TestWriteFailureFailsClosed: a failed append is never a run, the journal
// refuses new keys from then on, and a run whose finish could not be written
// answers unknown.
func TestWriteFailureFailsClosed(t *testing.T) {
	dir, now := t.TempDir(), &fakeNow{now: journalEpoch}
	j := openTestJournal(t, dir, now, 10)
	runAndFinish(t, j, testKey("done"))
	running := testKey("running")
	if begin := j.Begin(running, fp, "restart-web", true, 4); begin.Kind != BeginRun {
		t.Fatal(begin.Kind)
	}
	j.mu.Lock()
	_ = j.file.Close() // every later write fails
	j.mu.Unlock()
	if entry := j.Finish(running, completed()); entry.Finished != nil {
		t.Fatal("a finish that failed to write is reported durable")
	}
	select {
	case <-j.Failed():
	default:
		t.Fatal("the journal did not report the failure")
	}
	if begin := j.Begin(running, fp, "restart-web", true, 4); begin.Kind != BeginUnknown {
		t.Fatalf("unrecorded finish: %v", begin.Kind)
	}
	if begin := j.Begin(testKey("new"), fp, "restart-web", true, 4); begin.Kind != BeginUnavailable {
		t.Fatalf("new key on a failed journal: %v", begin.Kind)
	}
	if begin := j.Begin(testKey("done"), fp, "restart-web", true, 4); begin.Kind != BeginFinished {
		t.Fatalf("known key on a failed journal: %v", begin.Kind)
	}
}

// TestStartedIsDurableBeforeTheRun: when the command is dialed, its started
// record is already on disk.
func TestStartedIsDurableBeforeTheRun(t *testing.T) {
	var onDisk bool
	e := newEnv(t, func(o *Options, _ *JournalOptions) {
		o.Run = func(ctx context.Context, command *Command) sshclient.Result {
			records, _, err := ReadJournal(o.Journal.dir)
			onDisk = err == nil && len(records) == 1 && records[0].Op == recordStarted
			return runOverSSH(ctx, command)
		}
	})
	e.post("restart-web", testKey("durable")).is(t, loadAnswers(t), "completed")
	if !onDisk {
		t.Fatal("the run began before its started record was on disk")
	}
}

// TestSyncsPrecedeTheDial: the journal's directory entry is synced when the
// journal opens, the file is synced after the started record and before the
// run, and again after the finished record.
func TestSyncsPrecedeTheDial(t *testing.T) {
	var mu sync.Mutex
	var events []string
	note := func(event string) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	}
	original := syncFile
	syncFile = func(f *os.File) error {
		info, err := f.Stat()
		if err == nil && info.IsDir() {
			note("sync dir")
		} else {
			note("sync " + filepath.Base(f.Name()))
		}
		return original(f)
	}
	t.Cleanup(func() { syncFile = original })
	e := newEnv(t, func(o *Options, _ *JournalOptions) {
		o.Run = func(ctx context.Context, command *Command) sshclient.Result {
			note("dial")
			return runOverSSH(ctx, command)
		}
	})
	e.post("restart-web", testKey("order")).is(t, loadAnswers(t), "completed")
	mu.Lock()
	defer mu.Unlock()
	want := []string{"sync dir", "sync " + JournalFileName, "dial", "sync " + JournalFileName}
	if strings.Join(events, ",") != strings.Join(want, ",") {
		t.Fatalf("events %q, want %q", events, want)
	}
}

// TestSyncFailureBeforeTheDialRunsNothing: a started record whose fsync
// fails is not a run.
func TestSyncFailureBeforeTheDialRunsNothing(t *testing.T) {
	e := newEnv(t)
	original := syncFile
	syncFile = func(*os.File) error { return errors.New("injected fsync failure") }
	t.Cleanup(func() { syncFile = original })
	e.post("restart-web", testKey("fsync")).is(t, loadAnswers(t), "refused_journal_unavailable")
	if e.host.ssh.Accepts() != 0 {
		t.Fatal("dialed after a failed fsync")
	}
	select {
	case <-e.journal.Failed():
	default:
		t.Fatal("the journal did not fail")
	}
}

// TestOneFailedSyncFailsTheJournalForGood: Linux may report an fsync error
// once and then let the next fsync succeed over lost data, so after one
// failure no new key is ever started, even when the disk seems to recover.
func TestOneFailedSyncFailsTheJournalForGood(t *testing.T) {
	e := newEnv(t)
	original := syncFile
	syncFile = func(*os.File) error { return errors.New("injected fsync failure") }
	e.post("restart-web", testKey("first")).is(t, loadAnswers(t), "refused_journal_unavailable")
	syncFile = original
	t.Cleanup(func() { syncFile = original })
	e.post("restart-web", testKey("second")).is(t, loadAnswers(t), "refused_journal_unavailable")
	if e.host.ssh.Accepts() != 0 {
		t.Fatal("dialed on a failed journal")
	}
}

// TestBeginUnavailableDialsNothing: a started record that cannot be written
// means no run.
func TestBeginUnavailableDialsNothing(t *testing.T) {
	e := newEnv(t)
	e.journal.mu.Lock()
	_ = e.journal.file.Close()
	e.journal.mu.Unlock()
	e.post("restart-web", testKey("x")).is(t, loadAnswers(t), "refused_journal_unavailable")
	if e.host.ssh.Accepts() != 0 {
		t.Fatal("dialed with no durable started record")
	}
}

// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
)

// TestAClosedVocabularyAdjudicatesWhatItDoesNotDeclare is #740 phase 2, and
// the probe that matters is the readable token.
//
// Phase 1 refuses anything that is not a lowercase token, which takes the
// channel from a kilobyte to sixty-four bytes and no further:
// patient_4471_hiv_positive passes it. That probe passes in phase 1 and must
// fail here, because the difference between the two phases is exactly
// whether a well-formed token can still carry what the work read.
func TestAClosedVocabularyAdjudicatesWhatItDoesNotDeclare(t *testing.T) {
	declared := Action{Name: "query", ErrorCodes: []string{
		"target_rejected_502", "upstream_refused",
	}}
	for _, probe := range []struct {
		name   string
		action Action
		value  string
		open   bool
		closed bool
	}{
		{"a declared code", declared, "upstream_refused", false, false},
		{"a declared composed code", declared, "target_rejected_502",
			false, false},
		{"an undeclared token", declared, "upstream_unreachable",
			false, true},
		{
			// The whole point. Open it passes, closed it does not.
			name: "a readable token", action: declared,
			value: "patient_4471_hiv_positive", open: false, closed: true,
		},
		{"an action declaring nothing adjudicates everything",
			Action{}, "upstream_refused", false, true},
		{"prose is adjudicated either way", declared,
			"the target said: no", true, true},
		{"a service code is adjudicated either way", declared,
			"invalid_executor_output", true, true},
		{"no error is left alone either way", declared, "", false, false},
	} {
		t.Run(probe.name, func(t *testing.T) {
			open := (&DispatchService{}).
				reportedErrorCodeIsAdjudicated(probe.action, probe.value)
			if open != probe.open {
				t.Fatalf("with the vocabulary open, adjudicated = %v for "+
					"%q, want %v", open, probe.value, probe.open)
			}
			closed := (&DispatchService{closedErrorVocab: true}).
				reportedErrorCodeIsAdjudicated(probe.action, probe.value)
			if closed != probe.closed {
				t.Fatalf("with the vocabulary closed, adjudicated = %v for "+
					"%q, want %v", closed, probe.value, probe.closed)
			}
		})
	}
}

// TestTheFlagIsWhatChangesTheAnswer keeps the two phases from collapsing
// into one. If a change made the closed rule the default, or made the flag
// inert, every row above would still pass on one of its two columns — so
// this asserts that at least one value is decided differently by the
// setting alone, which is the only thing the flag is for.
func TestTheFlagIsWhatChangesTheAnswer(t *testing.T) {
	action := Action{ErrorCodes: []string{"upstream_refused"}}
	const undeclared = "patient_4471_hiv_positive"
	if (&DispatchService{}).
		reportedErrorCodeIsAdjudicated(action, undeclared) {
		t.Fatal("the default service adjudicated an undeclared token, so " +
			"the vocabulary is closed without the flag and an operator " +
			"has no chance to declare before their codes start vanishing")
	}
	if !(&DispatchService{closedErrorVocab: true}).
		reportedErrorCodeIsAdjudicated(action, undeclared) {
		t.Fatal("the flag changed nothing, so the channel is never closed")
	}
}

// TestTheConfigReachesTheService is the join. The flag is a field on
// DispatchConfig that nothing but NewDispatchService reads, so a missed
// assignment there leaves every test above green and the deployed service
// permanently in phase 1.
func TestTheConfigReachesTheService(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, _ := auth.NewAuthorityWithClock(func() time.Time { return now })
	registryStore := newMemoryStore()
	registry, err := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(),
		Recorder: &memoryRecorder{}, Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": &dispatchExecutor{}},
		Clock:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, closed := range []bool{false, true} {
		service, err := NewDispatchService(DispatchConfig{
			Store: newMemoryDispatchStore(), Registry: registry,
			Resolver: authority.Resolver(), Recorder: &dispatchRecorder{},
			Events:                &unentitledEvents{},
			Clock:                 func() time.Time { return now },
			ClosedErrorVocabulary: closed,
		})
		if err != nil {
			t.Fatal(err)
		}
		if service.closedErrorVocab != closed {
			t.Fatalf("ClosedErrorVocabulary=%v did not reach the service",
				closed)
		}
	}
}

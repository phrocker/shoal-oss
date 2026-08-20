package ingestservice

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/cclient"
	"github.com/phrocker/shoal-oss/internal/ingestrouter"
	clientgen "github.com/phrocker/shoal-oss/internal/thrift/gen/client"
	"github.com/phrocker/shoal-oss/internal/thrift/gen/data"
	"github.com/phrocker/shoal-oss/internal/thrift/gen/security"
	"github.com/phrocker/shoal-oss/internal/thrift/gen/tabletingest"
	"github.com/phrocker/shoal-oss/internal/thrift/gen/tabletserver"
)

type fakeAuth struct {
	mu          sync.Mutex
	deniedTable string
}

func (a *fakeAuth) Authenticate(_ context.Context, credentials *security.TCredentials) error {
	if credentials == nil || credentials.Principal != "writer" {
		return errors.New("denied")
	}
	return nil
}

func (a *fakeAuth) AuthorizeWrite(_ context.Context, _ *security.TCredentials, table string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if table == a.deniedTable {
		return ErrPermissionDenied
	}
	return nil
}

type fakeDirectory struct {
	mu      sync.Mutex
	tablets map[string]*fakeTablet
	errs    map[string]error
}

func (d *fakeDirectory) Lookup(_ context.Context, extent ingestrouter.Extent) (ingestrouter.HostedTablet, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.errs[extent.Key()]; err != nil {
		return nil, err
	}
	tablet := d.tablets[extent.Key()]
	if tablet == nil {
		return nil, ingestrouter.ErrNotHosted
	}
	return tablet, nil
}

type fakeTablet struct {
	mu      sync.Mutex
	extent  ingestrouter.Extent
	fence   ingestrouter.Fence
	commits []ingestrouter.CommitRequest
	err     error
}

func (t *fakeTablet) Extent() ingestrouter.Extent { return t.extent }
func (t *fakeTablet) Fence() ingestrouter.Fence   { return t.fence }
func (t *fakeTablet) Authority() ingestrouter.CommitAuthority {
	return ingestrouter.AuthorityAccumuloWAL
}
func (t *fakeTablet) Commit(_ context.Context, request ingestrouter.CommitRequest) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.commits = append(t.commits, request)
	return t.err
}

func newTestService(t *testing.T, cfg func(*Config), directory *fakeDirectory) *Service {
	t.Helper()
	router, err := ingestrouter.New(directory, ingestrouter.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	options := Config{Router: router, Authenticator: &fakeAuth{}}
	if cfg != nil {
		cfg(&options)
	}
	service, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func testCredentials() *security.TCredentials {
	return &security.TCredentials{Principal: "writer", InstanceId: "iid", Token: []byte("token")}
}

func testExtent(table string, end string) *data.TKeyExtent {
	return &data.TKeyExtent{Table: []byte(table), EndRow: []byte(end)}
}

func testMutation(t *testing.T, row string) *data.TMutation {
	t.Helper()
	mutation, _ := cclient.NewMutation([]byte(row))
	mutation.PutLatest([]byte("cf"), []byte("cq"), []byte("A"), []byte("value"))
	wire, err := mutation.ToThrift()
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func TestUpdateSessionAppliesAndCloses(t *testing.T) {
	extent := ingestrouter.Extent{TableID: "1", EndRow: []byte("z")}
	tablet := &fakeTablet{
		extent: extent,
		fence:  ingestrouter.Fence{ServerGeneration: "s", ManagerGeneration: "m", Assignment: 1},
	}
	service := newTestService(t, nil, &fakeDirectory{
		tablets: map[string]*fakeTablet{extent.Key(): tablet}, errs: make(map[string]error),
	})
	id, err := service.StartUpdate(context.Background(), nil, testCredentials(), tabletingest.TDurability_SYNC)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ApplyUpdates(context.Background(), nil, id, testExtent("1", "z"),
		[]*data.TMutation{testMutation(t, "a")}); err != nil {
		t.Fatal(err)
	}
	result, err := service.CloseUpdate(context.Background(), nil, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.FailedExtents) != 0 || len(result.ViolationSummaries) != 0 {
		t.Fatalf("close result = %#v", result)
	}
	tablet.mu.Lock()
	defer tablet.mu.Unlock()
	if len(tablet.commits) != 1 {
		t.Fatalf("commits = %#v", tablet.commits)
	}
	if tablet.commits[0].Mutations[0].Updates[0].Timestamp.Set {
		t.Fatal("PutLatest became an explicit timestamp before tablet authority")
	}
}

func TestUpdateSessionReportsRetryAndPartialFailure(t *testing.T) {
	first := ingestrouter.Extent{TableID: "1", EndRow: []byte("m")}
	second := ingestrouter.Extent{TableID: "1", PrevEndRow: []byte("m"), EndRow: []byte("z")}
	directory := &fakeDirectory{
		tablets: map[string]*fakeTablet{
			first.Key(): {
				extent: first,
				fence:  ingestrouter.Fence{ServerGeneration: "s", ManagerGeneration: "m", Assignment: 1},
			},
		},
		errs: map[string]error{
			second.Key(): &ingestrouter.RouteError{
				Cause: ingestrouter.ErrStaleExtent,
				RetryExtents: []ingestrouter.Extent{
					{TableID: "1", PrevEndRow: []byte("m"), EndRow: []byte("t")},
					{TableID: "1", PrevEndRow: []byte("t"), EndRow: []byte("z")},
				},
			},
		},
	}
	service := newTestService(t, nil, directory)
	id, _ := service.StartUpdate(context.Background(), nil, testCredentials(), tabletingest.TDurability_LOG)
	_ = service.ApplyUpdates(context.Background(), nil, id, testExtent("1", "m"),
		[]*data.TMutation{testMutation(t, "a")})
	secondWire := &data.TKeyExtent{Table: []byte("1"), PrevEndRow: []byte("m"), EndRow: []byte("z")}
	_ = service.ApplyUpdates(context.Background(), nil, id, secondWire,
		[]*data.TMutation{testMutation(t, "x")})
	result, err := service.CloseUpdate(context.Background(), nil, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.FailedExtents) != 1 {
		t.Fatalf("failed extents = %#v, want only the submitted stale extent", result.FailedExtents)
	}
	for _, committed := range result.FailedExtents {
		if committed != 0 {
			t.Fatalf("committed prefix = %d, want 0", committed)
		}
	}
	if metrics := service.Metrics(); metrics.AppliedBatches != 1 || metrics.RetriedBatches != 1 {
		t.Fatalf("metrics = %#v", metrics)
	}
}

func TestUpdateSessionReportsCommittedPrefixForLaterExtentFailure(t *testing.T) {
	extent := ingestrouter.Extent{TableID: "1", EndRow: []byte("z")}
	tablet := &fakeTablet{
		extent: extent,
		fence:  ingestrouter.Fence{ServerGeneration: "s", ManagerGeneration: "m", Assignment: 1},
	}
	service := newTestService(t, nil, &fakeDirectory{
		tablets: map[string]*fakeTablet{extent.Key(): tablet},
		errs:    map[string]error{},
	})
	id, _ := service.StartUpdate(context.Background(), nil, testCredentials(), tabletingest.TDurability_LOG)
	wireExtent := testExtent("1", "z")
	_ = service.ApplyUpdates(context.Background(), nil, id, wireExtent,
		[]*data.TMutation{testMutation(t, "a"), testMutation(t, "b")})
	tablet.err = ingestrouter.ErrRetryable
	_ = service.ApplyUpdates(context.Background(), nil, id, wireExtent,
		[]*data.TMutation{testMutation(t, "c")})
	result, err := service.CloseUpdate(context.Background(), nil, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, committed := range result.FailedExtents {
		if committed != 2 {
			t.Fatalf("committed prefix = %d, want 2", committed)
		}
	}
}

func TestUnsupportedModesAndConditionalFailExplicitly(t *testing.T) {
	service := newTestService(t, nil, &fakeDirectory{
		tablets: make(map[string]*fakeTablet), errs: make(map[string]error),
	})
	_, err := service.StartUpdate(context.Background(), nil, testCredentials(), tabletingest.TDurability_NONE)
	var securityErr *clientgen.ThriftSecurityException
	if !errors.As(err, &securityErr) || securityErr.Code != clientgen.SecurityErrorCode_UNSUPPORTED_OPERATION {
		t.Fatalf("NONE error = %v", err)
	}
	_, err = service.StartConditionalUpdate(context.Background(), nil, testCredentials(), nil, "1",
		tabletingest.TDurability_SYNC, "")
	if !errors.As(err, &securityErr) || securityErr.Code != clientgen.SecurityErrorCode_UNSUPPORTED_OPERATION {
		t.Fatalf("conditional error = %v", err)
	}
	_, err = service.ConditionalUpdate(context.Background(), nil, 1, nil, nil)
	var noSession *tabletserver.NoSuchScanIDException
	if !errors.As(err, &noSession) {
		t.Fatalf("conditional update error = %v", err)
	}
}

func TestWriteAuthorizationFailureIsReportedSeparately(t *testing.T) {
	router, err := ingestrouter.New(&fakeDirectory{
		tablets: make(map[string]*fakeTablet), errs: make(map[string]error),
	}, ingestrouter.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{
		Router: router, Authenticator: &fakeAuth{deniedTable: "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	id, err := service.StartUpdate(context.Background(), nil, testCredentials(), tabletingest.TDurability_SYNC)
	if err != nil {
		t.Fatal(err)
	}
	extent := testExtent("1", "z")
	_ = service.ApplyUpdates(context.Background(), nil, id, extent,
		[]*data.TMutation{testMutation(t, "a")})
	result, err := service.CloseUpdate(context.Background(), nil, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.AuthorizationFailures) != 1 || len(result.FailedExtents) != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestSessionExpiryBackpressureAndDrain(t *testing.T) {
	now := time.Unix(100, 0)
	service := newTestService(t, func(cfg *Config) {
		cfg.MaxSessions = 1
		cfg.SessionTTL = time.Second
		cfg.Now = func() time.Time { return now }
	}, &fakeDirectory{tablets: make(map[string]*fakeTablet), errs: make(map[string]error)})
	first, err := service.StartUpdate(context.Background(), nil, testCredentials(), tabletingest.TDurability_SYNC)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StartUpdate(context.Background(), nil, testCredentials(), tabletingest.TDurability_SYNC); err == nil {
		t.Fatal("second session should encounter backpressure")
	}
	now = now.Add(2 * time.Second)
	if _, err := service.StartUpdate(context.Background(), nil, testCredentials(), tabletingest.TDurability_SYNC); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CloseUpdate(context.Background(), nil, first); err == nil {
		t.Fatal("expired session unexpectedly existed")
	}
	service.BeginDrain()
	if service.Accepting() {
		t.Fatal("service remained accepting during drain")
	}
	if _, err := service.StartUpdate(context.Background(), nil, testCredentials(), tabletingest.TDurability_SYNC); err == nil {
		t.Fatal("draining service accepted a session")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Drain(ctx); err != nil {
		t.Fatalf("bounded drain: %v", err)
	}
}

func TestConcurrentCancelAndApply(t *testing.T) {
	extent := ingestrouter.Extent{TableID: "1", EndRow: []byte("z")}
	service := newTestService(t, nil, &fakeDirectory{
		tablets: map[string]*fakeTablet{extent.Key(): {
			extent: extent,
			fence:  ingestrouter.Fence{ServerGeneration: "s", ManagerGeneration: "m", Assignment: 1},
		}},
		errs: make(map[string]error),
	})
	id, _ := service.StartUpdate(context.Background(), nil, testCredentials(), tabletingest.TDurability_SYNC)
	mutation := testMutation(t, "a")
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = service.ApplyUpdates(context.Background(), nil, id, testExtent("1", "z"),
				[]*data.TMutation{mutation})
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = service.CancelUpdate(context.Background(), nil, id)
	}()
	wg.Wait()
}

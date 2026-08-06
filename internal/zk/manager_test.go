package zk

import (
	"context"
	"errors"
	"fmt"
	"testing"

	gozk "github.com/go-zookeeper/zk"
)

type managerLocator struct {
	children    []string
	childrenErr error
	data        []byte
	dataErr     error
	path        string
}

func (l *managerLocator) InstancePath() string { return "/accumulo/uuid-1" }

func (l *managerLocator) GetRaw(_ context.Context, path string) ([]byte, error) {
	l.path = path
	return l.data, l.dataErr
}

func (l *managerLocator) Children(_ context.Context, path string) ([]string, error) {
	l.path = path
	return l.children, l.childrenErr
}

func TestManagerAddress(t *testing.T) {
	locator := &managerLocator{
		children: []string{
			"not-a-lock",
			"zlock#f50c7911-a203-4e3d-b006-bdb30848f5bd#0000000002",
			"zlock#407b5e9b-9d92-4aaa-bff0-d9ba9be206a6#0000000001",
		},
		data: []byte(`{
		"descriptors": [
			{"uuid":"1","service":"FATE_CLIENT","address":"manager:9999","group":"default"},
			{"uuid":"1","service":"MANAGER","address":"manager:9997","group":"default"}
		]
	}`),
	}
	got, err := ManagerAddress(context.Background(), locator)
	if err != nil {
		t.Fatal(err)
	}
	if got != "manager:9997" {
		t.Fatalf("address = %q, want manager:9997", got)
	}
	if locator.path != "/accumulo/uuid-1/managers/lock/zlock#407b5e9b-9d92-4aaa-bff0-d9ba9be206a6#0000000001" {
		t.Fatalf("path = %q", locator.path)
	}
}

func TestManagerAddressUnavailableAndCancellation(t *testing.T) {
	for _, data := range []string{
		`{"descriptors":[]}`,
		`{"descriptors":[{"service":"MANAGER","address":"0.0.0.0:0"}]}`,
	} {
		_, err := ManagerAddress(context.Background(), &managerLocator{
			children: []string{"zlock#407b5e9b-9d92-4aaa-bff0-d9ba9be206a6#0000000001"},
			data:     []byte(data),
		})
		if !errors.Is(err, ErrManagerUnavailable) {
			t.Fatalf("error = %v, want ErrManagerUnavailable", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ManagerAddress(ctx, &managerLocator{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v", err)
	}
}

func TestFirstLockNodeUsesAccumuloSequenceRange(t *testing.T) {
	const uuid = "407b5e9b-9d92-4aaa-bff0-d9ba9be206a6"
	maxSequence := "zlock#" + uuid + "#2147483647"
	if got := firstLockNode([]string{maxSequence}); got != maxSequence {
		t.Fatalf("max Accumulo sequence = %q, want %q", got, maxSequence)
	}
	overflow := "zlock#" + uuid + "#2147483648"
	if got := firstLockNode([]string{overflow}); got != "" {
		t.Fatalf("overflow sequence = %q, want invalid", got)
	}
}

type clientServiceLocator struct {
	children map[string][]string
	data     map[string][]byte
	err      map[string]error
}

func (l *clientServiceLocator) InstancePath() string { return "/accumulo/uuid-1" }

func (l *clientServiceLocator) Children(ctx context.Context, path string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := l.err[path]; err != nil {
		return nil, err
	}
	children, ok := l.children[path]
	if !ok {
		return nil, gozk.ErrNoNode
	}
	return append([]string(nil), children...), nil
}

func (l *clientServiceLocator) GetRaw(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := l.err[path]; err != nil {
		return nil, err
	}
	data, ok := l.data[path]
	if !ok {
		return nil, gozk.ErrNoNode
	}
	return append([]byte(nil), data...), nil
}

func TestClientServiceAddressesUsesAllAccumulo4ServerTypes(t *testing.T) {
	const lock = "zlock#407b5e9b-9d92-4aaa-bff0-d9ba9be206a6#0000000001"
	locator := &clientServiceLocator{
		children: map[string][]string{},
		data:     map[string][]byte{},
		err:      map[string]error{},
	}
	servers := []struct {
		root, server, address string
	}{
		{"/tservers", "tablet:9997", "tablet-client:9997"},
		{"/sservers", "scan:9997", "scan-client:9997"},
		{"/compactors", "compactor:9997", "compactor-client:9997"},
	}
	for _, server := range servers {
		root := "/accumulo/uuid-1" + server.root
		group := root + "/default"
		serverPath := group + "/" + server.server
		lockPath := serverPath + "/" + lock
		locator.children[root] = []string{"default"}
		locator.children[group] = []string{server.server}
		locator.children[serverPath] = []string{"invalid", lock}
		locator.data[lockPath] = []byte(fmt.Sprintf(`{
			"descriptors":[
				{"service":"TABLET_SCAN","address":"ignored:9997"},
				{"service":"CLIENT","address":%q}
			]
		}`, server.address))
	}

	got, err := ClientServiceAddresses(context.Background(), locator)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"compactor-client:9997", "scan-client:9997", "tablet-client:9997"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("addresses = %v, want %v", got, want)
	}
}

func TestClientServiceAddressesUnavailableErrorsAndCancellation(t *testing.T) {
	locator := &clientServiceLocator{
		children: map[string][]string{},
		data:     map[string][]byte{},
		err:      map[string]error{},
	}
	if _, err := ClientServiceAddresses(context.Background(), locator); !errors.Is(err, ErrClientServiceUnavailable) {
		t.Fatalf("empty error = %v, want ErrClientServiceUnavailable", err)
	}

	locator.err["/accumulo/uuid-1/tservers"] = errors.New("zk unavailable")
	if _, err := ClientServiceAddresses(context.Background(), locator); err == nil ||
		errors.Is(err, ErrClientServiceUnavailable) {
		t.Fatalf("zookeeper error = %v, want surfaced discovery failure", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ClientServiceAddresses(ctx, locator); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v, want context.Canceled", err)
	}
}

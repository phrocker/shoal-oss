package zk

import (
	"context"
	"errors"
	"testing"
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

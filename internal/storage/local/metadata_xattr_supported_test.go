//go:build linux || darwin

package local

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestReconcilePlatformXattrsMatchesTargetSet(t *testing.T) {
	attrs := map[string]map[string][]byte{
		"target": {"user.keep": []byte("target")},
		"temp": {
			"user.keep":      []byte("inherited"),
			"user.inherited": []byte("remove"),
		},
	}
	ops := mapXattrOperations(attrs)

	if err := reconcilePlatformXattrs("temp", "target", ops); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(attrs["temp"], attrs["target"]) {
		t.Fatalf("temporary xattrs = %v, want exact target set %v", attrs["temp"], attrs["target"])
	}
}

func TestReconcilePlatformXattrsReportsRemovalFailure(t *testing.T) {
	removeErr := errors.New("remove denied")
	ops := xattrOperations{
		list: func(path string) ([]string, error) {
			if path == "temp" {
				return []string{"user.inherited"}, nil
			}
			return nil, nil
		},
		get:    func(string, string) ([]byte, error) { return nil, nil },
		set:    func(string, string, []byte, int) error { return nil },
		remove: func(string, string) error { return removeErr },
	}

	err := reconcilePlatformXattrs("temp", "target", ops)
	if !errors.Is(err, removeErr) || !strings.Contains(err.Error(), "remove inherited extended attribute") {
		t.Fatalf("error = %v, want explicit inherited-attribute removal failure", err)
	}
}

func TestReconcilePlatformXattrsDropsSecurityCapability(t *testing.T) {
	attrs := map[string]map[string][]byte{
		"target": {
			"user.keep":           []byte("target"),
			"security.capability": []byte("cap"),
			"security.ima":        []byte("ima"),
			"security.evm":        []byte("evm"),
		},
		"temp": {
			"user.keep":           []byte("inherited"),
			"security.capability": []byte("cap"),
			"security.ima":        []byte("ima"),
			"security.evm":        []byte("evm"),
			"user.inherited":      []byte("remove"),
		},
	}
	ops := mapXattrOperations(attrs)

	if err := reconcilePlatformXattrs("temp", "target", ops); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"security.capability", "security.ima", "security.evm"} {
		if _, ok := attrs["temp"][name]; ok {
			t.Fatalf("temporary xattrs = %v, unexpected %s", attrs["temp"], name)
		}
	}
	if got := string(attrs["temp"]["user.keep"]); got != "target" {
		t.Fatalf("preserved attribute = %q, want target", got)
	}
}

func mapXattrOperations(attrs map[string]map[string][]byte) xattrOperations {
	return xattrOperations{
		list: func(path string) ([]string, error) {
			var names []string
			for name := range attrs[path] {
				names = append(names, name)
			}
			return names, nil
		},
		get: func(path, name string) ([]byte, error) {
			return append([]byte(nil), attrs[path][name]...), nil
		},
		set: func(path, name string, value []byte, _ int) error {
			attrs[path][name] = append([]byte(nil), value...)
			return nil
		},
		remove: func(path, name string) error {
			delete(attrs[path], name)
			return nil
		},
	}
}

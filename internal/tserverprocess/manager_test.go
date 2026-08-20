package tserverprocess

import "testing"

func TestValidateAuthorizationsRequiresGrantedSubset(t *testing.T) {
	granted := [][]byte{[]byte("A"), []byte("tenant:7")}
	if err := validateAuthorizations("alice", [][]byte{[]byte("tenant:7")}, granted); err != nil {
		t.Fatal(err)
	}
	if err := validateAuthorizations("alice", [][]byte{[]byte("admin")}, granted); err == nil {
		t.Fatal("ungranted authorization accepted")
	}
}

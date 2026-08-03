package accumulo_test

import (
	"testing"

	"github.com/phrocker/shoal/accumulo"
)

func TestPublicBootstrapAPICompiles(t *testing.T) {
	instance, err := accumulo.NewStaticInstance("accumulo", "uuid-1")
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := accumulo.PasswordCredentials("root", []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	connector, err := accumulo.NewConnector(instance, credentials, accumulo.ConnectorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := connector.Close(); err != nil {
		t.Fatal(err)
	}
}

package ingestclient

import (
	"context"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
)

func TestAbsentConditionalValueIsOmittedOnWire(t *testing.T) {
	buffer := thrift.NewTMemoryBuffer()
	writer := thrift.NewTCompactProtocol(buffer)
	original := &data.TCondition{Cf: []byte("loc"), Cq: []byte("session"), Cv: []byte{}, Val: nil}
	if err := original.Write(context.Background(), writer); err != nil {
		t.Fatal(err)
	}
	reader := thrift.NewTCompactProtocol(buffer)
	decoded := &data.TCondition{}
	if err := decoded.Read(context.Background(), reader); err != nil {
		t.Fatal(err)
	}
	if decoded.Val != nil {
		t.Fatalf("absent condition value decoded as present: %v", decoded.Val)
	}
}

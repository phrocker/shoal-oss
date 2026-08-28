package model

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestFakeProvidersDeterministicAndIsolated(t *testing.T) {
	embedder := FakeEmbedder{Dimensions: 8}
	first, err := embedder.Embed(context.Background(), EmbedRequest{Text: "Hello"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := embedder.Embed(context.Background(), EmbedRequest{Text: "Hello"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("embedding is not deterministic: %#v != %#v", first, second)
	}
	first.Vector[0] = 99
	third, err := embedder.Embed(context.Background(), EmbedRequest{Text: "Hello"})
	if err != nil {
		t.Fatal(err)
	}
	if third.Vector[0] == 99 {
		t.Fatal("mutating one result changed a later result")
	}

	generator := FakeGenerator{}
	generated, err := generator.Generate(context.Background(), GenerateRequest{Prompt: "Find the project entity"})
	if err != nil {
		t.Fatal(err)
	}
	if generated.Text != "causal,entity" {
		t.Fatalf("text = %q", generated.Text)
	}
	if generated.Provenance.Provider != "fake" || third.Provenance.Provider != "fake" {
		t.Fatal("missing fake provenance")
	}
}

func TestFakeProvidersConcurrent(t *testing.T) {
	embedder := FakeEmbedder{Dimensions: 16}
	const workers = 32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := embedder.Embed(context.Background(), EmbedRequest{Text: "same"})
			if err != nil {
				errs <- err
				return
			}
			if len(result.Vector) != 16 {
				errs <- errors.New("unexpected vector length")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestFakeCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (FakeGenerator{}).Generate(ctx, GenerateRequest{})
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("error = %v, want ErrCanceled", err)
	}
}

package model

import "testing"

// TestConfiguredProvidersClassifyTheirOwnEgress is the property the fleet
// effect boundary depends on: the answer comes from the configuration that was
// actually validated, not from the type.
//
// The Ollama and OpenAI rows are the point. The same constructor, the same
// code, opposite answers — because a loopback inference server transmits
// nothing and a hosted endpoint transmits every prompt it is given.
func TestConfiguredProvidersClassifyTheirOwnEgress(t *testing.T) {
	loopbackOpenAI, err := NewOpenAIGenerator(OpenAIConfig{
		BaseURL: "http://127.0.0.1:8000", GenerationModel: "local",
		Credentials: staticCredential("k"),
	})
	if err != nil {
		t.Fatal(err)
	}
	hostedOpenAI, err := NewOpenAIGenerator(OpenAIConfig{
		BaseURL: "https://api.example.test", GenerationModel: "hosted",
		Credentials: staticCredential("k"),
	})
	if err != nil {
		t.Fatal(err)
	}
	loopbackOllama, err := NewOllamaGenerator(OllamaConfig{
		BaseURL: "http://localhost:11434", Model: "llama3",
	})
	if err != nil {
		t.Fatal(err)
	}
	hostedOllama, err := NewOllamaGenerator(OllamaConfig{
		BaseURL: "https://ollama.example.test", Model: "llama3",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("SHOAL_TEST_VOYAGE_KEY", "k")
	voyage, err := NewVoyageEmbedder(VoyageConfig{
		BaseURL: "https://api.voyageai.com", Model: "voyage-3",
		Dimensions: 8, APICredentialEnv: "SHOAL_TEST_VOYAGE_KEY",
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, probe := range []struct {
		name     string
		provider any
		egresses bool
	}{
		{"loopback openai-compatible", loopbackOpenAI, false},
		{"hosted openai-compatible", hostedOpenAI, true},
		{"loopback ollama", loopbackOllama, false},
		{"hosted ollama", hostedOllama, true},
		{"voyage is unconditionally hosted", voyage, true},
		{"fake generator", FakeGenerator{}, false},
		{"fake embedder", FakeEmbedder{Dimensions: 4}, false},
	} {
		if got := EgressesOffHost(probe.provider); got != probe.egresses {
			t.Fatalf("%s: EgressesOffHost = %v, want %v",
				probe.name, got, probe.egresses)
		}
	}
}

// TestUnclassifiableProviderIsTreatedAsEgressing is the fail-closed direction.
// A provider that cannot answer is not evidence that content stays put, and
// reading silence as "local" would make the permissive answer the default for
// exactly the class that exists to gate transmission.
func TestUnclassifiableProviderIsTreatedAsEgressing(t *testing.T) {
	if !EgressesOffHost(unclassifiedGenerator{}) {
		t.Fatal("a provider that cannot classify itself was read as local")
	}
	// A nil provider is not a configured endpoint, so there is nothing for it
	// to transmit to.
	if EgressesOffHost(nil) {
		t.Fatal("an absent provider was reported as egressing")
	}
}

// TestNilTypedProviderFailsClosed covers the typed-nil case, which reaches the
// method rather than the nil branch.
func TestNilTypedProviderFailsClosed(t *testing.T) {
	var generator *OpenAIGenerator
	if !EgressesOffHost(generator) {
		t.Fatal("an unconfigured OpenAI generator was read as local")
	}
	var ollama *OllamaGenerator
	if !EgressesOffHost(ollama) {
		t.Fatal("an unconfigured Ollama generator was read as local")
	}
}

type unclassifiedGenerator struct{}

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/phrocker/shoal/internal/agentmem"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9876", "shoal-embed gRPC address")
	table := flag.String("table", agentmem.DefaultTable, "shoal table")
	text := flag.String("text", "", "text for ingest/query; stdin is used when empty")
	embedderName := flag.String("embedder", "fake", "embedder adapter: fake or ollama")
	llmName := flag.String("llm", "fake", "LLM adapter: fake or ollama")
	ollamaHost := flag.String("ollama-host", agentmem.DefaultOllamaHost, "Ollama HTTP host")
	embedModel := flag.String("embed-model", agentmem.DefaultOllamaEmbedModel, "Ollama embedding model")
	llmModel := flag.String("llm-model", agentmem.DefaultOllamaLLMModel, "Ollama LLM model")
	flag.Parse()
	if flag.NArg() < 1 {
		usage()
		os.Exit(2)
	}
	ctx := context.Background()
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()
	cfg := agentmem.Config{Table: *table, Store: agentmem.NewGRPCStore(conn)}
	switch strings.ToLower(strings.TrimSpace(*embedderName)) {
	case "", "fake":
	case "ollama":
		cfg.Embedder = agentmem.NewOllamaEmbedder(agentmem.WithOllamaHost(*ollamaHost), agentmem.WithOllamaModel(*embedModel))
	default:
		fatal(fmt.Errorf("unknown embedder %q (want fake or ollama)", *embedderName))
	}
	switch strings.ToLower(strings.TrimSpace(*llmName)) {
	case "", "fake":
	case "ollama":
		cfg.LLM = agentmem.NewOllamaLLM(agentmem.WithOllamaHost(*ollamaHost), agentmem.WithOllamaModel(*llmModel))
	default:
		fatal(fmt.Errorf("unknown llm %q (want fake or ollama)", *llmName))
	}
	client, err := agentmem.New(cfg)
	if err != nil {
		fatal(err)
	}
	switch flag.Arg(0) {
	case "ingest":
		body := input(*text)
		res, err := client.Ingest(ctx, agentmem.IngestRequest{Text: body, Time: time.Now().UTC()})
		if err != nil {
			fatal(err)
		}
		fmt.Println(res.Row)
	case "query":
		body := input(*text)
		res, err := client.Query(ctx, agentmem.QueryRequest{Text: body, Time: time.Now().UTC()})
		if err != nil {
			fatal(err)
		}
		fmt.Println(res.Context)
	case "consolidate":
		co := agentmem.NewConsolidator(client, 1024)
		if err := co.SeedAll(ctx); err != nil {
			fatal(err)
		}
		// Local daemon mode: process seeded work until interrupted. Platform services
		// can reuse internal/agentmem and supply their own queue/lifecycle.
		fatal(co.Run(ctx))
	default:
		usage()
		os.Exit(2)
	}
}

func input(flagText string) string {
	if strings.TrimSpace(flagText) != "" {
		return flagText
	}
	b, err := io.ReadAll(os.Stdin)
	if err != nil {
		fatal(err)
	}
	return strings.TrimSpace(string(b))
}
func usage() {
	fmt.Fprintln(os.Stderr, "usage: veculo-agent [--addr host:port] [--table graph] [--text text] [--embedder fake|ollama] [--llm fake|ollama] [--ollama-host url] [--embed-model model] [--llm-model model] ingest|query|consolidate")
}
func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

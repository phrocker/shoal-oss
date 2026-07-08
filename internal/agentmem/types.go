package agentmem

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/phrocker/shoal/internal/embedpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultTable = "graph"
	DefaultDim   = 16
)

type Intent string

const (
	IntentWhy     Intent = "WHY"
	IntentWhen    Intent = "WHEN"
	IntentEntity  Intent = "ENTITY"
	IntentGeneral Intent = "GENERAL"
)

type Embedder interface {
	Embed(context.Context, string) ([]float32, error)
}
type LLM interface {
	Infer(context.Context, string) (string, error)
}
type IntentClassifier interface{ Classify(string) Intent }

type EmbedStore interface {
	CreateTable(context.Context, string, []string) error
	Write(context.Context, string, []*embedpb.Mutation) error
	Scan(context.Context, string, *embedpb.ScanRequest) ([]*embedpb.Cell, error)
	Flush(context.Context, string) error
}

type Config struct {
	Table       string
	Embedder    Embedder
	LLM         LLM
	Enricher    Enricher
	Classifier  IntentClassifier
	Store       EmbedStore
	MaxAnchors  int
	BeamWidth   int
	MaxDepth    int
	TokenBudget int

	// UseIVF, when true, sources the semantic anchor list from a trained
	// IVF-PQ index (see cmd/shoal-ivf-train and IvfIndex) instead of the
	// brute-force VectorSearch path. It degrades gracefully: if no index has
	// been trained yet, anchors() transparently falls back to brute force, so
	// enabling the flag is always safe. Default false preserves the exact
	// brute-force behavior.
	UseIVF bool
	// IvfNprobe is the number of coarse clusters probed per IVF query. Only
	// consulted when UseIVF is true; defaults to 8 when unset.
	IvfNprobe int

	// IvfFreshness, when true, keeps a trained IVF-PQ index current on the
	// write path: each Ingest assigns the new vector to its nearest existing
	// centroid, PQ-encodes it, and writes the posting into <table>_ivf so the
	// memory is searchable through the IVF path immediately — no retrain. It is
	// best-effort and always safe to enable: when no index has been trained the
	// hook is a no-op, and any indexing error is swallowed (the vector remains
	// findable via the brute-force fallback). Pairs naturally with UseIVF, but
	// is independent so producers can maintain freshness for downstream IVF
	// readers without querying via IVF themselves. Default false.
	IvfFreshness bool
}

type Client struct {
	cfg     Config
	ids     *IDGenerator
	ivfOnce sync.Once
	ivf     *IvfIndex
	ivfErr  error
}

func New(cfg Config) (*Client, error) {
	if cfg.Store == nil {
		return nil, errors.New("agentmem: store is required")
	}
	if cfg.Table == "" {
		cfg.Table = DefaultTable
	}
	if cfg.Embedder == nil {
		cfg.Embedder = FakeEmbedder{Dim: DefaultDim}
	}
	if cfg.LLM == nil {
		cfg.LLM = FakeLLM{}
	}
	if cfg.Enricher == nil {
		cfg.Enricher = HeuristicEnricher{}
	}
	if cfg.Classifier == nil {
		cfg.Classifier = RuleClassifier{}
	}
	if cfg.MaxAnchors <= 0 {
		cfg.MaxAnchors = 6
	}
	if cfg.BeamWidth <= 0 {
		cfg.BeamWidth = 8
	}
	if cfg.MaxDepth <= 0 {
		cfg.MaxDepth = 2
	}
	if cfg.TokenBudget <= 0 {
		cfg.TokenBudget = 160
	}
	if cfg.IvfNprobe <= 0 {
		cfg.IvfNprobe = 8
	}
	return &Client{cfg: cfg, ids: NewIDGenerator()}, nil
}

func PackVector(v []float32) []byte {
	out := make([]byte, len(v)*4)
	for i, f := range v {
		binary.BigEndian.PutUint32(out[i*4:], math.Float32bits(f))
	}
	return out
}

func UnpackVector(raw []byte) ([]float32, error) {
	if len(raw)%4 != 0 {
		return nil, errors.New("packed vector length must be a multiple of 4")
	}
	out := make([]float32, len(raw)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.BigEndian.Uint32(raw[i*4:]))
	}
	return out, nil
}

type GRPCStore struct{ Client embedpb.ShoalEmbedClient }

func NewGRPCStore(conn grpc.ClientConnInterface) GRPCStore {
	return GRPCStore{Client: embedpb.NewShoalEmbedClient(conn)}
}
func (s GRPCStore) CreateTable(ctx context.Context, table string, splits []string) error {
	_, err := s.Client.CreateTable(ctx, &embedpb.CreateTableRequest{Table: table, Splits: splits})
	return err
}
func (s GRPCStore) Write(ctx context.Context, table string, muts []*embedpb.Mutation) error {
	_, err := s.Client.Write(ctx, &embedpb.WriteRequest{Table: table, Mutations: muts})
	return err
}
func (s GRPCStore) Flush(ctx context.Context, table string) error {
	_, err := s.Client.Flush(ctx, &embedpb.FlushRequest{Table: table})
	return err
}
func (s GRPCStore) Scan(ctx context.Context, table string, req *embedpb.ScanRequest) ([]*embedpb.Cell, error) {
	clone := proto.Clone(req).(*embedpb.ScanRequest)
	clone.Table = table
	stream, err := s.Client.Scan(ctx, clone)
	if err != nil {
		return nil, err
	}
	var cells []*embedpb.Cell
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil, err
			}
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		cells = append(cells, resp.Cells...)
	}
	return cells, nil
}

// OllamaLLM is optional. Tests use FakeLLM; this adapter is only constructed explicitly.
type OllamaLLM struct {
	Endpoint string
	Client   *http.Client
	Model    string
}

func (o OllamaLLM) Infer(ctx context.Context, prompt string) (string, error) {
	// Keep the optional implementation dependency-free and conservative: callers may
	// replace this with a platform adapter. Returning a deterministic local fallback
	// avoids network use unless a downstream package opts into its own transport.
	if strings.TrimSpace(o.Endpoint) == "" {
		return FakeLLM{}.Infer(ctx, prompt)
	}
	return "ollama-adapter-configured", nil
}

func unixMillis(t time.Time) int64 {
	if t.IsZero() {
		t = time.Now().UTC()
	}
	return t.UTC().UnixNano() / int64(time.Millisecond)
}

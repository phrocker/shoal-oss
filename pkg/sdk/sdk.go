// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.

// Package sdk is the versioned Go client for gateways, collectors and other
// extensions. It imports only pkg/shoal and public */api packages, so an
// extension that depends on it never links Shoal internals. The import
// boundary test in internal/importboundary enforces this.
//
// Covered today: collector enrollment, artifact and observation submission
// and observation reads (pkg/collector/api); registered decision evaluation
// and reads (pkg/decision/api). Admission and report move here in a later
// slice of #446.
package sdk

import (
	"context"
	"fmt"
	"net/http"

	collectorapi "github.com/phrocker/shoal-oss/pkg/collector/api"
	decisionapi "github.com/phrocker/shoal-oss/pkg/decision/api"
)

// ProtocolVersion is the SDK's wire protocol version. It changes only when a
// covered route changes incompatibly.
const ProtocolVersion = 1

// Config is shared by every covered API. Token supplies the bearer token per
// request; the SDK never follows redirects or uses a cookie jar.
type Config struct {
	BaseURL    string
	HTTPClient *http.Client
	Token      func(context.Context) (string, error)
}

type Client struct {
	collectors *collectorapi.Client
	decisions  *decisionapi.Client
}

func New(c Config) (*Client, error) {
	collectors, e := collectorapi.NewClient(collectorapi.Config{BaseURL: c.BaseURL, HTTPClient: c.HTTPClient, Token: c.Token})
	if e != nil {
		return nil, fmt.Errorf("sdk: %w", e)
	}
	decisions, e := decisionapi.NewClient(decisionapi.Config{BaseURL: c.BaseURL, HTTPClient: c.HTTPClient, Token: c.Token})
	if e != nil {
		return nil, fmt.Errorf("sdk: %w", e)
	}
	return &Client{collectors: collectors, decisions: decisions}, nil
}

func (c *Client) Collectors() *collectorapi.Client { return c.collectors }
func (c *Client) Decisions() *decisionapi.Client   { return c.decisions }

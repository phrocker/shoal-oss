// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// tlsFilesConfig is the resolved (flag-or-env) TLS file configuration for
// shoal-embed serve. All three fields are optional; see
// buildServerTLSConfig for how they combine into a *tls.Config (or an
// error, when the configuration is invalid).
type tlsFilesConfig struct {
	// CertFile and KeyFile are PEM-encoded server certificate and private
	// key paths. Both empty disables TLS entirely for both the gRPC and
	// HTTP listeners (the pre-existing, still-default behavior). Setting
	// exactly one without the other is a startup configuration error, not
	// a silent fallback to plaintext.
	CertFile string
	KeyFile  string

	// ClientCAFile, when set, enables mutual TLS: client connections (both
	// gRPC and HTTP) must present a certificate that chains to one of the
	// CAs in this PEM bundle, or the TLS handshake is rejected. Requires
	// CertFile/KeyFile to also be set.
	ClientCAFile string
}

// empty reports whether no TLS file was configured at all, i.e. TLS stays
// disabled.
func (c tlsFilesConfig) empty() bool {
	return c.CertFile == "" && c.KeyFile == "" && c.ClientCAFile == ""
}

// validate reports a descriptive error for every partially-specified TLS
// configuration this package refuses to silently paper over: a cert
// without a key (or vice versa). An empty config (TLS fully disabled)
// always validates cleanly. A ClientCAFile with no CertFile/KeyFile is
// also rejected, via the same "cert/key both required" check, since
// CertFile/KeyFile being empty already fails it.
func (c tlsFilesConfig) validate() error {
	if c.CertFile == "" && c.KeyFile == "" && c.ClientCAFile == "" {
		return nil
	}
	if c.CertFile == "" || c.KeyFile == "" {
		return fmt.Errorf("TLS requires both --tls-cert and --tls-key (or SHOAL_EMBED_TLS_CERT/SHOAL_EMBED_TLS_KEY); got cert=%q key=%q", c.CertFile, c.KeyFile)
	}
	return nil
}

// buildServerTLSConfig loads a server certificate/key pair and, when
// clientCAFile is non-empty, configures mutual TLS: client certificates
// are required and must verify against the supplied CA bundle. It returns
// a fresh *tls.Config with MinVersion set to TLS 1.2 (current production
// baseline guidance); callers that need independent per-listener config
// should Clone() the result rather than share the pointer, since gRPC's
// credentials.NewTLS and net/http's tls.NewListener/http.Server can each
// mutate the config they're given (e.g. ALPN NextProtos) as a side effect
// of use.
func buildServerTLSConfig(certFile, keyFile, clientCAFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load TLS key pair (cert=%q, key=%q): %w", certFile, keyFile, err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if clientCAFile == "" {
		return cfg, nil
	}

	pemBytes, err := os.ReadFile(clientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read TLS client CA file %q: %w", clientCAFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("TLS client CA file %q contains no usable PEM certificates", clientCAFile)
	}
	cfg.ClientCAs = pool
	cfg.ClientAuth = tls.RequireAndVerifyClientCert

	return cfg, nil
}

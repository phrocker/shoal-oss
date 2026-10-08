// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/phrocker/shoal-oss/internal/engine"
	"github.com/phrocker/shoal-oss/internal/explorercoord"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/allocator"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// The identity-scheme row (#526) is written by the first replica to start
// for an issuer and checked by every one after. Separation of duty compares
// identities, so it holds across replicas only while they all name a human
// the same way: a replica on the stable scheme beside one on sub would let
// one human request through one and approve through the other. A replica
// whose scheme differs from the recorded one therefore refuses to start, and
// changing the scheme is an explicit operator act, -oidc-identity-scheme-migrate.
//
// Every OIDC deployment records its scheme, the sub-derived one included, so
// that a later switch to a stable claim is seen as a switch.

var (
	identitySchemeFamily    = []byte("identity-scheme")
	identitySchemeQualifier = []byte("v1")
)

// errIdentitySchemeMismatch is the startup refusal.
var errIdentitySchemeMismatch = errors.New(
	"the identity scheme configured for this issuer differs from the one " +
		"recorded in the coordination store")

// identitySchemeConfig is the OIDC identity scheme in force and whether the
// operator has asked to replace a recorded one.
type identitySchemeConfig struct {
	scheme  oidcIdentityScheme
	migrate bool
}

// approvals is what the approval service is told; the zero value without an
// OIDC authenticator.
func (c *identitySchemeConfig) approvals() fleet.IdentityScheme {
	if c == nil {
		return fleet.IdentityScheme{}
	}
	return c.scheme.approvals
}

// stampIdentityScheme records the scheme for its issuer, or checks it
// against the one recorded. Nil config records nothing.
func stampIdentityScheme(
	ctx context.Context, eng *engine.Engine, table string,
	config *identitySchemeConfig,
) error {
	if config == nil {
		return nil
	}
	if eng == nil {
		return shoal.NewError(
			shoal.ErrorUnavailable, "the coordination store is unavailable")
	}
	if table == "" {
		table = explorercoord.DefaultCoordinationTable
	}
	store, err := explorercoord.NewEngineStore(eng, table)
	if err != nil {
		return err
	}
	issuer := []byte(config.scheme.issuer)
	row, err := coordination.IdentitySchemeRow(workspacePublicationDomain, issuer)
	if err != nil {
		return err
	}
	value, err := coordination.MarshalIdentitySchemeV1(coordination.IdentitySchemeV1{
		Issuer: issuer, Scheme: coordination.Digest(config.scheme.digest),
	})
	if err != nil {
		return err
	}
	coordinate := allocator.Coordinate{
		Row: row, Family: identitySchemeFamily, Qualifier: identitySchemeQualifier,
	}
	// A lost race re-reads; the bound turns a pathological one into an
	// error rather than a spin.
	for attempt := 0; attempt < 8; attempt++ {
		cells, err := store.ReadExact(ctx, []allocator.Coordinate{coordinate})
		if err != nil {
			return err
		}
		mutation := allocator.Mutation{Row: row}
		if len(cells) == 0 {
			mutation.Conditions = []allocator.Condition{{
				Coordinate: coordinate, Absent: true,
			}}
			mutation.Updates = []allocator.Update{{
				Coordinate: coordinate, Value: value, Timestamp: 1,
			}}
		} else {
			stored, err := coordination.UnmarshalIdentitySchemeV1(cells[0].Value)
			if err != nil {
				return fmt.Errorf("refusing to start: the recorded identity "+
					"scheme cannot be read: %w", err)
			}
			if !bytes.Equal(stored.Issuer, issuer) {
				return fmt.Errorf("refusing to start: the recorded identity " +
					"scheme names another issuer")
			}
			if stored.Scheme == coordination.Digest(config.scheme.digest) {
				return nil
			}
			if !config.migrate {
				return fmt.Errorf(
					"refusing to start for issuer %s: %w; another replica, or "+
						"this one before a restart, names principals "+
						"differently, so one human could request through one "+
						"and approve through the other. Configure every "+
						"replica identically, or pass "+
						"-oidc-identity-scheme-migrate to record this scheme",
					config.scheme.issuer, errIdentitySchemeMismatch)
			}
			mutation.Conditions = []allocator.Condition{{
				Coordinate: coordinate, Value: cells[0].Value,
				Timestamp: cells[0].Timestamp, TimestampSet: true,
			}}
			mutation.Updates = []allocator.Update{{
				Coordinate: coordinate, Value: value,
				Timestamp: cells[0].Timestamp + 1,
			}}
		}
		status, err := store.CompareAndMutate(ctx, mutation)
		if status == allocator.StatusAccepted {
			return nil
		}
		if status != allocator.StatusRejected &&
			!errors.Is(err, allocator.ErrConditionalUnknown) {
			if err == nil {
				err = errors.New("identity scheme write outcome is unknown")
			}
			return err
		}
	}
	return fmt.Errorf("refusing to start: the identity scheme record kept " +
		"moving while it was being written")
}

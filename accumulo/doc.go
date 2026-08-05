// Package accumulo provides the public client bootstrap API for connecting
// Shoal-backed applications to Apache Accumulo.
//
// The current implementation targets the Accumulo 4 protocol and discovery
// layout. The connector supports table discovery, listing, existence checks,
// scanners, and mutation construction; batch writer and mutating administration
// APIs build on these stable boundaries.
package accumulo

// Package accumulo provides the public client bootstrap API for connecting
// Shoal-backed applications to Apache Accumulo.
//
// The current implementation targets the Accumulo 4 protocol and discovery
// layout. Scanner, writer, and administration APIs will build on these stable
// instance, credential, connector, and transport-lifecycle boundaries.
package accumulo

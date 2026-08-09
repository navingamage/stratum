// Package buildinfo carries version metadata stamped in at link time.
package buildinfo

import "fmt"

// Populated via -ldflags -X at build time; see the Makefile.
var (
	Version = "dev"
	Commit  = "none"
)

// String renders a human-readable build identifier.
func String() string {
	return fmt.Sprintf("stratum %s (%s)", Version, Commit)
}

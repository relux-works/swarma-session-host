//go:build unix

package sessiongoal

// The transport port is Unix-specific; the pure proof is independent of it.
var _ Protocol8RevisionProof = LegacyProof{}

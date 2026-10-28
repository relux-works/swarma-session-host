// Package musebuilds discovers the installed Muse builds and selects the
// newest one. This package owns discovery of the installed set (L1R) and newest ordering
// (L1D), host admission/publication (L1Q/L1F), first probe (L1B) and durable
// last-good (L1L). Reviewed cleanup and surface launch wiring remain separate.
//
// Discovery is read-only. It resolves the self-updating `muse` launcher on
// the supplied PATH, follows the launcher to its authoritative install
// directory, reads the `.muse-version` sidecar, enumerates the versioned
// `muse-bin-` namespace and selects the numerical maximum BEFORE any
// failure-record, permission or policy filtering. A BAD newest is a recorded
// start failure for the section-5 controller; it is never a silent skip to
// the next-newest here, because selection takes no failure input at all.
//
// Identity comes from the module-owned grammar
// (skill-agents-management/pkg/agentic/systems/muse): ParseBuildID for the
// sidecar text, ParsePinnedBinaryName for install-directory names and
// CompareBuildIDs for ordering. This package holds no second spelling of
// what a build is. The module's internal launch-environment package is not
// importable here, so PATH resolution mirrors its effective semantics
// (last PATH wins, absent PATH refuses, empty elements mean the working
// directory) without borrowing the process's ambient PATH. Relative PATH
// entries resolve against a cwd captured once per invocation and are absolutized
// before symlink resolution, so every reported root is absolute.
//
// The selected source is validated and hashed through ONE pinned descriptor:
// the name gate binds the path to the identity, the descriptor is verified
// to be that path, the static gates attest the descriptor, and the digest is
// read from it; the descriptor stays open through the consistency recheck.
// The same snapshot owns the directory graph, launcher, selector, selector
// target, inventory and source FDs until acceptance or refusal. The recheck
// uses fstat, descriptor-relative name checks, the held inventory/selector and
// a second hash of the SAME source FD, with no path reopen. Regular files
// must agree fully (dev/ino, size, mtime/ctime, mode, owner, link count);
// held directories compare by object identity (dev/ino, mode, owner), since
// their stamps move on any unrelated entry change. One disagreement restarts.
//
// Data opens use O_NOFOLLOW|O_NONBLOCK; only fstat-verified regular files are
// read. Directory search handles preserve execute-only traversal; enumeration
// has a separate readable handle. Context is checked around every filesystem
// operation and each bounded read/hash chunk, including the second hash.
// Reads stop at the initial fstat size; growth cannot extend their budget.
// Cancellation never starts another discovery step; descriptor cleanup still
// runs. Darwin and Linux are supported; other platforms refuse before I/O.
//
// Design §2 step 6 makes accepted SnapshotID the selection boundary. Changes
// after that observation affect the next invocation; freeze must verify again.
// This is an optimistic snapshot, not a filesystem transaction or protection
// against an active same-account attacker. In-progress kernel-uninterruptible
// I/O cannot be preempted by Go context (the §7/G1r3 platform bound). No worker
// goroutine is abandoned to simulate cancellation. Unrelated entries in a
// shared ancestor are not a disagreement: directory stamps are not compared,
// while every relied-upon name and the install inventory still are.
//
// Discovery never executes an installed candidate. The no-execution behavioral
// test proves sentinel installations are resolved but not run. Only the first
// probe runner executes, after verifying a protected frozen descriptor.
//
// Pass 2a adds host admission (L1Q) and freeze publication (L1F) behind
// PublishFrozen: dedupe first, then the global mini-build-lock with
// ramp CLEAR read under it, a durable O_EXCL day reservation, and a
// verified copy published as COPIED with its authoritative state.
// The lock shell-out lives in the minilock subpackage so discovery's
// no-exec guarantee keeps holding for this package. The first probe,
// BAD minting and last-good belong to pass 2b; PublishFrozen never
// runs a copy it published.
//
// Pass 2b adds probe state (L1B) and the last-good receipt (L1L). ProbeRunner
// orchestrates Freeze and ProbeFrozen via an injectable owned-group launcher
// in probeprocess, with bounded capture/drain/reap and a sticky cleanup gate.
// ProbeFrozen binds execution to the invocation's accepted newest discovery;
// a differing tuple is refused before child admission, and any non-newest
// execution requires the recorded failure-to-exact-last-good permit consumed
// by Recover (design section 5).
// RecordProbeOutcome classifies the observation and records
// it: success transitions COPIED to PROBE_OK, a deterministic
// identity contradiction transitions COPIED to BAD, and every
// transient failure records an attempt receipt without touching the
// lifecycle state. WriteLastGood admits only an observed successful
// startup, allocating the next admission sequence under the metadata
// lock and publishing atomically. ReadBuildState, BadRecord,
// ReadAttemptReceipt and ReadLastGood serve the section-5 recovery
// controller; every file is written FD-relative under the held store
// root. All three documents share one encode/validate codec with their reader.
// ProbeRunner.Recover drives an invocation-bound offer and one exact last-good
// attempt through StartOutcomeReporter. The L2/L3 consumers still own actual
// MSP/PTY startup observation and warning delivery; a failed fallback never
// clears the durable last-good receipt.
//
// L2C adds the shared recovery controller in launch.go: PrepareNewestLaunch
// selects newest on every route and classifies freeze/probe outcomes,
// consumer Report calls bind the newest leg to a durable attempt receipt,
// RequestPermit re-verifies that evidence and issues the invocation's one
// opaque fallback permit with its durable warning, and the permit
// authorizes fallback probes under leases plus exactly one
// exact-last-good leg once no probe is outstanding. Host-cap deferral
// is the second explicit non-newest route (Amendment 2, keeper ruling
// $1F-4jRKQ option A): it runs under its own authorization, never a
// permit, only when the host cap is configured and tripped, for the
// newest already-frozen PROBE_OK target with the typed host-cap
// warning, an interactive offer on interactive legs, and a trace
// record; it never runs an unfrozen build.
package musebuilds

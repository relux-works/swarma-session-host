package sessioncontext

import (
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"time"
)

type ContextProvider = sm.ContextProvider
type ContextRecord = sm.ContextRecord
type ContextState = sm.ContextState
type ContextStateReason = sm.StateReason
type ContextResolutionMode = sm.ContextResolutionMode
type ContextRetentionPolicy = sm.RetentionPolicy
type ContextRetentionRisk = sm.ContextRetentionRisk
type ContextRetentionOutlook = sm.ContextRetentionOutlook

const ContextProviderCodex ContextProvider = "codex"
const ContextProviderClaude ContextProvider = "claude"
const ContextResolutionAuto ContextResolutionMode = "auto"
const ContextStateReady ContextState = "ready"
const ContextReasonPayloadVanished ContextStateReason = "payload_vanished"
const ContextReasonProviderIncompatible ContextStateReason = "provider_incompatible"
const ContextErrorPayloadVanished = "context_payload_vanished"
const ContextErrorPayloadUnverified = "context_payload_unverified"
const ContextErrorProviderIncompatible = "context_provider_incompatible"
const ContextErrorContractInvalid = "context_contract_invalid"
const contextTextLimit = 512

func NormalizeContextID(s string) string        { return normalizeContextID(s) }
func NormalizeContextElementID(s string) string { return normalizeElementID(s) }
func NormalizeContextRole(s string) string      { return normalizeRole(s) }
func contextError(c, m string, e error) error   { return &ContextError{Code: c, Message: m, Cause: e} }
func retentionNow(p ContextRetentionPolicy) time.Time {
	if p.Now.IsZero() {
		return time.Now().UTC()
	}
	return p.Now.UTC()
}
func retentionWarnWindow(p ContextRetentionPolicy) time.Duration {
	if p.WarnWindow > 0 {
		return p.WarnWindow
	}
	return DefaultContextRetentionWarnWindow
}
func IsContextErrorCode(e error, c string) bool { return isContextErrorCode(e, c) }

const ContextPayloadStateAvailable sm.ContextPayloadState = "available"

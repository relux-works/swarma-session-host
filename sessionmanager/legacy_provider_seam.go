package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

// DecodeProviderHost converts the actual historical {provider,opaque} subrecord.
// Unsupported active native state is refused exactly as in DecodeLegacy; this
// is not authorization to materialize or restore the returned execution facts.
func (c *LegacyRecordCodec) DecodeProviderHost(ctx context.Context, raw []byte) (ProviderHostRecord, error) {
	if e := ctx.Err(); e != nil {
		return ProviderHostRecord{}, e
	}
	var wire legacyProviderImage
	if e := decodeLegacyJSON(raw, &wire); e != nil {
		return ProviderHostRecord{}, e
	}
	snapshot, e := c.decodeSnapshot(wire, false)
	if e != nil {
		return ProviderHostRecord{}, e
	}
	out := ProviderHostRecord{Provider: wire.Provider, Snapshot: snapshot}
	if e = validateRunnerSnapshot(out); e != nil {
		return ProviderHostRecord{}, e
	}
	return out, nil
}

// EncodeProviderHost merges against the actual original subrecord, retaining
// unknown/provider-private fields and unchanged bytes. A nil original creates
// the actual legacy wire shape from a complete admitted typed snapshot.
func (c *LegacyRecordCodec) EncodeProviderHost(ctx context.Context, original []byte, record ProviderHostRecord) ([]byte, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	var prior *legacyProviderImage
	if len(original) > 0 {
		if _, e := c.DecodeProviderHost(ctx, original); e != nil {
			return nil, e
		}
		prior = &legacyProviderImage{}
		if e := decodeLegacyJSON(original, prior); e != nil {
			return nil, e
		}
	}
	opaque, e := c.encodeSnapshot(record, prior)
	if e != nil {
		return nil, e
	}
	out := legacyProviderImage{Provider: record.Provider, Opaque: opaque}
	var raw []byte
	if prior != nil {
		raw, e = encodeLegacySnapshot(original, *prior, out)
	} else {
		raw, e = json.Marshal(out)
	}
	if e != nil {
		return nil, e
	}
	if _, e = c.DecodeProviderHost(ctx, raw); e != nil {
		return nil, e
	}
	return raw, nil
}

// LegacyMuseAccountingSnapshot pairs the actual native accounting field group
// with the original native identity/location/transport. The consumer must bind
// these facts to its verified current lease; reading them establishes no lease.
type LegacyMuseAccountingSnapshot struct {
	SessionID     string `json:"sessionId"`
	WorkspaceRoot string `json:"workspaceRoot,omitempty"`
	GoalTransport string `json:"goal_transport"`
	LegacyMuseNativeAccounting
}

func (c *LegacyRecordCodec) museAccountingWire(ctx context.Context, raw []byte) (legacyProviderImage, legacyMuseSnapshot, error) {
	if e := ctx.Err(); e != nil {
		return legacyProviderImage{}, legacyMuseSnapshot{}, e
	}
	var wire legacyProviderImage
	if e := decodeLegacyJSON(raw, &wire); e != nil {
		return wire, legacyMuseSnapshot{}, e
	}
	if wire.Provider != "muse" {
		return wire, legacyMuseSnapshot{}, ErrLegacyCodecMismatch
	}
	// Reuse the real full union and original validators in data-only archival
	// mode. No RunnerRecord or restore admission is returned by this API.
	snapshot, e := c.decodeSnapshot(wire, true)
	if e != nil {
		return wire, legacyMuseSnapshot{}, e
	}
	if snapshot.Kind != "muse-msp" || snapshot.MuseMSP.GoalTransport != "native" {
		return wire, legacyMuseSnapshot{}, fmt.Errorf("%w: native Muse accounting required", ErrHostCapabilityUnavailable)
	}
	var value legacyMuseSnapshot
	if e = decodeLegacyJSON(wire.Opaque, &value); e != nil {
		return wire, value, e
	}
	return wire, value, nil
}

// DecodeMuseAccounting exposes original native accounting for its real supplier.
// Text-child accounting remains in the original image, outside this field group.
func (c *LegacyRecordCodec) DecodeMuseAccounting(ctx context.Context, raw []byte) (LegacyMuseAccountingSnapshot, error) {
	_, value, e := c.museAccountingWire(ctx, raw)
	if e != nil {
		return LegacyMuseAccountingSnapshot{}, e
	}
	return LegacyMuseAccountingSnapshot{SessionID: value.SessionID, WorkspaceRoot: value.WorkspaceRoot, GoalTransport: value.GoalTransport, LegacyMuseNativeAccounting: value.LegacyMuseNativeAccounting}, nil
}

// EncodeMuseAccounting updates only the native accounting field group against
// the original image. Identity/transport changes fail; all other known/unknown
// state is retained. This method has no disk, native process or ACK effects.
func (c *LegacyRecordCodec) EncodeMuseAccounting(ctx context.Context, raw []byte, state LegacyMuseAccountingSnapshot) ([]byte, error) {
	wire, before, e := c.museAccountingWire(ctx, raw)
	if e != nil {
		return nil, e
	}
	if state.SessionID != before.SessionID || state.WorkspaceRoot != before.WorkspaceRoot || state.GoalTransport != before.GoalTransport {
		return nil, ErrLegacyViewStale
	}
	after := before
	after.LegacyMuseNativeAccounting = state.LegacyMuseNativeAccounting
	if e = validateLegacyMuseSnapshot(&after); e != nil {
		return nil, e
	}
	opaque, e := encodeLegacySnapshot(wire.Opaque, before, after)
	if e != nil {
		return nil, e
	}
	if bytes.Equal(opaque, wire.Opaque) {
		return append([]byte(nil), raw...), nil
	}
	next := wire
	next.Opaque = opaque
	updated, e := encodeLegacySnapshot(raw, wire, next)
	if e != nil {
		return nil, e
	}
	if _, _, e = c.museAccountingWire(ctx, updated); e != nil {
		return nil, e
	}
	return updated, nil
}

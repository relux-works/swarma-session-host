package hostdefaults

import (
	"context"
	"fmt"

	"github.com/relux-works/swarma-session-host/sessioncontext"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// NewMaterializationHostCodec connects the context catalog's historical
// provider-host reader and writer to the actual record codec. Both the request
// and host lifetime bound conversion. This grants no materialization authority
// or native restore.
func NewMaterializationHostCodec(life context.Context, codec *sm.LegacyRecordCodec) (sessioncontext.MaterializationHostCodec, error) {
	if life == nil || codec == nil {
		return nil, fmt.Errorf("materialization codec requires lifetime context and legacy record codec")
	}
	return materializationHostCodec{life: life, codec: codec}, nil
}

type materializationHostCodec struct {
	life  context.Context
	codec *sm.LegacyRecordCodec
}

func (c materializationHostCodec) conversionContext(ctx context.Context) (context.Context, func(), error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("materialization conversion requires request context")
	}
	if err := c.life.Err(); err != nil {
		return nil, nil, err
	}
	joined, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.life, cancel)
	return joined, func() { stop(); cancel() }, nil
}

func (c materializationHostCodec) DecodeProviderHost(ctx context.Context, raw []byte) (sm.ProviderHostRecord, error) {
	ctx, release, err := c.conversionContext(ctx)
	if err != nil {
		return sm.ProviderHostRecord{}, err
	}
	defer release()
	return c.codec.DecodeProviderHost(ctx, raw)
}

func (c materializationHostCodec) EncodeProviderHost(ctx context.Context, raw []byte, record sm.ProviderHostRecord) ([]byte, error) {
	ctx, release, err := c.conversionContext(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return c.codec.EncodeProviderHost(ctx, raw, record)
}

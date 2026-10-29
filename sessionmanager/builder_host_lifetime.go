package sessionmanager

import "fmt"

// BuilderGatewayLifecycle is the lifetime subset of the actual selected
// BuilderGatewayRuntime. It supplies neither launch/holder authority nor an
// enforcement claim. The same runtime must be configured in the catalog.
type BuilderGatewayLifecycle interface{ Close() error }

type hostBuilderClose struct {
	done chan struct{}
	err  error
}

// Caller holds mu. Gateway finalization can read the actual native observer,
// so it runs outside the map lock and before that observation is closed.
// Preserve this one-shot operation's actual result: an empty runtime on retry
// cannot turn an earlier failed completeness observation into a healthy handoff.
func (h *SessionHost) closeBuilderGatewaysLocked() error {
	if absentHostPort(h.options.BuilderGateways) {
		return nil
	}
	if prior := h.builderClose; prior != nil {
		h.mu.Unlock()
		<-prior.done
		h.mu.Lock()
		return prior.err
	}
	result := &hostBuilderClose{done: make(chan struct{})}
	h.builderClose = result
	result.err = h.callNativeAccountingUnlocked(func() error { return h.options.BuilderGateways.Close() })
	close(result.done)
	return result.err
}

// BuilderGatewayCloseError reports this host's actual joined gateway result.
// The executable owner calls it after Host.Close instead of closing the runtime
// first and losing its original result. An unstarted/in-flight close is explicit.
func (h *SessionHost) BuilderGatewayCloseError() error {
	if h == nil {
		return ErrHostClosed
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if absentHostPort(h.options.BuilderGateways) {
		return nil
	}
	if result := h.builderClose; result != nil {
		select {
		case <-result.done:
			return result.err
		default:
		}
	}
	return fmt.Errorf("%w: builder gateway close has not joined", ErrHostCapabilityUnavailable)
}

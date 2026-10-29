package sessionmanager

// NativeIdentity is a read-only provider observation, not managed session or
// caller authority. Keep it on the actual writer/backend so fresh and staged
// goal delivery need not re-enter the host's closed publication reservation.
func (h *sessionInput) NativeIdentity() string {
	h.mu.Lock()
	lease := ControllerLease{SessionID: h.sessionID, Epoch: h.epoch}
	backend := h.backend
	h.mu.Unlock()
	native, ok := backend.(ProviderNativeIdentity)
	if !ok || absentHostPort(native) || h.requireLease(h.lifeCtx, lease) != nil {
		return ""
	}
	id := native.NativeIdentity()
	if h.requireLease(h.lifeCtx, lease) != nil {
		return ""
	}
	return id
}
func (w *attachmentWriter) NativeIdentity() string {
	w.mu.Lock()
	active := w.active && w.life.Err() == nil
	parent := w.parent
	w.mu.Unlock()
	if !active {
		return ""
	}
	native, ok := parent.(ProviderNativeIdentity)
	if !ok || absentHostPort(native) {
		return ""
	}
	id := native.NativeIdentity()
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.active || w.life.Err() != nil {
		return ""
	}
	return id
}

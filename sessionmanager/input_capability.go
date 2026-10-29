package sessionmanager

// SupportsNativeGoals is actual provider capability metadata, not admission.
// Every operation still needs the real current controller and scoped writer.
func (h *sessionInput) SupportsNativeGoals() bool { return h.backend.SupportsNativeGoals() }
func (w *attachmentWriter) SupportsNativeGoals() bool {
	if actual, ok := w.parent.(interface{ SupportsNativeGoals() bool }); ok {
		return actual.SupportsNativeGoals()
	}
	// Unknown preserves the pre-existing native/refusal path. It must not be
	// misreported as known unsupported to bypass required native hydration.
	return true
}

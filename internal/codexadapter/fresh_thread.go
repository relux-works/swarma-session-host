package codexadapter

// FreshThread observes parser-established initialize/thread-start correlation.
// A restored checkpoint never supplies fresh create evidence, even after replay.
func (d *Driver) FreshThread() (string, bool) {
	if d == nil || d.stream == nil {
		return "", false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	v := d.stream.Snapshot()
	return v.Thread, d.begun && !d.restored && d.atTail && v.Thread != "" && (v.State == Idle || v.State == Busy) && !v.AuthorityRequired
}

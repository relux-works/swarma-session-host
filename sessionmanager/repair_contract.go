package sessionmanager

// RepairReport is the secret-free result returned by the session doctor and recorded for the automatic startup sweep.
type RepairReport struct {
	OrphanProcessesTerminated int `json:"orphan_processes_terminated"`
	LiveProcessesPreserved    int `json:"live_processes_preserved"`
	UnsafeProcessesPreserved  int `json:"unsafe_processes_preserved"`
	StaleSocketsRemoved       int `json:"stale_sockets_removed"`
	LiveSocketsPreserved      int `json:"live_sockets_preserved"`
	StaleEndpointsRemoved     int `json:"stale_endpoints_removed"`
	LiveEndpointsPreserved    int `json:"live_endpoints_preserved"`
	// StaleAttachmentDirsRemoved counts empty attachments-<hex> directories
	// that belonged to no live attachment when the sweep ran.
	StaleAttachmentDirsRemoved  int `json:"stale_attachment_dirs_removed"`
	LiveAttachmentDirsPreserved int `json:"live_attachment_dirs_preserved"`
}

type DoctorReport struct {
	Startup RepairReport `json:"startup"`
	Current RepairReport `json:"current"`
}

func (r DoctorReport) Total() RepairReport {
	total := r.Startup
	total.merge(r.Current)
	return total
}

func (r RepairReport) Repairs() int {
	return r.OrphanProcessesTerminated +
		r.StaleSocketsRemoved +
		r.StaleEndpointsRemoved +
		r.StaleAttachmentDirsRemoved
}

func (r *RepairReport) merge(other RepairReport) {
	r.OrphanProcessesTerminated += other.OrphanProcessesTerminated
	r.LiveProcessesPreserved += other.LiveProcessesPreserved
	r.UnsafeProcessesPreserved += other.UnsafeProcessesPreserved
	r.StaleSocketsRemoved += other.StaleSocketsRemoved
	r.LiveSocketsPreserved += other.LiveSocketsPreserved
	r.StaleEndpointsRemoved += other.StaleEndpointsRemoved
	r.LiveEndpointsPreserved += other.LiveEndpointsPreserved
	r.StaleAttachmentDirsRemoved += other.StaleAttachmentDirsRemoved
	r.LiveAttachmentDirsPreserved += other.LiveAttachmentDirsPreserved
}

package sessionhost

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	managerschema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
)

type operationNotification struct {
	Contract string `json:"contract"`
	Backend  string `json:"backend"`
	Workflow struct {
		Reference string `json:"reference"`
		SHA256    string `json:"sha256"`
	} `json:"workflow"`
	memberNotificationInput
}

type memberOperationRequest struct {
	Operation string `json:"operation_id"`
	Runtime   string `json:"runtime"`
	Profile   struct {
		Name           string `json:"name"`
		Lock           string `json:"lock_sha256"`
		FragmentDigest string `json:"fragment_digest"`
	} `json:"profile"`
	Cwd   string `json:"cwd"`
	Scope struct {
		Cwd string `json:"cwd"`
	} `json:"scope"`
	AdmissionKey *string               `json:"admission_key"`
	Doorbell     json.RawMessage       `json:"doorbell"`
	Notification operationNotification `json:"notification"`
}

// A retained operation is an actual record/request/workspace correlation, not
// full manager authority. Its workflow comes from the incoming Core projection;
// this type cannot attest Core origin, resolve a profile or activate a broker.
// It deliberately does not implement notificationManagerCustody.
type retainedMemberOperation struct {
	record            *retainedMemberRecord
	request           memberOperationRequest
	kind              string
	digest            [32]byte
	workspace         *os.File
	workspaceIdentity os.FileInfo
	workspacePath     string
	mu                sync.Mutex
	lost              bool
}

func parseMemberOperation(kind string, data []byte) (memberOperationRequest, error) {
	var q memberOperationRequest
	if (kind != "launch" && kind != "resume") || managerschema.Validate(kind+".schema.json", data) != nil {
		return q, errMemberRecordUnavailable
	}
	if _, err := managerschema.CanonicalJSON(data); err != nil {
		return q, errMemberRecordUnavailable
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(data, &object) != nil || object == nil {
		return q, errMemberRecordUnavailable
	}
	raw, present := object["notification"]
	if !present || json.Unmarshal(data, &q) != nil {
		return memberOperationRequest{}, errMemberRecordUnavailable
	}
	var shape map[string]json.RawMessage
	if json.Unmarshal(raw, &shape) != nil || len(shape) != 6 {
		return memberOperationRequest{}, errMemberRecordUnavailable
	}
	for _, key := range []string{"contract", "backend", "workflow", "member", "admission_reference", "call_timeout_ms"} {
		if shape[key] == nil {
			return memberOperationRequest{}, errMemberRecordUnavailable
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&q.Notification) != nil || q.Notification.Contract != NotificationContract || q.Notification.Backend != "host" || !validMemberInput(q.Notification.memberNotificationInput) {
		return memberOperationRequest{}, errMemberRecordUnavailable
	}
	var workflow, member map[string]json.RawMessage
	if json.Unmarshal(shape["workflow"], &workflow) != nil || len(workflow) != 2 || workflow["reference"] == nil || workflow["sha256"] == nil || !scalar.MatchString(q.Notification.Workflow.Reference) || !notificationSHA256(q.Notification.Workflow.SHA256) {
		return memberOperationRequest{}, errMemberRecordUnavailable
	}
	if json.Unmarshal(shape["member"], &member) != nil || len(member) != 4 {
		return memberOperationRequest{}, errMemberRecordUnavailable
	}
	for _, key := range []string{"mxid", "agent_selector", "connection", "configuration_root"} {
		if member[key] == nil {
			return memberOperationRequest{}, errMemberRecordUnavailable
		}
	}
	if !bytes.Equal(bytes.TrimSpace(q.Doorbell), []byte("null")) {
		return memberOperationRequest{}, errMemberRecordUnavailable
	}
	return q, nil
}

// The only lookup source is the already retained composition-selected directory.
// Neither station selection nor plain --member changes ordinary admission_key.
func (s *memberRecordSource) retainOperation(kind string, data []byte) (*retainedMemberOperation, error) {
	q, err := parseMemberOperation(kind, data)
	if err != nil {
		return nil, err
	}
	r, err := s.retain(q.Notification.AdmissionReference)
	if err != nil {
		return nil, err
	}
	fail := func() (*retainedMemberOperation, error) { r.close(); return nil, errMemberRecordUnavailable }
	if r.record.Runtime != q.Runtime || r.record.Profile != q.Profile.Name || r.record.ProfileLock != q.Profile.Lock || r.record.Notification == nil || *r.record.Notification != q.Notification.memberNotificationInput || !bytes.Equal(bytes.TrimSpace(r.record.Doorbell), []byte("null")) {
		return fail()
	}
	if q.AdmissionKey != nil && *q.AdmissionKey != "werkstatt:"+r.record.ID {
		return fail()
	}
	requestPath := q.Cwd
	if kind == "resume" {
		requestPath = q.Scope.Cwd
	}
	clone, err := filepath.EvalSymlinks(r.record.Clone)
	if err != nil {
		return fail()
	}
	actual, err := filepath.EvalSymlinks(requestPath)
	if err != nil || !filepath.IsAbs(actual) || actual != clone {
		return fail()
	}
	f, err := os.Open(clone)
	if err != nil {
		return fail()
	}
	info, err := f.Stat()
	if err != nil || !info.IsDir() {
		_ = f.Close()
		return fail()
	}
	s.mu.Lock()
	if r.lost || s.closed {
		s.mu.Unlock()
		_ = f.Close()
		return fail()
	}
	r.workspace = f
	s.mu.Unlock()
	o := &retainedMemberOperation{record: r, request: q, kind: kind, digest: sha256.Sum256(data), workspace: f, workspaceIdentity: info, workspacePath: requestPath}
	if o.recheckPhysical(s, kind, data) != nil {
		return fail()
	}
	return o, nil
}

// Rechecks exact operation bytes as well as current independently retained
// origin. Canonical-equivalent replacement input is still a different request.
func (o *retainedMemberOperation) recheck(owner *continuingHostOwner, source *memberRecordSource, kind string, data []byte) error {
	if owner == nil || source == nil || owner != source.owner {
		return errMemberRecordUnavailable
	}
	return o.recheckPhysical(source, kind, data)
}

func (o *retainedMemberOperation) recheckPhysical(source *memberRecordSource, kind string, data []byte) error {
	if o == nil || o.record == nil || o.workspace == nil || source == nil || source != o.record.source {
		return errMemberRecordUnavailable
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.lost || kind != o.kind || sha256.Sum256(data) != o.digest {
		return errMemberRecordUnavailable
	}
	if o.record.recheckPhysical(source) == nil {
		held, err := o.workspace.Stat()
		clone, e1 := filepath.EvalSymlinks(o.record.record.Clone)
		actual, e2 := filepath.EvalSymlinks(o.workspacePath)
		named, e3 := os.Stat(actual)
		if err == nil && e1 == nil && e2 == nil && e3 == nil && actual == clone && held.IsDir() && named.IsDir() && os.SameFile(o.workspaceIdentity, held) && os.SameFile(held, named) {
			return nil
		}
	}
	o.lost = true
	return errMemberRecordUnavailable
}

func (o *retainedMemberOperation) close() {
	if o != nil && o.record != nil {
		o.record.close()
	}
}

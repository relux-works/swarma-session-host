package sessionmanager

import "encoding/json"

const MuseSessionLaunchContract = "task-board.muse-session-launch-v1"

type MuseBinarySelection string

type MuseSessionLaunch struct {
	Contract        string              `json:"contract"`
	Model           string              `json:"model"`
	Effort          string              `json:"effort,omitempty"`
	EffortRequired  bool                `json:"effort_required,omitempty"`
	WorkDir         string              `json:"work_dir"`
	Env             []string            `json:"env"`
	Yolo            bool                `json:"yolo,omitempty"`
	ToolRelease     string              `json:"tool_release,omitempty"`
	OperatorArgs    []string            `json:"operator_args,omitempty"`
	BinarySelection MuseBinarySelection `json:"binary_selection,omitempty"`
	Fragment        json.RawMessage     `json:"fragment"`
	FragmentSHA256  string              `json:"fragment_sha256"`
	Profile         string              `json:"profile"`
	ProfileLock     string              `json:"profile_lock_sha256"`
	ManagedHome     string              `json:"managed_home"`
	CuratorPath     string              `json:"curator_path,omitempty"`
	CuratorVersion  string              `json:"curator_version,omitempty"`
	Term            string              `json:"term,omitempty"`
	Rows            uint16              `json:"rows,omitempty"`
	Cols            uint16              `json:"cols,omitempty"`
}

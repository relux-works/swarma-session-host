package sessionhost

// ControlOperation reserves host operations independently of any adapter's
// native-goal support. Goal operations currently always return unsupported.
type ControlOperation string

const (
	GoalSet   ControlOperation = "goal.set"
	GoalClear ControlOperation = "goal.clear"
)

// CapabilityName is a declaration, never a grant or caller authentication.
type CapabilityName string

const GoalProvider CapabilityName = "goal-provider"

// GoalControl carries only a reference. No goal service or authority is active.
// Future set/clear requires a session-scoped goal-provider grant held by the
// launching caller or another granted provider. Agents never set their own goal;
// the authority and namespace of agent self-requests remain undefined.
type GoalControl struct {
	Reference string `json:"reference,omitempty"`
}

package sessionmanager

import (
	"errors"
	"testing"
	"time"
)

// Admission binds an observation to the exact live session/epoch and refuses
// unbound/stale epochs. This checks the pure admission predicate; writer-fence
// ordering, actual provider reads and I/O require the writer integration tests.
func TestNativeGoalObservationRejectsStaleOrUnboundEpoch(t *testing.T) {
	lease := ControllerLease{SessionID: "native-session", Epoch: 3}
	current := NativeGoalObservation{SessionID: lease.SessionID, Epoch: lease.Epoch, Objective: "native objective", Status: "active", Sequence: 7, ObservedAt: time.Unix(100, 0)}
	if err := validateNativeGoalObservation(lease, current); err != nil {
		t.Fatalf("bound positive control: %v", err)
	}
	for _, tc := range []struct {
		name    string
		session string
		epoch   ControllerEpoch
	}{
		{"stale_epoch", lease.SessionID, 2}, {"observation_without_epoch_binding", lease.SessionID, 0}, {"other_session", "other-session", lease.Epoch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed := current
			observed.SessionID = tc.session
			observed.Epoch = tc.epoch
			if err := validateNativeGoalObservation(lease, observed); !errors.Is(err, ErrStaleInputController) {
				t.Fatalf("wrong refusal: %v", err)
			}
		})
	}
	for _, tc := range []struct {
		name     string
		observed NativeGoalObservation
	}{
		{"no_sequence", NativeGoalObservation{SessionID: lease.SessionID, Epoch: lease.Epoch, ObservedAt: current.ObservedAt}},
		{"no_observation_time", NativeGoalObservation{SessionID: lease.SessionID, Epoch: lease.Epoch, Sequence: current.Sequence}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateNativeGoalObservation(lease, tc.observed); !errors.Is(err, ErrNativeGoalObservationUnavailable) {
				t.Fatalf("wrong absence refusal: %v", err)
			}
		})
	}
}

// The typed union admits each concrete command, with pause tied to the host's
// bound observation sequence; missing/ambiguous variants and stale pauses are
// refused. Native status never selects a command or becomes canonical policy.
func TestNativeMuseCommandAdmission(t *testing.T) {
	lease := ControllerLease{SessionID: "native-session", Epoch: 3}
	observed := NativeGoalObservation{SessionID: lease.SessionID, Epoch: lease.Epoch, Objective: "native objective", Status: "active", Sequence: 7, ObservedAt: time.Unix(100, 0)}
	for _, command := range []*MuseGoalCommand{
		{CommandID: "set", Set: &MuseGoalSet{Objective: "new"}}, {CommandID: "edit", Edit: &MuseGoalEdit{Objective: "changed"}},
		{CommandID: "pause", Pause: &MuseGoalPause{Sequence: observed.Sequence}}, {CommandID: "clear", Clear: &MuseGoalClear{}},
	} {
		if err := validateMuseGoalCommand(lease, &observed, command); err != nil {
			t.Fatalf("valid command %#v: %v", command, err)
		}
	}
	for _, tc := range []struct {
		name        string
		command     *MuseGoalCommand
		observation *NativeGoalObservation
		want        error
	}{
		{"missing_command", nil, &observed, ErrInvalidMuseGoalCommand},
		{"no_command_id", &MuseGoalCommand{Clear: &MuseGoalClear{}}, &observed, ErrInvalidMuseGoalCommand},
		{"no_variant", &MuseGoalCommand{CommandID: "x"}, &observed, ErrInvalidMuseGoalCommand},
		{"multiple_variants", &MuseGoalCommand{CommandID: "x", Set: &MuseGoalSet{Objective: "new"}, Pause: &MuseGoalPause{Sequence: 7}}, &observed, ErrInvalidMuseGoalCommand},
		{"empty_set", &MuseGoalCommand{CommandID: "x", Set: &MuseGoalSet{}}, &observed, ErrInvalidMuseGoalCommand},
		{"empty_edit", &MuseGoalCommand{CommandID: "x", Edit: &MuseGoalEdit{}}, &observed, ErrInvalidMuseGoalCommand},
		{"pause_without_observation", &MuseGoalCommand{CommandID: "x", Pause: &MuseGoalPause{Sequence: 7}}, nil, ErrNativeGoalObservationUnavailable},
		{"pause_changed_sequence", &MuseGoalCommand{CommandID: "x", Pause: &MuseGoalPause{Sequence: 6}}, &observed, ErrNativeGoalChanged},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateMuseGoalCommand(lease, tc.observation, tc.command); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
	stale := observed
	stale.Epoch--
	if err := validateMuseGoalCommand(lease, &stale, &MuseGoalCommand{CommandID: "x", Pause: &MuseGoalPause{Sequence: 7}}); !errors.Is(err, ErrStaleInputController) {
		t.Fatalf("stale pause = %v", err)
	}
}

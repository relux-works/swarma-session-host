package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
	curatorenv "github.com/relux-works/swarma-session-host/sessionmanager/runner/environment"
)

const CodexErrorCuratorHomeCopy = "codex_curator_home_copy_refused"

func refuseCodexHomeCopy(profile string, cause error) error {
	var credentialErr *curatorenv.CredentialError
	if errors.As(cause, &credentialErr) {
		return &curatorIdentityReadError{public: &PublicError{Code: curatorenv.CredentialConflictCode, Message: "credential conflict requires operator action"}, cause: cause}
	}
	var nativeErr *curatorenv.Error
	if errors.As(cause, &nativeErr) {
		return &curatorIdentityReadError{public: &PublicError{Code: curatorenv.NativeHomeUnknown, Message: curatorenv.NativeHomeGuidance}, cause: cause}
	}

	return &curatorIdentityReadError{public: &PublicError{
		Code:    CodexErrorCuratorHomeCopy,
		Message: fmt.Sprintf("private Codex home copy refused for Curator profile %q", profile),
	}, cause: cause}
}

type codexHomeItem struct {
	source, name string
	optional     bool
}

func prepareCuratorCodexHome(ctx context.Context, sessionDir string, plan *PrimarySessionLaunchPlan, builder *BuilderConfinementRequest, policy ClientToolPolicy) (string, *CodexCuratorHomeCopy, error) {
	selected, present, err := extractCuratorProvenance(plan.Sources)
	if err != nil {
		return "", nil, refuseCodexHomeCopy("unknown", err)
	}
	if !present {
		home, err := preparePrivateCodexHome(sessionDir, plan.ProjectDir, builder, policy)
		return home, nil, err
	}
	profile := selected.ProfileName
	refuse := func(err error) (string, *CodexCuratorHomeCopy, error) {
		return "", nil, refuseCodexHomeCopy(profile, err)
	}
	if selected.Fragment.Environment != "codex_cli" || selected.Fragment.SystemPrompt != nil {
		return refuse(errors.New("unsupported Curator home channel"))
	}
	// Interim: remove after Curator rc.7 (env materialize --into).
	// Curator homes are mutable: lock/fragment equality alone cannot detect a
	// concurrent skill sync, so resolve brackets a regular-file copy AND a byte
	// comparison. Enumeration here implements copying, never content policy.
	for attempt := 0; attempt < 3; attempt++ {
		before, digest, err := resolveCodexHomeFragment(ctx, plan.ProjectDir, selected)
		if err != nil {
			return refuse(err)
		}
		// Selection and capabilities cannot change underneath an already composed
		// plan. In particular a fresh locked grant must pass the host posture gate.
		freshPlan := *plan
		encoded, err := marshalCodexHomeProvenance(before)
		if err != nil {
			return refuse(err)
		}
		freshPlan.Sources = []LaunchPlanSource{{Kind: CuratorProvenanceSourceKind, Path: encoded}}
		if err := RefuseLockedCodexLaunch(&freshPlan); err != nil {
			return "", nil, err
		}
		if before.Fragment.Environment != "codex_cli" || before.ManagedHome != selected.ManagedHome ||
			before.Fragment.SystemPrompt != nil || before.Fragment.PathPrepend != "" ||
			!reflect.DeepEqual(before.Fragment.MCP, selected.Fragment.MCP) {
			return refuse(errors.New("Curator home selection or channel changed"))
		}
		if info, err := os.Stat(before.ManagedHome); err != nil || !info.IsDir() {
			return refuse(errors.New("Curator source home unavailable"))
		}
		items := []codexHomeItem{
			{filepath.Join(before.ManagedHome, "config.toml"), "config.toml", true},
			{filepath.Join(before.ManagedHome, "AGENTS.md"), "AGENTS.md", true},
			{filepath.Join(before.ManagedHome, "skills"), "skills", true},
		}
		if before.Fragment.MCP != nil {
			items = append(items, codexHomeItem{before.Fragment.MCP.Path, "curator-mcp.config.toml", false})
		}
		stage, err := os.MkdirTemp(sessionDir, ".curator-home-*")
		if err != nil {
			return refuse(err)
		}
		evidence, err := copyCodexHomeBracket(ctx, plan.ProjectDir, before, digest, items, stage)
		if err != nil {
			_ = os.RemoveAll(stage)
			return refuse(err)
		}
		if evidence == nil {
			_ = os.RemoveAll(stage)
			continue
		}
		privateConfig, err := renderPrivateCodexConfig(stage, plan.ProjectDir, builder, policy, before.Fragment.MCP != nil)
		if err == nil {
			// Keep native auth selection and all legacy setup in the existing helper.
			// Its config override uses the accepted profile-derived config.
			var privateHome string
			privateHome = filepath.Join(sessionDir, "codex-home")
			if info, statErr := os.Lstat(privateHome); statErr == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
				err = errors.New("private home is not a regular directory")
			} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
				err = statErr
			}
			if err == nil {
				// Atomic config writer must not follow a pre-existing output symlink.
				configPath := filepath.Join(privateHome, "config.toml")
				if info, statErr := os.Lstat(configPath); statErr == nil && info.Mode()&os.ModeSymlink != 0 {
					err = errors.New("private config is a symlink")
				}
			}
			if err == nil {
				err = validateCuratorCodexAuthDestination(privateHome)
			}
			if err == nil {
				privateHome, err = preparePrivateCodexHome(sessionDir, plan.ProjectDir, builder, policy, privateConfig)
			}
			if err == nil {
				err = publishCodexHomeCopy(stage, privateHome, privateConfig)
			}
			_ = os.RemoveAll(stage)
			if err != nil {
				return refuse(err)
			}
			return privateHome, evidence, nil
		}
		_ = os.RemoveAll(stage)
		return refuse(err)
	}
	return refuse(errors.New("Curator fragment or copied tree changed during all 3 attempts"))
}

// Curator launches keep only an existing direct link to native auth. A profile
// hop (even one resolving to native auth) or a regular file refuses through the
// profile-naming home-copy error before provider start, without reading,
// copying or replacing credentials. Absence retains the helper's native-link
// installation. No-fragment launches bypass this check and keep legacy behavior.
func validateCuratorCodexAuthDestination(privateHome string) error {
	targetAuth := filepath.Join(privateHome, "auth.json")
	info, err := os.Lstat(targetAuth)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return errors.New("private authentication destination is not a direct native link")
	}
	sourceHome, err := curatorenv.CodexNativeHome()
	if err != nil {
		return err
	}
	destination, err := os.Readlink(targetAuth)
	if err != nil {
		return err
	}
	if destination != filepath.Join(sourceHome, "auth.json") {
		return errors.New("private authentication link does not name native auth directly")
	}
	return nil
}

func marshalCodexHomeProvenance(p agentic.CuratorContextProvenance) (string, error) {
	data, err := json.Marshal(p)
	return string(data), err
}

func copyCodexHomeBracket(ctx context.Context, projectDir string, before agentic.CuratorContextProvenance, digest string, items []codexHomeItem, stage string) (*CodexCuratorHomeCopy, error) {
	evidence := &CodexCuratorHomeCopy{Profile: before.ProfileName, FragmentSHA256: digest, LockSHA256: before.LockSHA256}
	for _, item := range items {
		if _, err := os.Lstat(item.source); errors.Is(err, os.ErrNotExist) && item.optional {
			evidence.Absent = append(evidence.Absent, item.name)
			continue
		} else if err != nil {
			return nil, err
		}
		if err := copyCodexHomeTree(item.source, filepath.Join(stage, item.name), nil); err != nil {
			return nil, err
		}
	}
	after, afterDigest, err := resolveCodexHomeFragment(ctx, projectDir, before)
	if err != nil {
		return nil, err
	}
	if digest != afterDigest || before.LockSHA256 != after.LockSHA256 {
		return nil, nil
	}
	for _, item := range items {
		same, err := compareCodexHomeTree(item.source, filepath.Join(stage, item.name), nil)
		if err != nil {
			return nil, err
		}
		if !same {
			return nil, nil
		}
	}
	return evidence, nil
}

// Follow legitimate Curator source links, but never reproduce one in the
// private tree. Resolve at each directory to detect ancestor cycles.
func codexHomeTreeInfo(source string, ancestors []string) (os.FileInfo, []string, error) {
	resolved, err := filepath.EvalSymlinks(source)
	if err != nil {
		return nil, nil, err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return nil, nil, err
	}
	if !info.IsDir() && !info.Mode().IsRegular() {
		return nil, nil, errors.New("non-file Curator context item")
	}
	if info.IsDir() {
		for _, ancestor := range ancestors {
			if resolved == ancestor {
				return nil, nil, errors.New("cyclic Curator context tree")
			}
		}
		ancestors = append(append([]string(nil), ancestors...), resolved)
	}
	return info, ancestors, nil
}

func copyCodexHomeTree(source, target string, ancestors []string) error {
	info, ancestors, err := codexHomeTreeInfo(source, ancestors)
	if err != nil {
		return err
	}
	if info.IsDir() {
		if err := os.Mkdir(target, 0o700); err != nil {
			return err
		}
		entries, err := os.ReadDir(source)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := copyCodexHomeTree(filepath.Join(source, entry.Name()), filepath.Join(target, entry.Name()), ancestors); err != nil {
				return err
			}
		}
		return nil
	}
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	mode := os.FileMode(0o600)
	if info.Mode().Perm()&0o111 != 0 {
		mode |= 0o100
	}
	output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	return errors.Join(copyErr, closeErr)
}

func compareCodexHomeTree(source, target string, ancestors []string) (bool, error) {
	// An absent optional input must still be absent after the second resolve.
	_, sourceErr := os.Lstat(source)
	targetInfo, targetErr := os.Lstat(target)
	if errors.Is(sourceErr, os.ErrNotExist) || errors.Is(targetErr, os.ErrNotExist) {
		if (sourceErr != nil && !errors.Is(sourceErr, os.ErrNotExist)) || (targetErr != nil && !errors.Is(targetErr, os.ErrNotExist)) {
			return false, errors.Join(sourceErr, targetErr)
		}
		return errors.Is(sourceErr, os.ErrNotExist) && errors.Is(targetErr, os.ErrNotExist), nil
	}
	if sourceErr != nil || targetErr != nil {
		return false, errors.Join(sourceErr, targetErr)
	}
	info, ancestors, err := codexHomeTreeInfo(source, ancestors)
	if err != nil {
		return false, err
	}
	if targetInfo.Mode()&os.ModeSymlink != 0 || info.IsDir() != targetInfo.IsDir() {
		return false, nil
	}
	if info.IsDir() {
		sourceEntries, err := os.ReadDir(source)
		if err != nil {
			return false, err
		}
		targetEntries, err := os.ReadDir(target)
		if err != nil {
			return false, err
		}
		if len(sourceEntries) != len(targetEntries) {
			return false, nil
		}
		for i, entry := range sourceEntries {
			if entry.Name() != targetEntries[i].Name() {
				return false, nil
			}
			same, err := compareCodexHomeTree(filepath.Join(source, entry.Name()), filepath.Join(target, entry.Name()), ancestors)
			if err != nil || !same {
				return same, err
			}
		}
		return true, nil
	}
	if !targetInfo.Mode().IsRegular() || (info.Mode().Perm()&0o111 != 0) != (targetInfo.Mode().Perm()&0o111 != 0) {
		return false, nil
	}
	input, err := os.Open(source)
	if err != nil {
		return false, err
	}
	defer input.Close()
	output, err := os.Open(target)
	if err != nil {
		return false, err
	}
	defer output.Close()
	a, b := make([]byte, 32*1024), make([]byte, 32*1024)
	for {
		na, ea := io.ReadFull(input, a)
		nb, eb := io.ReadFull(output, b)
		if ea != nil && ea != io.EOF && ea != io.ErrUnexpectedEOF {
			return false, ea
		}
		if eb != nil && eb != io.EOF && eb != io.ErrUnexpectedEOF {
			return false, eb
		}
		if !bytes.Equal(a[:na], b[:nb]) {
			return false, nil
		}
		if ea != nil || eb != nil {
			return ea == eb, nil
		}
	}
}

func publishCodexHomeCopy(stage, privateHome string, config []byte) error {
	for _, name := range []string{"AGENTS.md", "skills", "curator-mcp.config.toml"} {
		target := filepath.Join(privateHome, name)
		// Remove only context surfaces owned by this bridge; never auth or rollouts.
		if err := os.RemoveAll(target); err != nil {
			return err
		}
		source := filepath.Join(stage, name)
		if _, err := os.Lstat(source); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		if err := os.Rename(source, target); err != nil {
			return err
		}
	}
	return writePrivateFile(filepath.Join(privateHome, "config.toml"), config)
}

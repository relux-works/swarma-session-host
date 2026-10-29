package sessionmanager

import (
	"errors"
	"fmt"
	contextpolicy "github.com/relux-works/swarma-session-host/sessionmanager/resource"
	"path"
	"regexp"
	"sort"
	"strings"
)

// These are original stable refusal codes and resource spelling constraints,
// not a canonical registry or a grant of authority.
const (
	ContextErrorBuilderAllowlistDenied          = "context_builder_allowlist_denied"
	ContextErrorBuilderTraceUnavailable         = "context_builder_trace_unavailable"
	ContextErrorBuilderToolIsolationUnavailable = "context_builder_tool_isolation_unavailable"
	builderTraceTextLimit                       = 512
)

var builderRunIDPattern = regexp.MustCompile(`^RUN-[A-Za-z0-9][A-Za-z0-9-]*$`)
var builderContextIDPattern = regexp.MustCompile(`^CTX-[0-9]{6}-[a-z0-9]{6}$`)

func builderNormalizeContextID(id string) string { return contextpolicy.NormalizeContextID(id) }
func builderNormalizeContextPathSpec(spec string) string {
	return contextpolicy.NormalizeContextPathSpec(spec)
}
func builderValidateContextID(id string) error {
	if !builderContextIDPattern.MatchString(id) {
		return fmt.Errorf("invalid context id %q", id)
	}
	return nil
}
func builderTraceError(code, message string, cause error) error {
	return &BuilderTraceError{Code: code, Message: message, Cause: cause}
}
func builderIsContextErrorCode(err error, code string) bool {
	var e *PublicError
	return errors.As(err, &e) && e != nil && e.Code == code
}

// NewContextBuilderAllowlist retains the original normalized, sorted path set.
func NewContextBuilderAllowlist(paths []string) ContextBuilderAllowlist {
	cleaned := make([]string, 0, len(paths))
	seen := map[string]struct{}{}
	for _, raw := range paths {
		p := contextpolicy.NormalizeContextPathSpec(raw)
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		cleaned = append(cleaned, p)
	}
	sort.Strings(cleaned)
	return ContextBuilderAllowlist{Paths: cleaned}
}
func (a ContextBuilderAllowlist) Configured() bool { return len(a.Paths) > 0 }
func (a ContextBuilderAllowlist) Allows(spec string) bool {
	candidate := contextpolicy.NormalizeContextPathSpec(spec)
	if candidate == "" {
		return false
	}
	for _, allowed := range a.Paths {
		if candidate == allowed || strings.HasPrefix(candidate, strings.TrimSuffix(allowed, "/")+"/") {
			return true
		}
		if matched, err := path.Match(allowed, candidate); err == nil && matched {
			return true
		}
	}
	return false
}

// BuilderTraceError retains the source's stable classification and cause while
// interoperating with the existing neutral public error projection.
type BuilderTraceError struct {
	Code    string
	Message string
	Cause   error
}

func (e *BuilderTraceError) Error() string {
	if e == nil {
		return ""
	}
	return e.Code + ": " + e.Message
}
func (e *BuilderTraceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
func (e *BuilderTraceError) As(target any) bool {
	p, ok := target.(**PublicError)
	if !ok || e == nil {
		return false
	}
	*p = &PublicError{Code: e.Code, Message: e.Message}
	return true
}

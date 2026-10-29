package sessionmanager

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const claudeRemovalExceptionMarker = "task-board-claude-removal-guard: allow "

var expectedClaudeRemovalExceptions = []string{
	"claude_context_lease.go:l.file.Truncate",
	"claude_transcript.go:os.Rename",
}

// TestClaudeContextProductionHasNoFilesystemRemoval mechanically protects the
// ownership boundary agreed after review cycle 7: provider artifacts are
// retained and cataloged here; deletion requires lifecycle and lease reconciliation.
func TestClaudeContextProductionHasNoFilesystemRemoval(t *testing.T) {
	root := "."
	names, err := claudeNoRemovalProductionFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) == 0 {
		t.Fatal("no Claude production files found")
	}

	var allowed []string
	for _, name := range names {
		name := name
		t.Run(name, func(t *testing.T) {
			fset := token.NewFileSet()
			path := filepath.Join(root, name)
			file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
			if err != nil {
				t.Fatalf("parse %s: %v", name, err)
			}
			violations, exceptions := claudeRemovalFindings(fset, file)
			for _, exception := range exceptions {
				allowed = append(allowed, name+":"+exception)
			}
			if len(violations) > 0 {
				t.Fatalf(
					"%s performs filesystem removal reserved for lease-aware reclamation: %s",
					name, strings.Join(violations, ", "),
				)
			}
		})
	}
	slices.Sort(allowed)
	if !slices.Equal(allowed, expectedClaudeRemovalExceptions) {
		t.Fatalf(
			"Claude removal exceptions = %v, want the two reviewed narrow exceptions %v",
			allowed, expectedClaudeRemovalExceptions,
		)
	}
}

func claudeNoRemovalProductionFiles(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("list sessionmanager package: %w", err)
	}
	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.Type().IsRegular() &&
			strings.HasPrefix(name, "claude") &&
			strings.HasSuffix(name, ".go") &&
			!strings.HasSuffix(name, "_test.go") {
			names = append(names, name)
		}
	}
	return names, nil
}

func TestClaudeContextNoRemovalGuardDiscoversNewProductionFile(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"claude_future_context.go":      "package sessionmanager\nfunc bad() { _ = os.RemoveAll(path) }\n",
		"claude_future_context_test.go": "package sessionmanager\n",
		"codex_context.go":              "package sessionmanager\n",
	}
	for name, source := range files {
		if err := os.WriteFile(filepath.Join(root, name), []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	names, err := claudeNoRemovalProductionFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"claude_future_context.go"}; !slices.Equal(names, want) {
		t.Fatalf("discovered files = %v, want %v", names, want)
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(
		fset,
		filepath.Join(root, names[0]),
		nil,
		parser.ParseComments,
	)
	if err != nil {
		t.Fatal(err)
	}
	violations, exceptions := claudeRemovalFindings(fset, file)
	if len(violations) != 1 || len(exceptions) != 0 {
		t.Fatalf(
			"new production file findings = violations %v, exceptions %v; want one violation",
			violations, exceptions,
		)
	}
}

// TestClaudeContextNoRemovalGuardRejectsEvasions proves the guard itself, not
// only the current source. Each case is a mutation that previously evaded the
// hardcoded file/call-shape check.
func TestClaudeContextNoRemovalGuardRejectsEvasions(t *testing.T) {
	tests := []struct {
		name string
		call string
	}{
		{name: "os remove", call: `os.Remove(path)`},
		{name: "os remove all", call: `os.RemoveAll(path)`},
		{name: "root remove", call: `root.Remove(path)`},
		{name: "root remove all", call: `root.RemoveAll(path)`},
		{name: "syscall unlink", call: `syscall.Unlink(path)`},
		{name: "os truncate", call: `os.Truncate(path, 0)`},
		{name: "file truncate", call: `file.Truncate(0)`},
		{name: "os create", call: `os.Create(path)`},
		{name: "os write file", call: `os.WriteFile(path, nil, 0o600)`},
		{name: "os open file trunc", call: `os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0o600)`},
		{name: "os rename", call: `os.Rename(path, path+".evicted")`},
		{name: "aliased package", call: `filesystem.RemoveAll(path)`},
		{name: "remove helper", call: `removeArtifact(path)`},
		{name: "unlink helper", call: `unlinkArtifact(path)`},
		{name: "delete helper", call: `deleteArtifact(path)`},
		{name: "purge helper", call: `purgeArtifact(path)`},
		{name: "discard helper", call: `discardArtifact(path)`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutation := "package mutation\nfunc bad() { _ = " + tt.call + " }\n"
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "mutation.go", mutation, 0)
			if err != nil {
				t.Fatal(err)
			}
			violations, exceptions := claudeRemovalFindings(fset, file)
			if len(violations) != 1 || len(exceptions) != 0 {
				t.Fatalf(
					"guard findings for %s = violations %v, exceptions %v; want one violation",
					tt.call, violations, exceptions,
				)
			}
		})
	}
}

func TestClaudeContextNoRemovalGuardAllowsNonTruncatingCreates(t *testing.T) {
	tests := []struct {
		name string
		call string
	}{
		{name: "create temp", call: `os.CreateTemp(dir, ".staged-*")`},
		{name: "open file create", call: `os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := "package mutation\nfunc allowed() { _, _ = " + tt.call + " }\n"
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "mutation.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			violations, exceptions := claudeRemovalFindings(fset, file)
			if len(violations) != 0 || len(exceptions) != 0 {
				t.Fatalf(
					"guard findings for %s = violations %v, exceptions %v; want none",
					tt.call, violations, exceptions,
				)
			}
		})
	}
}

func claudeRemovalFindings(fset *token.FileSet, file *ast.File) ([]string, []string) {
	exceptionMarkers := claudeRemovalExceptionMarkers(fset, file)
	var violations []string
	var exceptions []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		callee, destructive := claudeDestructiveCallee(call)
		if !destructive {
			return true
		}
		position := fset.Position(call.Pos())
		if marker, ok := exceptionMarkers[position.Line-1]; ok && marker == callee {
			exceptions = append(exceptions, callee)
			return true
		}
		violations = append(violations, fmt.Sprintf("%s:%d", callee, position.Line))
		return true
	})
	return violations, exceptions
}

func claudeDestructiveCallee(call *ast.CallExpr) (string, bool) {
	switch called := call.Fun.(type) {
	case *ast.SelectorExpr:
		switch called.Sel.Name {
		case "Remove", "RemoveAll", "Unlink", "Truncate", "Rename", "Create", "WriteFile":
			return claudeExprString(called), true
		case "OpenFile":
			if claudeOpenFileTruncates(call) {
				return claudeExprString(called), true
			}
		}
	case *ast.Ident:
		switch called.Name {
		case "Create", "WriteFile":
			return called.Name, true
		case "OpenFile":
			if claudeOpenFileTruncates(call) {
				return called.Name, true
			}
		}
		lower := strings.ToLower(called.Name)
		for _, prefix := range []string{"remove", "unlink", "delete", "purge", "discard"} {
			// The exact predeclared delete function mutates a map, not the
			// filesystem. Helpers such as deleteArtifact remain violations.
			if strings.HasPrefix(lower, prefix) && lower != "delete" {
				return called.Name, true
			}
		}
	}
	return "", false
}

func claudeOpenFileTruncates(call *ast.CallExpr) bool {
	if len(call.Args) < 2 {
		return false
	}
	truncates := false
	ast.Inspect(call.Args[1], func(node ast.Node) bool {
		switch flag := node.(type) {
		case *ast.SelectorExpr:
			truncates = truncates || flag.Sel.Name == "O_TRUNC"
		case *ast.Ident:
			truncates = truncates || flag.Name == "O_TRUNC"
		}
		return !truncates
	})
	return truncates
}

func claudeExprString(expr ast.Expr) string {
	var rendered bytes.Buffer
	if err := printer.Fprint(&rendered, token.NewFileSet(), expr); err != nil {
		return "<unprintable>"
	}
	return rendered.String()
}

func claudeRemovalExceptionMarkers(fset *token.FileSet, file *ast.File) map[int]string {
	markers := make(map[int]string)
	for _, group := range file.Comments {
		for _, comment := range group.List {
			text := strings.TrimSpace(strings.TrimPrefix(comment.Text, "//"))
			if !strings.HasPrefix(text, claudeRemovalExceptionMarker) {
				continue
			}
			spec := strings.TrimPrefix(text, claudeRemovalExceptionMarker)
			callee, reason, ok := strings.Cut(spec, ";")
			if !ok || strings.TrimSpace(reason) == "" {
				continue
			}
			markers[fset.Position(comment.End()).Line] = strings.TrimSpace(callee)
		}
	}
	return markers
}

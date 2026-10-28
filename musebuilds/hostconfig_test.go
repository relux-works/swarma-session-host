package musebuilds

import (
	"os/user"
	"path/filepath"
	"strings"
	"testing"
)

// TestDefaultHostConfigIgnoresHomeOverride proves the production
// bindings come from the password database, not from HOME: even with
// HOME pointed at a scratch directory, every binding stays under the
// authenticated account's home.
func TestDefaultHostConfigIgnoresHomeOverride(t *testing.T) {
	passwd, err := user.Current()
	if err != nil {
		t.Skipf("no password-database account: %v", err)
	}
	if passwd.HomeDir == "" || !filepath.IsAbs(passwd.HomeDir) {
		t.Skipf("no absolute password-database home: %q", passwd.HomeDir)
	}
	evil, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", evil)
	cfg, err := DefaultHostConfig()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"FrozenRoot":    filepath.Join(passwd.HomeDir, ".local", "share", "muse-frozen"),
		"LedgerDir":     filepath.Join(passwd.HomeDir, ".local", "share", "mini-infra", "muse-freeze-ledger"),
		"RampStatePath": filepath.Join(passwd.HomeDir, ".config", "syspolicyd-watch.state"),
		"RampLogPath":   filepath.Join(passwd.HomeDir, "Library", "Logs", "syspolicyd-watch.tsv"),
	}
	got := map[string]string{
		"FrozenRoot":    cfg.FrozenRoot,
		"LedgerDir":     cfg.LedgerDir,
		"RampStatePath": cfg.RampStatePath,
		"RampLogPath":   cfg.RampLogPath,
	}
	for name, wantPath := range want {
		if got[name] != wantPath {
			t.Fatalf("%s = %q, want the password-database %q", name, got[name], wantPath)
		}
		if strings.HasPrefix(got[name], evil) {
			t.Fatalf("%s = %q, follows the HOME override %q", name, got[name], evil)
		}
	}
	if cfg.User != passwd.Username {
		t.Fatalf("User = %q, want the password-database %q", cfg.User, passwd.Username)
	}
	if cfg.Locker == nil {
		t.Fatal("production locker is nil")
	}
}

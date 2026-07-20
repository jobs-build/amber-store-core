package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runApp runs the CLI with args (without the leading program name) and
// returns everything it printed to the app writer.
func runApp(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var buf bytes.Buffer
	app := newApp()
	app.Writer = &buf
	err := app.Run(append([]string{"amber-store"}, args...))
	return buf.String(), err
}

// writeFixture builds a small source tree: files, a subdirectory, a symlink.
func writeFixture(t *testing.T, dir string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "b.txt"), []byte("beta"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a.txt", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
}

func TestE2E_IngestLsExportRestore(t *testing.T) {
	src := t.TempDir()
	writeFixture(t, src)
	store := t.TempDir()

	// Ingest, recording a reference.
	out, err := runApp(t, "--store", store, "ingest", "--no-progress", "--ref", "snap", src)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	root := strings.TrimSpace(out)
	if root == "" {
		t.Fatal("ingest printed no root key")
	}

	// ls by reference and by key, root and subpath.
	for _, spec := range []string{"ref:snap", root, "ref:snap@sub", root + "/sub"} {
		out, err := runApp(t, "--store", store, "ls", spec)
		if err != nil {
			t.Fatalf("ls %s: %v", spec, err)
		}
		want := "a.txt"
		if strings.Contains(spec, "sub") {
			want = "b.txt"
		}
		if !strings.Contains(out, want) {
			t.Errorf("ls %s output %q does not mention %s", spec, out, want)
		}
	}
	out, err = runApp(t, "--store", store, "ls", "ref:snap")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "link -> a.txt") {
		t.Errorf("ls output %q does not render the symlink", out)
	}

	// Export to a tar file.
	tarPath := filepath.Join(t.TempDir(), "snap.tar")
	if _, err := runApp(t, "--store", store, "export", "-o", tarPath, "ref:snap"); err != nil {
		t.Fatalf("export: %v", err)
	}
	if info, err := os.Stat(tarPath); err != nil || info.Size() == 0 {
		t.Fatalf("export wrote no tar: %v", err)
	}

	// Restore and compare with the source.
	dest := filepath.Join(t.TempDir(), "restored")
	if _, err := runApp(t, "--store", store, "restore", "ref:snap", dest); err != nil {
		t.Fatalf("restore: %v", err)
	}
	for _, f := range []struct{ rel, content string }{
		{"a.txt", "alpha"},
		{filepath.Join("sub", "b.txt"), "beta"},
	} {
		got, err := os.ReadFile(filepath.Join(dest, f.rel))
		if err != nil {
			t.Fatalf("restored %s: %v", f.rel, err)
		}
		if string(got) != f.content {
			t.Errorf("restored %s = %q, want %q", f.rel, got, f.content)
		}
	}
	if target, err := os.Readlink(filepath.Join(dest, "link")); err != nil || target != "a.txt" {
		t.Errorf("restored symlink target = %q (%v), want a.txt", target, err)
	}
	if info, err := os.Stat(filepath.Join(dest, "sub", "b.txt")); err == nil {
		if info.Mode().Perm() != 0o600 {
			t.Errorf("restored sub/b.txt mode = %o, want 600", info.Mode().Perm())
		}
	}
}

func TestE2E_RefCommands(t *testing.T) {
	src := t.TempDir()
	writeFixture(t, src)
	store := t.TempDir()

	out, err := runApp(t, "--store", store, "ingest", "--no-progress", src)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	root := strings.TrimSpace(out)

	if _, err := runApp(t, "--store", store, "ref", "set", "v1", root); err != nil {
		t.Fatalf("ref set: %v", err)
	}
	out, err = runApp(t, "--store", store, "ref", "get", "v1")
	if err != nil {
		t.Fatalf("ref get: %v", err)
	}
	if strings.TrimSpace(out) != root {
		t.Errorf("ref get = %q, want %q", strings.TrimSpace(out), root)
	}
	out, err = runApp(t, "--store", store, "ref", "list")
	if err != nil {
		t.Fatalf("ref list: %v", err)
	}
	if !strings.Contains(out, "v1") || !strings.Contains(out, root) {
		t.Errorf("ref list %q missing v1 / root", out)
	}
	if _, err := runApp(t, "--store", store, "ref", "rm", "v1"); err != nil {
		t.Fatalf("ref rm: %v", err)
	}
	if _, err := runApp(t, "--store", store, "ref", "get", "v1"); err == nil {
		t.Error("ref get after rm should fail")
	}
}

func TestE2E_NoIgnoreFlag(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, ".amberignore"), []byte("*.log\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "keep.txt"), []byte("kept"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "drop.log"), []byte("dropped"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := t.TempDir()

	withIgnore, err := runApp(t, "--store", store, "ingest", "--no-progress", src)
	if err != nil {
		t.Fatal(err)
	}
	withoutIgnore, err := runApp(t, "--store", store, "ingest", "--no-progress", "--no-ignore", src)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(withIgnore) == strings.TrimSpace(withoutIgnore) {
		t.Error("--no-ignore must change the root key")
	}
}

func TestE2E_MissingStoreFlag(t *testing.T) {
	t.Setenv("AMBER_STORE", "")
	if _, err := runApp(t, "ls", strings.Repeat("00", 32)); err == nil {
		t.Error("expected an error without --store / $AMBER_STORE")
	}
}

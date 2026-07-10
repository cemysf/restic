package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/restic/restic/internal/data"
	rtest "github.com/restic/restic/internal/test"
)

// TestBackupRcloneSource backs up an rclone remote end-to-end. A plain local
// directory is a valid rclone remote, so this exercises the real rclone
// binary without any remote service.
func TestBackupRcloneSource(t *testing.T) {
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skipf("rclone binary not found: %v", err)
	}

	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, env.gopts)

	srcdir := filepath.Join(env.base, "rclone-source")
	rtest.OK(t, os.MkdirAll(filepath.Join(srcdir, "dir"), 0755))
	rtest.OK(t, os.WriteFile(filepath.Join(srcdir, "a.txt"), []byte("object a content"), 0644))
	rtest.OK(t, os.WriteFile(filepath.Join(srcdir, "dir", "b.txt"), []byte("object b content"), 0644))

	source := "rclone:" + srcdir

	backup := func(wantSummary string) {
		t.Helper()
		gopts := env.gopts
		gopts.Quiet = false
		gopts.Verbosity = 1 // the summary line is only printed at normal verbosity
		out, err := testRunBackupOutput(t, BackupOptions{
			GroupBy: data.SnapshotGroupByOptions{Host: true, Path: true},
		}, gopts, []string{source})
		rtest.OK(t, err)
		matched, err := regexp.Match(wantSummary, out)
		rtest.OK(t, err)
		rtest.Assert(t, matched, "summary %q not found in output:\n%s", wantSummary, out)
	}

	// first backup reads everything
	backup(`2 new,\s+0 changed,\s+0 unmodified`)
	testListSnapshots(t, env.gopts, 1)

	// nothing changed: everything is skipped via the parent snapshot
	backup(`0 new,\s+0 changed,\s+2 unmodified`)

	// exactly the changed file is re-read
	rtest.OK(t, os.WriteFile(filepath.Join(srcdir, "a.txt"), []byte("object a CHANGED content"), 0644))
	backup(`0 new,\s+1 changed,\s+1 unmodified`)
	testListSnapshots(t, env.gopts, 3)

	// restore the last snapshot and verify the contents
	restoreDir := filepath.Join(env.base, "restore")
	testRunRestore(t, env.gopts, restoreDir, "latest")

	for name, want := range map[string]string{
		"a.txt":     "object a CHANGED content",
		"dir/b.txt": "object b content",
	} {
		content, err := os.ReadFile(filepath.Join(restoreDir, filepath.FromSlash(name)))
		rtest.OK(t, err)
		rtest.Equals(t, want, string(content))
	}

	testRunCheck(t, env.gopts)
}

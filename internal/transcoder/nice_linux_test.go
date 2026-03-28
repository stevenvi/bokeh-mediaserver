//go:build linux

package transcoder

import (
	"bytes"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

// TestSetNiceAppliesNiceness verifies that setNice actually sets the OS-level
// nice value of the spawned process to 10. We run `nice` with no arguments,
// which prints the calling process's own niceness and exits, then assert the
// output equals "10".
func TestSetNiceAppliesNiceness(t *testing.T) {
	cmd := exec.Command("nice")
	setNice(cmd)

	var out bytes.Buffer
	cmd.Stdout = &out

	if err := cmd.Run(); err != nil {
		t.Fatalf("command failed: %v", err)
	}

	got, err := strconv.Atoi(strings.TrimSpace(out.String()))
	if err != nil {
		t.Fatalf("could not parse `nice` output %q: %v", out.String(), err)
	}

	const want = 10
	if got != want {
		t.Errorf("niceness: got %d, want %d", got, want)
	}
}

// TestSetNiceSysProcAttr verifies that setNice sets SysProcAttr.Niceness on
// the command without running it, so the test passes even without `nice` in PATH.
func TestSetNiceSysProcAttr(t *testing.T) {
	cmd := exec.Command("true")
	setNice(cmd)

	if cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr is nil after setNice")
	}

	const want = 10
	if cmd.SysProcAttr.Niceness != want {
		t.Errorf("SysProcAttr.Niceness: got %d, want %d", cmd.SysProcAttr.Niceness, want)
	}
}

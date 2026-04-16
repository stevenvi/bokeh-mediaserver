package jobsutils_test

import (
	"os/exec"
	"testing"

	jobsutils "github.com/stevenvi/bokeh-mediaserver/internal/jobs/utils"
)

// TestSetNiceWrapsCommand verifies that setNice prepends "nice -n 10" to the command.
func TestSetNiceWrapsCommand(t *testing.T) {
	nicePath, err := exec.LookPath("nice")
	if err != nil {
		t.Skip("nice not in PATH")
	}

	cmd := exec.Command("echo", "hello")
	originalArg0 := cmd.Args[0]
	jobsutils.SetNice(cmd, 10)

	if cmd.Path != nicePath {
		t.Errorf("cmd.Path: got %q, want %q", cmd.Path, nicePath)
	}

	// Args should be: [nicePath, "-n", "10", originalArg0, "hello"]
	if len(cmd.Args) < 4 {
		t.Fatalf("cmd.Args too short: %v", cmd.Args)
	}
	if cmd.Args[1] != "-n" || cmd.Args[2] != "10" {
		t.Errorf("expected '-n 10' in args, got %v", cmd.Args[1:3])
	}
	if cmd.Args[3] != originalArg0 {
		t.Errorf("original arg0: got %q, want %q", cmd.Args[3], originalArg0)
	}
}

// TestSetNiceRunsAtReducedPriority runs a real command through setNice and
// verifies it succeeds. This confirms the wrapped invocation is well-formed.
func TestSetNiceRunsAtReducedPriority(t *testing.T) {
	if _, err := exec.LookPath("nice"); err != nil {
		t.Skip("nice not in PATH")
	}

	cmd := exec.Command("true")
	jobsutils.SetNice(cmd, 19)

	if err := cmd.Run(); err != nil {
		t.Fatalf("command failed: %v", err)
	}
}

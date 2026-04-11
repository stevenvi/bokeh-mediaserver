package jobsutils

import (
	"os/exec"
	"strconv"
)

// SetNice wraps cmd to run with a nice level.
// If nice is not in PATH, the command is left unchanged.
// Works on Linux, macOS, and other POSIX systems, but likely not Windows.
func SetNice(cmd *exec.Cmd, value int) {
	nice, err := exec.LookPath("nice")
	if err != nil {
		return
	}
	cmd.Args = append([]string{nice, "-n", strconv.Itoa(value)}, cmd.Args...)
	cmd.Path = nice
}

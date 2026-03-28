//go:build linux

package transcoder

import (
	"os/exec"
	"syscall"
)

// setNice configures the command to run at low OS priority (nice level 10).
func setNice(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Niceness: 10,
	}
}

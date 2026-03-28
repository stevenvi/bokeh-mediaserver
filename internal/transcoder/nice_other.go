//go:build !linux

package transcoder

import "os/exec"

// setNice is a no-op on non-Linux platforms.
// On Linux, the process is started at nice level 10 via SysProcAttr.
func setNice(_ *exec.Cmd) {}

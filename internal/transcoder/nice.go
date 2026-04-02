package transcoder

import "os/exec"

// setNice wraps cmd to run at nice level 10 by prepending "nice -n 10".
// If nice is not in PATH, the command is left unchanged.
// Works on Linux, macOS, and other POSIX systems.
func setNice(cmd *exec.Cmd) {
	nice, err := exec.LookPath("nice")
	if err != nil {
		return
	}
	cmd.Args = append([]string{nice, "-n", "10"}, cmd.Args...)
	cmd.Path = nice
}

package utils

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ─── Exiftool stay-open process ───────────────────────────────────────────────

// ExiftoolProcess wraps a persistent exiftool process using stay_open mode.
// One process is started per processing worker and reused for every file —
// avoids per-file Perl startup overhead which would be ~150ms × file count.
type ExiftoolProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Scanner
	mu     sync.Mutex
}

func NewExiftoolProcess() (*ExiftoolProcess, error) {
	cmd := exec.Command("exiftool",
		"-stay_open", "true",
		"-@", "-",
		"-common_args",
		"-json",
		"-struct",
		"-n", // numeric output: GPS as decimal degrees, FNumber as float, etc.
	)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start exiftool: %w", err)
	}

	return &ExiftoolProcess{
		cmd:    cmd,
		stdin:  stdin,
		stdout: bufio.NewScanner(stdoutPipe),
	}, nil
}

// Extract sends a single file path to the running exiftool process and
// returns its metadata as a raw map. Thread-safe via mutex.
func (e *ExiftoolProcess) Extract(path string) (map[string]any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Send filename + -execute sentinel
	if _, err := fmt.Fprintf(e.stdin, "%s\n-execute\n", path); err != nil {
		return nil, fmt.Errorf("write to exiftool: %w", err)
	}

	// Read lines until exiftool writes "{ready}" as the completion sentinel
	var sb strings.Builder
	for e.stdout.Scan() {
		line := e.stdout.Text()
		if line == "{ready}" {
			break
		}
		sb.WriteString(line)
		sb.WriteByte('\n')
	}
	if err := e.stdout.Err(); err != nil {
		return nil, fmt.Errorf("read from exiftool: %w", err)
	}

	// exiftool -json always returns an array
	var results []map[string]any
	if err := json.Unmarshal([]byte(sb.String()), &results); err != nil {
		return nil, fmt.Errorf("parse exiftool json: %w", err)
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no exiftool results for %s", path)
	}

	return results[0], nil
}

func (e *ExiftoolProcess) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, _ = fmt.Fprintln(e.stdin, "-stay_open\nfalse")
	_ = e.stdin.Close()
	_ = e.cmd.Wait()
}

// ─── Exiftool field helpers ───────────────────────────────────────────────────
// These extract typed values from the raw map exiftool returns.
// All return pointers (not primitives) because the DB columns they populate
// are nullable — pgx requires *T for nullable column bindings.
// All return nil on missing or unparseable fields — never panic.

func ExifStr(m map[string]any, key string) *string {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	s := fmt.Sprintf("%v", v)
	return &s
}

func ExifInt(m map[string]any, key string) *int {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	// encoding/json always unmarshals numbers into float64 when target is any,
	// so the float64 case handles all JSON-sourced numeric values.
	switch t := v.(type) {
	case float64:
		i := int(t)
		return &i
	case string:
		i, err := strconv.Atoi(t)
		if err != nil {
			return nil
		}
		return &i
	}
	return nil
}

func ExifFloat(m map[string]any, key string) *float64 {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	// encoding/json always unmarshals numbers into float64 when target is any,
	// so the float64 case handles all JSON-sourced numeric values.
	switch t := v.(type) {
	case float64:
		return &t
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return nil
		}
		return &f
	}
	return nil
}

// ExifTime parses an exiftool date/time field (e.g. DateTimeOriginal) into a *time.Time.
// Exiftool format: "2023:06:15 14:30:00" (no timezone). The time is assumed to be UTC.
func ExifTime(m map[string]any, key string) *time.Time {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	s, ok := v.(string)
	if !ok {
		return nil
	}
	t, err := time.ParseInLocation("2006:01:02 15:04:05", s, time.UTC)
	if err != nil {
		return nil
	}
	return &t
}

// ExifTimeWithOffset parses an exiftool date/time field along with its companion
// offset field (e.g. DateTimeOriginal + OffsetTimeOriginal = "-04:00").
//
// Priority:
//  1. If the date string itself contains an offset (some cameras embed it), use that.
//  2. If offsetKey is present in the map, apply that offset to the wall-clock time.
//  3. Otherwise treat the wall-clock time as UTC.
func ExifTimeWithOffset(m map[string]any, dateKey, offsetKey string) *time.Time {
	v, ok := m[dateKey]
	if !ok || v == nil {
		return nil
	}
	s, ok := v.(string)
	if !ok {
		return nil
	}

	// 1. Try the combined format some cameras write into the date field itself.
	if t, err := time.Parse("2006:01:02 15:04:05-07:00", s); err == nil {
		return &t
	}

	// 2. Look for the companion offset field (e.g. "-04:00" or "+05:30").
	loc := time.UTC
	if offsetKey != "" {
		if ov, ok2 := m[offsetKey]; ok2 && ov != nil {
			if os, ok3 := ov.(string); ok3 {
				// time.Parse with a pure offset reference time gives us the seconds offset.
				if ref, err := time.Parse("-07:00", os); err == nil {
					_, secs := ref.Zone()
					loc = time.FixedZone(os, secs)
				}
			}
		}
	}

	t, err := time.ParseInLocation("2006:01:02 15:04:05", s, loc)
	if err != nil {
		return nil
	}
	return &t
}

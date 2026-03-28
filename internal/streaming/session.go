// Package streaming manages on-the-fly HLS sessions for video items.
// When a client requests a video, a Session is created that runs ffmpeg to
// produce HLS segments in a temporary directory. Sessions are keyed on itemID
// so concurrent users watching the same file share one ffmpeg process and one
// set of segments.
//
// The dispatcher is paused while any session is active to yield CPU to ffmpeg.
// A monitor goroutine (started from main.go) resumes the dispatcher once all
// sessions have been idle for SessionIdleTimeout.
//
// WARNING: I am concerned there are still potential thrashing races in here 
// involving seeking in a stream if multiple disjoint regions of a video are 
// being requested simultaneously.
//
// TODO: There is a functional bug in this implementation: we treat all idle
// periods the same. This is not correct in reality. If I pause a video and
// go to the bathroom, something should detect that I'm not actively requesting
// data and shut off ffmpeg. But when I come back it should start ffmpeg back
// up to continue transcoding for me. But if I pause my video and go to bed,
// it should at some point give up on my returning and delete all the
// transcoded files.
//
// Now if I start up 25 videos, watch a few frames, and navigate away, it's
// going to continue 25 sessions running even though I am no longer requesting
// data from them. Some additional logic in this area is required as this will
// eventually crush the performance of the server. Maybe limit the number of
// transcodes that can be occurring simultaneously on a server?
//
// We could also have it auto-shutdown a transcode once it gets 5m or so ahead,
// and then pick back up when it is only 1m ahead. This will start and stop
// the transcoder regularly, but will prevent it from endlessly running.
//
// We could also have the client send an explicit "video closed" event to the
// server so that it can shut down a session prior to the cleanup job finding
// it. This would require tracking the number of active playback devices
// using the stream, so perhaps also a "video opened" event is also used. This
// could also be done implicitly via the bookmark endpoint that should already
// be used, that may be an even better approach.
//
// Obviously a malicious authenticated user could still game this to DoS your 
// server, but you shouldn't be letting untrusted users into your system.
package streaming

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/transcoder"
)

const (
	// TranscodeSeekRestartThreshold is how far ahead of the encode head a seek
	// must be before we restart ffmpeg at the new position (transcode mode).
	TranscodeSeekRestartThreshold = 18 * time.Second

	// RemuxSeekRestartThreshold is the equivalent threshold for remux mode.
	// Remux is near-instant so we tolerate much larger jumps before restarting.
	RemuxSeekRestartThreshold = 10 * time.Minute

	// SessionIdleTimeout is how long a session may go without any segment
	// request before it is torn down by the idle sweeper.
	SessionIdleTimeout = 15 * time.Minute

	// segmentDuration is the HLS segment target duration in seconds (must match -hls_time).
	// this is apparently the standard duration to use
	segmentDuration = 6
)

// Session holds the state for one active on-the-fly HLS encode.
type Session struct {
	itemID            int64
	fsPath            string
	tempDir           string
	cmd               *exec.Cmd
	isRemux           bool
	segments          segmentSet // segments fully written to disk
	currentSegment    int        // segment ffmpeg is currently writing
	lastActivity      time.Time
	mu                sync.Mutex
	watcher           *fsnotify.Watcher
	manifestReady     chan struct{}  // closed by watchSegments when manifest.m3u8 is created
	manifestOnce      sync.Once
	segmentAdded      chan struct{}  // closed and replaced each time a segment is marked available
}

var (
	sessions   = map[int64]*Session{}  // maps item id to session
	sessionsMu sync.Mutex
)

// ActiveSessionCount returns the number of live streaming sessions.
func ActiveSessionCount() int {
	sessionsMu.Lock()
	defer sessionsMu.Unlock()
	return len(sessions)
}

// GetOrCreateSession returns an existing session for the given item, or creates
// a new one. bitrateThreshold is cfg.TranscodeBitrateKbps. If the video's stored
// bitrate is at or below the threshold, remux mode is used; otherwise ffmpeg
// transcodes to 480p on-the-fly.
func GetOrCreateSession(itemID int64, bitrateKbps *int, fsPath string, bitrateThreshold int, dispatcher *jobs.Dispatcher) (*Session, error) {
	sessionsMu.Lock()
	defer sessionsMu.Unlock()

	if s, ok := sessions[itemID]; ok {
		s.mu.Lock()
		s.lastActivity = time.Now()
		s.mu.Unlock()
		return s, nil
	}

	tempDir, err := os.MkdirTemp("", "bokeh-live-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, fmt.Errorf("create fs watcher: %w", err)
	}
	if err := watcher.Add(tempDir); err != nil {
		_ = watcher.Close()
		_ = os.RemoveAll(tempDir)
		return nil, fmt.Errorf("watch temp dir: %w", err)
	}

	isRemux := bitrateKbps != nil && *bitrateKbps <= bitrateThreshold

	cmd := buildFFmpegCommand(fsPath, tempDir, isRemux, 0, 0, 0)
	if err := cmd.Start(); err != nil {
		_ = watcher.Close()
		_ = os.RemoveAll(tempDir)
		return nil, fmt.Errorf("start ffmpeg: %w", err)
	}

	s := &Session{
		itemID:        itemID,
		fsPath:        fsPath,
		tempDir:       tempDir,
		cmd:           cmd,
		isRemux:       isRemux,
		lastActivity:  time.Now(),
		watcher:       watcher,
		manifestReady: make(chan struct{}),
		segmentAdded:  make(chan struct{}),
	}
	sessions[itemID] = s

	// Pause background work while streaming
	dispatcher.Pause()
	transcoder.PauseActive()

	go s.watchSegments()
	go s.waitAndLog()

	return s, nil
}

// GetSession returns the existing session for the given item, or nil.
func GetSession(itemID int64) *Session {
	sessionsMu.Lock()
	defer sessionsMu.Unlock()
	return sessions[itemID]
}

// ServeManifest waits up to 6 seconds for the HLS manifest to appear, then
// serves it. Updates session.lastActivity.
func ServeManifest(w http.ResponseWriter, r *http.Request, s *Session) {
	select {
	case <-time.After(6 * time.Second):
		http.Error(w, "stream not ready", http.StatusServiceUnavailable)
		return
	case <-r.Context().Done():
		// request was closed, don't try to serve it
		return
	case <-s.manifestReady:
		// Serves the file
		s.mu.Lock()
		s.lastActivity = time.Now()
		s.mu.Unlock()

		http.ServeFile(w, r, filepath.Join(s.tempDir, "manifest.m3u8"))
	}
}

// ServeSegment serves a single HLS segment file. It waits up to 12 seconds for
// the segment to appear. If the requested segment is out of reach of the current
// encode position, ffmpeg is restarted at the first missing segment in the gap.
func ServeSegment(w http.ResponseWriter, r *http.Request, s *Session, segName string) {
	if !isValidSegmentName(segName) {
		http.Error(w, "invalid segment", http.StatusBadRequest)
		return
	}

	requestedNum := parseSegmentNumber(segName)

	// Fast path: segment already on disk.
	s.mu.Lock()
	available := s.segments.contains(requestedNum, 1)
	if available {
		s.lastActivity = time.Now()
		s.mu.Unlock()
		http.ServeFile(w, r, filepath.Join(s.tempDir, segName))
		s.proactiveGapFill(requestedNum + 1)
		return
	}

	// Slow path: segment is not yet available.
	// Determine whether the encoder will reach this segment soon.
	current := s.currentSegment
	isRemux := s.isRemux
	s.mu.Unlock()

	threshold := TranscodeSeekRestartThreshold
	if isRemux {
		threshold = RemuxSeekRestartThreshold
	}
	lookaheadSegs := int(threshold.Seconds()) / segmentDuration

	if requestedNum > current+lookaheadSegs || requestedNum < current {
		if err := s.seekTo(requestedNum); err != nil {
			slog.Warn("seek restart failed", "item_id", s.itemID, "err", err)
		}
	}

	// Wait for the segment to become available.
	deadline := time.After(12 * time.Second)
	for {
		s.mu.Lock()
		available = s.segments.contains(requestedNum, 1)
		notify := s.segmentAdded
		s.mu.Unlock()
		if available {
			break
		}
		select {
		case <-deadline:
			http.Error(w, "segment not ready", http.StatusServiceUnavailable)
			return
		case <-r.Context().Done():
			// request was closed, don't try to serve it
			return
		case <-notify:
			// a segment was marked available; re-check if it's the one we need
		}
	}

	// Serve the segment
	s.mu.Lock()
	s.lastActivity = time.Now()
	s.mu.Unlock()

	http.ServeFile(w, r, filepath.Join(s.tempDir, segName))
	s.proactiveGapFill(requestedNum + 1)
}

// seekTo restarts ffmpeg to fill the gap containing targetSeg. It finds the
// first missing segment at or before targetSeg, then encodes only up to the
// next already-available block (if one exists) so existing segments are never
// overwritten.
func (s *Session) seekTo(targetSeg int) error {
	s.mu.Lock()

	// If targetSeg is already available, nothing to do.
	if s.segments.contains(targetSeg, 1) {
		s.mu.Unlock()
		return nil
	}

	// targetSeg is the first missing segment; find where the next available
	// range begins so we can bound the ffmpeg run to just this gap.
	firstMissing := targetSeg
	nextAvailable, hasNext := s.segments.firstAvailableAfter(firstMissing)
	s.mu.Unlock()

	s.killAfterCurrentSegment()

	offsetSecs := firstMissing * segmentDuration
	durationSecs := 0
	if hasNext {
		durationSecs = (nextAvailable - firstMissing) * segmentDuration
	}

	cmd := buildFFmpegCommand(s.fsPath, s.tempDir, s.isRemux, offsetSecs, firstMissing, durationSecs)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("restart ffmpeg: %w", err)
	}

	s.mu.Lock()
	s.cmd = cmd
	s.currentSegment = firstMissing	 // set this now to prevent races between this instant and any other segment requests before ffmpeg starts up
	s.mu.Unlock()

	go s.waitAndLog()
	return nil
}

// killAfterCurrentSegment waits for the segment ffmpeg is currently writing to
// be finalized, then kills the process and removes the partial next segment.
func (s *Session) killAfterCurrentSegment() {
	s.mu.Lock()
	current := s.currentSegment
	s.mu.Unlock()

	// Wait up to one segment duration + 1s for the current segment to be marked
	// available (which happens when the watcher sees the next seg being created).
	deadline := time.After(time.Duration(segmentDuration+1) * time.Second)
	segmentCompleted := true
waitLoop:
	for {
		s.mu.Lock()
		done := s.segments.contains(current, 1)
		notify := s.segmentAdded
		s.mu.Unlock()
		if done {
			// Current segment is encoded, time to shut down ffmpeg
			break
		}
		select {
		case <-deadline:
			// Time's up, shut down ffmpeg regardless
			segmentCompleted = false
			break waitLoop
		case <-notify:
			// Check again if the segment we were waiting on is complete
			// (it should be)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
		_ = s.cmd.Wait()
		s.cmd = nil
	}

	if segmentCompleted {
		// Remove the partial segment that triggered completion detection
		// (ffmpeg just started writing it when we killed).
		_ = os.Remove(filepath.Join(s.tempDir, formatSegName(current+1)))
	} else {
		// The current segment never completed, so it should be removed
		// as it is only a partial result
		_ = os.Remove(filepath.Join(s.tempDir, formatSegName(current)))
	}
}

// watchSegments listens for fsnotify CREATE events in the session temp dir.
// When seg_N.ts is created, seg_{N-1} is known to be fully written.
func (s *Session) watchSegments() {
	for {
		select {
		case event, ok := <-s.watcher.Events:
			if !ok {
				slog.Warn("watchSegments bailed")
				return
			}
			if event.Op&fsnotify.Create == 0 {
				// Not a file create event, discard it
				continue
			}

			name := filepath.Base(event.Name)
			if name == "manifest.m3u8" {
				s.manifestOnce.Do(func() { close(s.manifestReady) })
				continue
			}
			if !isValidSegmentName(name) {
				// Not a segment file, ignore it
				continue
			}

			n := parseSegmentNumber(name)
			s.mu.Lock()
			if n > 0 {
				// Check if the previous segment exists. If so, then we know it was just created and should be tracked.
				prevSegName := formatSegName(n - 1)
				_, err := os.Stat(filepath.Join(s.tempDir, prevSegName))
				if err != nil {
					s.segments.add(n - 1)
				}
			}
			s.currentSegment = n
			old := s.segmentAdded
			s.segmentAdded = make(chan struct{})
			s.mu.Unlock()
			close(old)
		case err, ok := <-s.watcher.Errors:
			if !ok {
				slog.Warn("watchSegments", "error", err)
				return
			}
		}
	}
}

// waitAndLog waits for the session's ffmpeg process and marks the last segment
// available on clean exit (there is no "next" segment CREATE to trigger it).
func (s *Session) waitAndLog() {
	s.mu.Lock()
	cmd := s.cmd
	s.mu.Unlock()
	if cmd == nil {
		return
	}
	if err := cmd.Wait(); err != nil {
		slog.Debug("ffmpeg live session exited", "item_id", s.itemID, "err", err)
		return
	}
	// Natural exit: scan for the highest segment and mark it available.
	entries, _ := os.ReadDir(s.tempDir)
	highest := -1
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".ts") {
			continue
		}
		if n := parseSegmentNumber(e.Name()); n > highest {
			highest = n
		}
	}
	if highest >= 0 {
		s.mu.Lock()
		s.segments.add(highest)
		old := s.segmentAdded
		s.segmentAdded = make(chan struct{})
		s.mu.Unlock()
		close(old)
	}
}

// proactiveGapFill checks whether the two segments starting at nextSeg are
// available. If not, and if the encoder is not naturally heading there, it
// asynchronously calls seekTo to fill the gap before playback reaches it.
// Called after every successfully served segment to prevent hiccups at gap
// boundaries.
func (s *Session) proactiveGapFill(nextSeg int) {
	s.mu.Lock()
	lookaheadOK := s.segments.contains(nextSeg, 2)
	current := s.currentSegment
	isRemux := s.isRemux
	s.mu.Unlock()

	if lookaheadOK {
		return
	}

	lookaheadOK1 := s.segments.contains(nextSeg, 1)
	lookaheadOK2 := s.segments.contains(nextSeg + 1, 1)

	threshold := TranscodeSeekRestartThreshold
	if isRemux {
		threshold = RemuxSeekRestartThreshold
	}
	lookaheadSegs := int(threshold.Seconds()) / segmentDuration

	// If the encoder is already within lookahead range of nextSeg, it will
	// produce it naturally — no need to restart.
	if nextSeg >= current && nextSeg <= current+lookaheadSegs {
		return
	}

	go func() {
		if lookaheadOK1 && !lookaheadOK2 {
			// If we already have the first but not the second lookahead, set the seek position appropriately
			nextSeg = nextSeg + 1
		}

		if err := s.seekTo(nextSeg); err != nil {
			slog.Warn("proactive gap fill failed", "item_id", s.itemID, "err", err)
		}
	}()
}

// buildFFmpegCommand constructs the ffmpeg command for on-the-fly HLS.
// isRemux uses -c copy; otherwise transcodes to 480p at 1.5 Mbps.
// offsetSecs sets -ss (pre-input fast seek); startNum sets -hls_start_number.
// durationSecs, when non-zero, sets -t to limit output to a gap range.
func buildFFmpegCommand(fsPath, tempDir string, isRemux bool, offsetSecs, startNum, durationSecs int) *exec.Cmd {
	// Generate a proper manifest on initial create,
	// generate an alternative, unused manifest when seeking
	manifestFile := "manifest.m3u8"
	if offsetSecs != 0 || durationSecs != 0 {
		manifestFile = "manifest-seek.m3u8"
	}
	manifestPath := filepath.Join(tempDir, manifestFile)
	segPattern := filepath.Join(tempDir, "seg_%05d.ts")

	args := []string{}
	if offsetSecs > 0 {
		args = append(args, "-ss", strconv.Itoa(offsetSecs))
	}
	args = append(args, "-i", fsPath)

	if durationSecs > 0 {
		args = append(args, "-t", strconv.Itoa(durationSecs))
	}

	if isRemux {
		args = append(args, "-c", "copy")
	} else {
		args = append(args,
			"-c:v", "libx264",
			"-preset", "ultrafast",
			"-vf", "scale=-2:480",
			"-b:v", "1500k",
			"-c:a", "aac",
			"-b:a", "96k",
		)
	}

	args = append(args,
		"-hls_time", strconv.Itoa(segmentDuration),
		"-hls_playlist_type", "vod",
		"-hls_list_size", "0",
		"-hls_start_number", strconv.Itoa(startNum),
		"-hls_segment_filename", segPattern,
		manifestPath,
	)

	return exec.Command("ffmpeg", args...)
}

// StartIdleSweeper starts a background goroutine that tears down sessions that
// have been idle for longer than SessionIdleTimeout. Exits when ctx is cancelled.
func StartIdleSweeper(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sweepIdleSessions()
			}
		}
	}()
}

func sweepIdleSessions() {
	sessionsMu.Lock()
	defer sessionsMu.Unlock()

	now := time.Now()
	for itemID, s := range sessions {
		s.mu.Lock()
		idle := now.Sub(s.lastActivity)
		s.mu.Unlock()
		if idle > SessionIdleTimeout {
			cleanupSession(itemID, s)
		}
	}
}

// cleanupSession kills ffmpeg, closes the watcher, removes the temp dir, and
// removes the session from the map. Caller must hold sessionsMu.
func cleanupSession(itemID int64, s *Session) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
		_ = s.cmd.Wait()
	}
	_ = s.watcher.Close()
	_ = os.RemoveAll(s.tempDir)
	delete(sessions, itemID)

	slog.Debug("cleaned up idle streaming session", "item_id", itemID)
}

// formatSegName formats a segment number as the filename ffmpeg produces.
func formatSegName(n int) string {
	return fmt.Sprintf("seg_%05d.ts", n)
}

// isValidSegmentName returns true if name matches the pattern seg_NNNNN.ts.
func isValidSegmentName(name string) bool {
	if !strings.HasPrefix(name, "seg_") || !strings.HasSuffix(name, ".ts") {
		return false
	}
	inner := name[4 : len(name)-3]
	for _, c := range inner {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(inner) > 0
}

// parseSegmentNumber extracts the integer segment number from a name like "seg_00042.ts".
func parseSegmentNumber(name string) int {
	s := strings.TrimPrefix(name, "seg_")
	s = strings.TrimSuffix(s, ".ts")
	n, _ := strconv.Atoi(s)
	return n
}

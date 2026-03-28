package streaming

import "sort"

// segRange is an inclusive [lo, hi] range of available segment numbers.
type segRange struct {
	lo, hi int
}

// segmentSet tracks which HLS segments are available as a sorted list of
// non-overlapping, non-adjacent [lo, hi] ranges. In typical use the list
// stays very short (one range per ffmpeg run, typically 2–4 total).
type segmentSet struct {
	ranges []segRange
}

// add marks segment n as available, extending or merging existing ranges as needed.
func (s *segmentSet) add(n int) {
	// pos is the index of the first range whose lo > n.
	pos := sort.Search(len(s.ranges), func(i int) bool { return s.ranges[i].lo > n })

	if pos > 0 && s.ranges[pos-1].hi >= n {
		// Already included in the range immediately to the left.
		return
	}

	leftAdjacent := pos > 0 && s.ranges[pos-1].hi == n-1
	rightAdjacent := pos < len(s.ranges) && s.ranges[pos].lo == n+1

	switch {
	case leftAdjacent && rightAdjacent:
		// n bridges two existing ranges — merge them into one.
		s.ranges[pos-1].hi = s.ranges[pos].hi
		s.ranges = append(s.ranges[:pos], s.ranges[pos+1:]...)
	case leftAdjacent:
		s.ranges[pos-1].hi = n
	case rightAdjacent:
		s.ranges[pos].lo = n
	default:
		// Insert a new single-element range at pos.
		s.ranges = append(s.ranges, segRange{})
		copy(s.ranges[pos+1:], s.ranges[pos:])
		s.ranges[pos] = segRange{n, n}
	}
}

// contains reports whether all segments in [n, n+length) are available.
// Use length=1 to check a single segment.
func (s *segmentSet) contains(n, length int) bool {
	pos := sort.Search(len(s.ranges), func(i int) bool { return s.ranges[i].lo > n })
	return pos > 0 && s.ranges[pos-1].hi >= n+length-1
}

// firstAvailableAfter returns the lowest available segment number whose
// index is strictly greater than n, and true. Returns 0, false if no
// such segment exists. This is used by seekTo to find the end of a gap.
func (s *segmentSet) firstAvailableAfter(n int) (int, bool) {
	pos := sort.Search(len(s.ranges), func(i int) bool { return s.ranges[i].lo > n })
	if pos >= len(s.ranges) {
		return 0, false
	}
	return s.ranges[pos].lo, true
}

package streaming

import "testing"

// checkRanges asserts the internal range list of s matches want exactly.
func checkRanges(t *testing.T, s *segmentSet, want []segRange) {
	t.Helper()
	if len(s.ranges) != len(want) {
		t.Fatalf("got %d ranges %v, want %d ranges %v", len(s.ranges), s.ranges, len(want), want)
	}
	for i, r := range s.ranges {
		if r != want[i] {
			t.Errorf("ranges[%d] = %v, want %v", i, r, want[i])
		}
	}
}

func TestSegmentSetAdd(t *testing.T) {
	t.Run("empty set gains single range", func(t *testing.T) {
		var s segmentSet
		s.add(5)
		checkRanges(t, &s, []segRange{{5, 5}})
	})

	t.Run("segment zero", func(t *testing.T) {
		var s segmentSet
		s.add(0)
		checkRanges(t, &s, []segRange{{0, 0}})
	})

	t.Run("adjacent right extends range", func(t *testing.T) {
		var s segmentSet
		s.add(5)
		s.add(6)
		checkRanges(t, &s, []segRange{{5, 6}})
	})

	t.Run("adjacent left extends range", func(t *testing.T) {
		var s segmentSet
		s.add(5)
		s.add(4)
		checkRanges(t, &s, []segRange{{4, 5}})
	})

	t.Run("non-adjacent creates separate ranges", func(t *testing.T) {
		var s segmentSet
		s.add(3)
		s.add(7)
		checkRanges(t, &s, []segRange{{3, 3}, {7, 7}})
	})

	t.Run("element bridging two ranges merges all three", func(t *testing.T) {
		var s segmentSet
		s.add(3)
		s.add(5)
		s.add(4)
		checkRanges(t, &s, []segRange{{3, 5}})
	})

	t.Run("element adjacent only to left range extends it", func(t *testing.T) {
		var s segmentSet
		s.add(3)
		s.add(7)
		s.add(4)
		checkRanges(t, &s, []segRange{{3, 4}, {7, 7}})
	})

	t.Run("element adjacent only to right range extends it", func(t *testing.T) {
		var s segmentSet
		s.add(3)
		s.add(8)
		s.add(7)
		checkRanges(t, &s, []segRange{{3, 3}, {7, 8}})
	})

	t.Run("adding same element is idempotent", func(t *testing.T) {
		var s segmentSet
		s.add(5)
		s.add(5)
		checkRanges(t, &s, []segRange{{5, 5}})
	})

	t.Run("adding element within existing range is idempotent", func(t *testing.T) {
		var s segmentSet
		s.add(3)
		s.add(4)
		s.add(5)
		s.add(4)
		checkRanges(t, &s, []segRange{{3, 5}})
	})

	t.Run("sequential addition from zero forms single range", func(t *testing.T) {
		var s segmentSet
		for i := 0; i < 10; i++ {
			s.add(i)
		}
		checkRanges(t, &s, []segRange{{0, 9}})
	})

	t.Run("reverse sequential addition forms single range", func(t *testing.T) {
		var s segmentSet
		for i := 9; i >= 0; i-- {
			s.add(i)
		}
		checkRanges(t, &s, []segRange{{0, 9}})
	})

	t.Run("sparse addition creates multiple disjoint ranges", func(t *testing.T) {
		var s segmentSet
		s.add(0)
		s.add(2)
		s.add(4)
		checkRanges(t, &s, []segRange{{0, 0}, {2, 2}, {4, 4}})
	})

	t.Run("filling gaps reduces range count incrementally", func(t *testing.T) {
		var s segmentSet
		s.add(0)
		s.add(2)
		s.add(4)
		s.add(1)
		checkRanges(t, &s, []segRange{{0, 2}, {4, 4}})
		s.add(3)
		checkRanges(t, &s, []segRange{{0, 4}})
	})

	t.Run("insert before all existing ranges", func(t *testing.T) {
		var s segmentSet
		s.add(5)
		s.add(10)
		s.add(2)
		checkRanges(t, &s, []segRange{{2, 2}, {5, 5}, {10, 10}})
	})

	t.Run("insert after all existing ranges", func(t *testing.T) {
		var s segmentSet
		s.add(2)
		s.add(5)
		s.add(10)
		checkRanges(t, &s, []segRange{{2, 2}, {5, 5}, {10, 10}})
	})
}

func TestSegmentSetContains(t *testing.T) {
	tests := []struct {
		name   string
		setup  []int
		query  int
		length int
		want   bool
	}{
		// Single-segment checks (length=1)
		{"empty set", nil, 5, 1, false},
		{"exact single element", []int{5}, 5, 1, true},
		{"below single element", []int{5}, 4, 1, false},
		{"above single element", []int{5}, 6, 1, false},
		{"lo boundary of range", []int{3, 4, 5}, 3, 1, true},
		{"mid range", []int{3, 4, 5}, 4, 1, true},
		{"hi boundary of range", []int{3, 4, 5}, 5, 1, true},
		{"just below range", []int{3, 4, 5}, 2, 1, false},
		{"just above range", []int{3, 4, 5}, 6, 1, false},
		{"in gap between two ranges", []int{2, 4}, 3, 1, false},
		{"lo boundary of second range", []int{1, 2, 5, 6}, 5, 1, true},
		{"hi boundary of first range", []int{1, 2, 5, 6}, 2, 1, true},
		{"in gap of three-range set", []int{0, 5, 10}, 3, 1, false},
		{"zero is available", []int{0}, 0, 1, true},
		{"zero is not available", []int{1}, 0, 1, false},

		// Multi-segment checks (length>1)
		{"range fully within single interval", []int{3, 4, 5, 6, 7}, 4, 3, true},
		{"range starts at lo boundary", []int{3, 4, 5, 6, 7}, 3, 5, true},
		{"range ends at hi boundary", []int{3, 4, 5, 6, 7}, 5, 3, true},
		{"range exceeds hi boundary by one", []int{3, 4, 5, 6, 7}, 5, 4, false},
		{"range starts in gap", []int{1, 2, 5, 6}, 2, 2, false},
		{"range spans a gap between two intervals", []int{1, 2, 5, 6}, 1, 5, false},
		{"lookahead of 2 both available", []int{10, 11, 12}, 10, 2, true},
		{"lookahead of 2 second missing", []int{10, 12}, 10, 2, false},
		{"lookahead of 2 both missing", nil, 10, 2, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s segmentSet
			for _, n := range tt.setup {
				s.add(n)
			}
			if got := s.contains(tt.query, tt.length); got != tt.want {
				t.Errorf("contains(%d, %d) = %v, want %v", tt.query, tt.length, got, tt.want)
			}
		})
	}
}

func TestSegmentSetFirstAvailableAfter(t *testing.T) {
	tests := []struct {
		name    string
		setup   []int
		query   int
		wantVal int
		wantOK  bool
	}{
		{"empty set", nil, 0, 0, false},
		{"single range, query before it", []int{5}, 3, 5, true},
		{"single range, query at its only element", []int{5}, 5, 0, false},
		{"single range, query above it", []int{5}, 6, 0, false},
		{"query in gap between two ranges", []int{1, 2, 5, 6}, 3, 5, true},
		{"query at hi of first range skips to second", []int{1, 2, 5, 6}, 2, 5, true},
		{"query at hi of last range", []int{1, 2, 5, 6}, 6, 0, false},
		{"three ranges, query returns nearest following", []int{1, 5, 10}, 2, 5, true},
		{"three ranges, query in second gap", []int{1, 5, 10}, 6, 10, true},
		{"three ranges, query past all", []int{1, 5, 10}, 10, 0, false},
		{"query before all ranges", []int{5, 10}, 0, 5, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s segmentSet
			for _, n := range tt.setup {
				s.add(n)
			}
			got, ok := s.firstAvailableAfter(tt.query)
			if ok != tt.wantOK {
				t.Fatalf("firstAvailableAfter(%d) ok = %v, want %v", tt.query, ok, tt.wantOK)
			}
			if ok && got != tt.wantVal {
				t.Errorf("firstAvailableAfter(%d) = %d, want %d", tt.query, got, tt.wantVal)
			}
		})
	}
}

// TestSegmentSetSeekScenario simulates the real-world seek pattern that motivated
// the interval set: play from start, seek to middle, seek back. Verifies that gap
// boundaries are correctly reported so seekTo can fill them without scanning.
// This test may not be strictly necessary, just spitballing at the moment.
func TestSegmentSetSeekScenario(t *testing.T) {
	var s segmentSet

	// Phase 1: initial play — ffmpeg produces segments 0–49.
	for i := 0; i <= 49; i++ {
		s.add(i)
	}
	checkRanges(t, &s, []segRange{{0, 49}})

	// Phase 2: seek to middle — ffmpeg restarts and produces 500–520.
	for i := 500; i <= 520; i++ {
		s.add(i)
	}
	checkRanges(t, &s, []segRange{{0, 49}, {500, 520}})

	// Phase 3: seek back to beginning. Segments 0–49 still available.
	if !s.contains(0, 1) || !s.contains(49, 1) {
		t.Error("segments 0–49 should still be available after seek")
	}

	// The gap starts at segment 50.
	if s.contains(50, 1) {
		t.Error("segment 50 should be in the gap")
	}

	// Proactive lookahead: contains(N+1, 2) after serving segment 48 detects
	// that the two-segment window [49, 50] is not fully available.
	if s.contains(49, 2) {
		t.Error("lookahead contains(49, 2) should detect the gap at 50")
	}

	// seekTo(50) asks: where does the next available range start after the gap?
	nextAvail, ok := s.firstAvailableAfter(50)
	if !ok {
		t.Fatal("firstAvailableAfter(50) returned no result, expected 500")
	}
	if nextAvail != 500 {
		t.Errorf("firstAvailableAfter(50) = %d, want 500", nextAvail)
	}

	// Phase 4: fill the gap — ffmpeg produces 50–499.
	for i := 50; i <= 499; i++ {
		s.add(i)
	}
	checkRanges(t, &s, []segRange{{0, 520}})
}

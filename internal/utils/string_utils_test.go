package utils

import (
	"testing"
	"time"
)

func tp(year, month, day int) *time.Time {
	t := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	return &t
}

func TestExtractDatePrefix(t *testing.T) {
	cases := []struct {
		input       string
		wantName    string
		wantStart   *time.Time
		wantEnd     *time.Time
	}{
		{
			input:     "No date title",
			wantName:  "No date title",
			wantStart: nil,
			wantEnd:   nil,
		},
		{
			input:     "2004 Misc",
			wantName:  "Misc",
			wantStart: tp(2004, 1, 1),
			wantEnd:   tp(2004, 12, 31),
		},
		{
			input:     "2005.05 Mayterm",
			wantName:  "Mayterm",
			wantStart: tp(2005, 5, 1),
			wantEnd:   tp(2005, 5, 31),
		},
		{
			input:     "2002.08-12 Spring Semester",
			wantName:  "Spring Semester",
			wantStart: tp(2002, 8, 1),
			wantEnd:   tp(2002, 12, 31),
		},
		{
			input:     "2023.05.30 Cades Cove, Cataract Falls",
			wantName:  "Cades Cove, Cataract Falls",
			wantStart: tp(2023, 5, 30),
			wantEnd:   tp(2023, 5, 30),
		},
		{
			input:     "2024.06.02-04 Ithaca Trip",
			wantName:  "Ithaca Trip",
			wantStart: tp(2024, 6, 2),
			wantEnd:   tp(2024, 6, 4),
		},
		{
			input:     "2025.06.29-07.01 Driving To Chicago",
			wantName:  "Driving To Chicago",
			wantStart: tp(2025, 06, 29),
			wantEnd:   tp(2025, 07, 1),
		},
		{
			input:     "2025.12.29-2026.01.02 Post-Christmas in VA",
			wantName:  "Post-Christmas in VA",
			wantStart: tp(2025, 12, 29),
			wantEnd:   tp(2026, 1, 2),
		},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			gotName, gotStart, gotEnd := ExtractDatePrefix(tc.input)
			if gotName != tc.wantName {
				t.Errorf("name = %q, want %q", gotName, tc.wantName)
			}
			if tc.wantStart == nil && gotStart != nil {
				t.Errorf("startDate = %v, want nil", gotStart)
			} else if tc.wantStart != nil && (gotStart == nil || !gotStart.Equal(*tc.wantStart)) {
				t.Errorf("startDate = %v, want %v", gotStart, tc.wantStart)
			}
			if tc.wantEnd == nil && gotEnd != nil {
				t.Errorf("endDate = %v, want nil", gotEnd)
			} else if tc.wantEnd != nil && (gotEnd == nil || !gotEnd.Equal(*tc.wantEnd)) {
				t.Errorf("endDate = %v, want %v", gotEnd, tc.wantEnd)
			}
		})
	}
}

func TestGenerateSortName(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		// Articles moved to end
		{"The Beatles", "beatles the"},
		{"the beatles", "beatles the"},
		{"A Brick In The Wall", "brick in the wall a"},
		{"An Artist", "artist an"},

		// Punctuation stripped
		{`"Weird Al" Yankovic`, `weird al yankovic`},
		{"'Til Tuesday", "til tuesday"},

		// Mixed case without article sorts lowercase
		{"dc Talk", "dc talk"},
		{"AC/DC", "acdc"},
		{"ZZ Top", "zz top"},

		// No transformation needed
		{"Nightwish", "nightwish"},
		{"Sabaton", "sabaton"},

		// Article-like prefix that is not an article
		{"Theater of Pain", "theater of pain"},
		{"Another Day", "another day"},

		// Leading punctuation followed by an article
		{`"The Alarm"`, `alarm the`},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got := GenerateSortName(tc.input)
			if got != tc.want {
				t.Errorf("GenerateSortName(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

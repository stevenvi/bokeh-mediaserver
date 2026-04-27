package utils

import (
	"testing"
)

func sp(s string) *string { return &s }

func TestExtractDatePrefix(t *testing.T) {
	cases := []struct {
		input     string
		wantName  string
		wantLabel *string
	}{
		{
			input:     "No date title",
			wantName:  "No date title",
			wantLabel: nil,
		},
		{
			input:     "2004 Misc",
			wantName:  "Misc",
			wantLabel: sp("2004"),
		},
		{
			input:     "2005.05 Mayterm",
			wantName:  "Mayterm",
			wantLabel: sp("May 2005"),
		},
		{
			input:     "2002.08-12 Spring Semester",
			wantName:  "Spring Semester",
			wantLabel: sp("August - December, 2002"),
		},
		{
			input:     "2023.05.30 Cades Cove, Cataract Falls",
			wantName:  "Cades Cove, Cataract Falls",
			wantLabel: sp("May 30, 2023"),
		},
		{
			input:     "2024.06.02-04 Ithaca Trip",
			wantName:  "Ithaca Trip",
			wantLabel: sp("June 2 - 4, 2024"),
		},
		{
			input:     "2025.06.29-07.01 Driving To Chicago",
			wantName:  "Driving To Chicago",
			wantLabel: sp("June 29 - July 1, 2025"),
		},
		{
			input:     "2025.12.29-2026.01.02 Post-Christmas in VA",
			wantName:  "Post-Christmas in VA",
			wantLabel: sp("December 29, 2025 - January 2, 2026"),
		},
		{
			input:     "1984-1987 Various",
			wantName:  "Various",
			wantLabel: sp("1984 - 1987"),
		},
		{
			input:     "2024.01.02-03.04 Winter Trip",
			wantName:  "Winter Trip",
			wantLabel: sp("January 2 - March 4, 2024"),
		},
		{
			input:     "2001.12.15-2002.01.05 Christmas Break",
			wantName:  "Christmas Break",
			wantLabel: sp("December 15, 2001 - January 5, 2002"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			gotName, gotLabel := ExtractDatePrefix(tc.input)
			if gotName != tc.wantName {
				t.Errorf("name = %q, want %q", gotName, tc.wantName)
			}
			if tc.wantLabel == nil && gotLabel != nil {
				t.Errorf("label = %q, want nil", *gotLabel)
			} else if tc.wantLabel != nil && (gotLabel == nil || *gotLabel != *tc.wantLabel) {
				got := "<nil>"
				if gotLabel != nil {
					got = *gotLabel
				}
				t.Errorf("label = %q, want %q", got, *tc.wantLabel)
			}
		})
	}
}

func TestExtractDatePrefixStr(t *testing.T) {
	cases := []struct {
		input      string
		wantName   string
		wantPrefix *string
	}{
		{"No date title", "No date title", nil},
		{"2004 Misc", "Misc", sp("2004")},
		{"2005.05 Mayterm", "Mayterm", sp("2005.05")},
		{"2024.06.02-04 Ithaca Trip", "Ithaca Trip", sp("2024.06.02-04")},
		{"2025.06.29-07.01 Road Trip", "Road Trip", sp("2025.06.29-07.01")},
		{"1984-1987 Various", "Various", sp("1984-1987")},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			gotName, gotPrefix := ExtractDatePrefixStr(tc.input)
			if gotName != tc.wantName {
				t.Errorf("name = %q, want %q", gotName, tc.wantName)
			}
			if tc.wantPrefix == nil && gotPrefix != nil {
				t.Errorf("prefix = %q, want nil", *gotPrefix)
			} else if tc.wantPrefix != nil && (gotPrefix == nil || *gotPrefix != *tc.wantPrefix) {
				got := "<nil>"
				if gotPrefix != nil {
					got = *gotPrefix
				}
				t.Errorf("prefix = %q, want %q", got, *tc.wantPrefix)
			}
		})
	}
}

func TestParseDateString(t *testing.T) {
	cases := []struct {
		input string
		want  *string
	}{
		{"", nil},
		{"garbage", nil},
		{"2004", sp("2004")},
		{"1984-1987", sp("1984 - 1987")},
		{"2005.05", sp("May 2005")},
		{"2002.08-12", sp("August - December, 2002")},
		{"2023.05.30", sp("May 30, 2023")},
		{"2024.06.02-04", sp("June 2 - 4, 2024")},
		{"2025.06.29-07.01", sp("June 29 - July 1, 2025")},
		{"2025.12.29-2026.01.02", sp("December 29, 2025 - January 2, 2026")},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got := ParseDateString(tc.input)
			if tc.want == nil && got != nil {
				t.Errorf("ParseDateString(%q) = %q, want nil", tc.input, *got)
			} else if tc.want != nil && (got == nil || *got != *tc.want) {
				g := "<nil>"
				if got != nil {
					g = *got
				}
				t.Errorf("ParseDateString(%q) = %q, want %q", tc.input, g, *tc.want)
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

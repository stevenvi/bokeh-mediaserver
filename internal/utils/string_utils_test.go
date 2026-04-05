package utils

import (
	"testing"
)

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

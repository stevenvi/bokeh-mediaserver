package utils

import (
	"regexp"
	"strings"
)

var punctuationRegex, _ = regexp.Compile("[^a-zA-Z0-9 ]+")

// GenerateSortName creates a sort-friendly name by:
//  1. Lower-casing the result so mixed-case names sort with their peers
//  2. Stripping punctuation (e.g. `"Weird Al" Yankovic` → `weird al yankovic`)
//  3. Moving common articles to the end (e.g. `The Beatles` → `beatles the`)
func GenerateSortName(name string) string {
	// Strip punctuation characters so names like `"Weird Al" Yankovic`
	// sort on the first letter rather than the quote character.
	stripped := punctuationRegex.ReplaceAllString(name, "")
	if stripped == "" {
		stripped = name // nothing left after stripping — keep original
	}

	lower := strings.ToLower(stripped)
	for _, article := range []string{"the ", "a ", "an "} {
		if strings.HasPrefix(lower, article) {
			rest := lower[len(article):]
			suffix := strings.TrimSpace(lower[:len(article)])
			return rest + " " + suffix
		}
	}
	return lower
}

package utils

import (
	"regexp"
	"strconv"
	"strings"
	"time"
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

// ExtractDatePrefix extracts a date prefix from a string name. It returns the
// stripped name and the extracted date, or nil if no date prefix is found.
//
// Supported formats:
//   - YYYY.MM[.DD[-DD]] (e.g., "2023.07 Summer vacation" or "2023.07.15 Beach trip")
//
// Returns: (strippedName, date, endDate)
func ExtractDatePrefix(name string) (string, *time.Time, *time.Time) {
	// Regex matches: YYYY.MM[.DD[-DD]] followed by whitespace and the rest
	re := regexp.MustCompile(`^(\d{4})\.(\d{2})(?:\.(\d{2})(?:-(\d{2}))?)?[\s_](.+)$`)
	matches := re.FindStringSubmatch(name)
	if matches == nil {
		return name, nil, nil
	}

	year, _ := strconv.Atoi(matches[1])
	month, _ := strconv.Atoi(matches[2])

	startDay := 1
	if matches[3] != "" {
		startDay, _ = strconv.Atoi(matches[3])
	}

	startTime := time.Date(year, time.Month(month), startDay, 0, 0, 0, 0, time.UTC)
	strippedName := strings.TrimSpace(matches[5])

	var endDate *time.Time
	if matches[4] != "" {
		endDay, _ := strconv.Atoi(matches[4])
		endTime := time.Date(year, time.Month(month), endDay, 0, 0, 0, 0, time.UTC)
		endDate = &endTime
	}

	return strippedName, &startTime, endDate
}

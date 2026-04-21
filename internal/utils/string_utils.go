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

// datePatternDay matches day-precision prefixes: YYYY.MM.DD with an optional end.
// End alternatives: YYYY.MM.DD | MM.DD | DD
var datePatternDay = regexp.MustCompile(`^(\d{4})\.(\d{2})\.(\d{2})(?:-(?:(\d{4})\.(\d{2})\.(\d{2})|(\d{2})\.(\d{2})|(\d{2})))?[\s_](.+)$`)

// datePatternMonth matches month-precision prefixes: YYYY.MM with an optional end month.
var datePatternMonth = regexp.MustCompile(`^(\d{4})\.(\d{2})(?:-(\d{2}))?[\s_](.+)$`)

// datePatternYear matches year-precision prefixes: YYYY.
var datePatternYear = regexp.MustCompile(`^(\d{4})[\s_](.+)$`)

// lastDay returns the last calendar day of the given month/year.
func lastDay(year, month int) int {
	return time.Date(year, time.Month(month+1), 0, 0, 0, 0, 0, time.UTC).Day()
}

// ExtractDatePrefix extracts a date prefix from a string name. It returns the
// stripped name and the extracted date range, or nil dates if no prefix is found.
//
// Supported formats (most- to least-precise):
//   - YYYY.MM.DD[-DD]           same-month range,  e.g. "2024.06.02-04 Ithaca Trip"
//   - YYYY.MM.DD[-MM.DD]        cross-month range, e.g. "2025.06.29-07.01 Road Trip"
//   - YYYY.MM.DD[-YYYY.MM.DD]   cross-year range,  e.g. "2025.12.29-2026.01.02 Holiday"
//   - YYYY.MM[-MM]              month range,        e.g. "2002.08-12 Spring Semester"
//   - YYYY.MM                   single month,       e.g. "2005.05 Mayterm"
//   - YYYY                      single year,        e.g. "2004 Misc"
//
// When no explicit end is given, endDate is set to the last moment of the
// start period (same day, last day of month, or Dec 31 for year-only).
//
// Returns: (strippedName, startDate, endDate)
func ExtractDatePrefix(name string) (string, *time.Time, *time.Time) {
	// --- Day precision ---
	// Groups: [1]=startYear [2]=startMonth [3]=startDay
	//         [4][5][6]=endYYYY.MM.DD  [7][8]=endMM.DD  [9]=endDD  [10]=name
	if m := datePatternDay.FindStringSubmatch(name); m != nil {
		year, _ := strconv.Atoi(m[1])
		month, _ := strconv.Atoi(m[2])
		startDay, _ := strconv.Atoi(m[3])
		start := time.Date(year, time.Month(month), startDay, 0, 0, 0, 0, time.UTC)

		var end time.Time
		switch {
		case m[4] != "": // YYYY.MM.DD
			endYear, _ := strconv.Atoi(m[4])
			endMonth, _ := strconv.Atoi(m[5])
			endDay, _ := strconv.Atoi(m[6])
			end = time.Date(endYear, time.Month(endMonth), endDay, 0, 0, 0, 0, time.UTC)
		case m[7] != "": // MM.DD (same year)
			endMonth, _ := strconv.Atoi(m[7])
			endDay, _ := strconv.Atoi(m[8])
			end = time.Date(year, time.Month(endMonth), endDay, 0, 0, 0, 0, time.UTC)
		case m[9] != "": // DD (same year and month)
			endDay, _ := strconv.Atoi(m[9])
			end = time.Date(year, time.Month(month), endDay, 0, 0, 0, 0, time.UTC)
		default: // single day
			end = start
		}
		return strings.TrimSpace(m[10]), &start, &end
	}

	// --- Month precision ---
	if m := datePatternMonth.FindStringSubmatch(name); m != nil {
		year, _ := strconv.Atoi(m[1])
		startMonth, _ := strconv.Atoi(m[2])
		start := time.Date(year, time.Month(startMonth), 1, 0, 0, 0, 0, time.UTC)

		endMonth := startMonth
		if m[3] != "" {
			endMonth, _ = strconv.Atoi(m[3])
		}
		end := time.Date(year, time.Month(endMonth), lastDay(year, endMonth), 0, 0, 0, 0, time.UTC)
		return strings.TrimSpace(m[4]), &start, &end
	}

	// --- Year precision ---
	if m := datePatternYear.FindStringSubmatch(name); m != nil {
		year, _ := strconv.Atoi(m[1])
		start := time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(year, time.December, 31, 0, 0, 0, 0, time.UTC)
		return strings.TrimSpace(m[2]), &start, &end
	}

	return name, nil, nil
}

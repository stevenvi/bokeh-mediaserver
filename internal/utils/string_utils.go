package utils

import (
	"fmt"
	"regexp"
	"strconv"
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

// datePatternDay matches day-precision prefixes: YYYY.MM.DD with an optional end.
// End alternatives: YYYY.MM.DD | MM.DD | DD
var datePatternDay = regexp.MustCompile(`^(\d{4})\.(\d{2})\.(\d{2})(?:-(?:(\d{4})\.(\d{2})\.(\d{2})|(\d{2})\.(\d{2})|(\d{2})))?[\s_](.+)$`)

// datePatternMonth matches month-precision prefixes: YYYY.MM with an optional end month.
var datePatternMonth = regexp.MustCompile(`^(\d{4})\.(\d{2})(?:-(\d{2}))?[\s_](.+)$`)

// datePatternYearRange matches year-range prefixes: YYYY-YYYY.
var datePatternYearRange = regexp.MustCompile(`^(\d{4})-(\d{4})[\s_](.+)$`)

// datePatternYear matches year-precision prefixes: YYYY.
var datePatternYear = regexp.MustCompile(`^(\d{4})[\s_](.+)$`)

var monthNames = [13]string{
	"", "January", "February", "March", "April", "May", "June",
	"July", "August", "September", "October", "November", "December",
}

// parseDatePrefix is the shared implementation behind the public date-prefix functions.
// Returns the name stripped of its date prefix and a human-readable label.
// Both values are empty string when no recognized prefix is found.
func parseDatePrefix(name string) (stripped, label string) {
	// --- Day precision ---
	// Groups: [1]=startYear [2]=startMonth [3]=startDay
	//         [4][5][6]=endYYYY.MM.DD  [7][8]=endMM.DD  [9]=endDD  [10]=name
	if m := datePatternDay.FindStringSubmatch(name); m != nil {
		year, _ := strconv.Atoi(m[1])
		month, _ := strconv.Atoi(m[2])
		startDay, _ := strconv.Atoi(m[3])
		stripped = strings.TrimSpace(m[10])

		var endYear, endMonth, endDay int
		switch {
		case m[4] != "": // YYYY.MM.DD
			endYear, _ = strconv.Atoi(m[4])
			endMonth, _ = strconv.Atoi(m[5])
			endDay, _ = strconv.Atoi(m[6])
		case m[7] != "": // MM.DD (same year)
			endYear = year
			endMonth, _ = strconv.Atoi(m[7])
			endDay, _ = strconv.Atoi(m[8])
		case m[9] != "": // DD (same year and month)
			endYear, endMonth = year, month
			endDay, _ = strconv.Atoi(m[9])
		default: // single day
			endYear, endMonth, endDay = year, month, startDay
		}

		switch {
		case year == endYear && month == endMonth && startDay == endDay:
			label = fmt.Sprintf("%s %d, %d", monthNames[month], startDay, year)
		case year == endYear && month == endMonth:
			label = fmt.Sprintf("%s %d - %d, %d", monthNames[month], startDay, endDay, year)
		case year == endYear:
			label = fmt.Sprintf("%s %d - %s %d, %d", monthNames[month], startDay, monthNames[endMonth], endDay, year)
		default:
			label = fmt.Sprintf("%s %d, %d - %s %d, %d", monthNames[month], startDay, year, monthNames[endMonth], endDay, endYear)
		}
		return
	}

	// --- Month precision ---
	if m := datePatternMonth.FindStringSubmatch(name); m != nil {
		year, _ := strconv.Atoi(m[1])
		startMonth, _ := strconv.Atoi(m[2])
		endMonth := startMonth
		if m[3] != "" {
			endMonth, _ = strconv.Atoi(m[3])
		}
		stripped = strings.TrimSpace(m[4])

		if startMonth == endMonth {
			label = fmt.Sprintf("%s %d", monthNames[startMonth], year)
		} else {
			label = fmt.Sprintf("%s - %s, %d", monthNames[startMonth], monthNames[endMonth], year)
		}
		return
	}

	// --- Year range ---
	if m := datePatternYearRange.FindStringSubmatch(name); m != nil {
		startYear, _ := strconv.Atoi(m[1])
		endYear, _ := strconv.Atoi(m[2])
		stripped = strings.TrimSpace(m[3])
		label = fmt.Sprintf("%d - %d", startYear, endYear)
		return
	}

	// --- Year precision ---
	if m := datePatternYear.FindStringSubmatch(name); m != nil {
		year, _ := strconv.Atoi(m[1])
		stripped = strings.TrimSpace(m[2])
		label = strconv.Itoa(year)
		return
	}

	stripped = name
	return
}

// ExtractDatePrefixStr extracts a date prefix from name and returns both the
// stripped name and the raw prefix string (e.g. "2024.06.02-04"), or nil if
// no recognized prefix is found. Use this to obtain the storable raw form.
func ExtractDatePrefixStr(name string) (string, *string) {
	stripped, label := parseDatePrefix(name)
	if label == "" {
		return stripped, nil
	}
	prefix := strings.TrimRight(name[:len(name)-len(stripped)], " _")
	return stripped, &prefix
}

// ParseDateString parses a stored raw date string (as produced by scanning) and
// returns a human-readable label, or nil if the string is empty or unrecognized.
// Supported forms match those produced by ExtractDatePrefixStr: "YYYY",
// "YYYY-YYYY", "YYYY.MM", "YYYY.MM-MM", "YYYY.MM.DD", "YYYY.MM.DD-DD", etc.
func ParseDateString(s string) *string {
	if s == "" {
		return nil
	}
	_, label := parseDatePrefix(s + " x")
	if label == "" {
		return nil
	}
	return &label
}

// ExtractDatePrefix extracts a date prefix from name and returns the stripped name
// along with a human-readable date label (nil if no prefix found).
//
// Supported formats (most- to least-precise):
//   - YYYY.MM.DD[-DD]           same-month range,  e.g. "2024.06.02-04 Ithaca Trip"      → "June 2 - 4, 2024"
//   - YYYY.MM.DD[-MM.DD]        cross-month range, e.g. "2025.06.29-07.01 Road Trip"     → "June 29 - July 1, 2025"
//   - YYYY.MM.DD[-YYYY.MM.DD]   cross-year range,  e.g. "2025.12.29-2026.01.02 Holiday"  → "December 29, 2025 - January 2, 2026"
//   - YYYY.MM[-MM]              month range,        e.g. "2002.08-12 Spring Semester"     → "August - December, 2002"
//   - YYYY.MM                   single month,       e.g. "2005.05 Mayterm"               → "May 2005"
//   - YYYY-YYYY                 year range,         e.g. "1984-1987 Various"              → "1984 - 1987"
//   - YYYY                      single year,        e.g. "2004 Misc"                      → "2004"
func ExtractDatePrefix(name string) (string, *string) {
	stripped, label := parseDatePrefix(name)
	if label == "" {
		return stripped, nil
	}
	return stripped, &label
}

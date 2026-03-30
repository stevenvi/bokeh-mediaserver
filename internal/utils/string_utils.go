package utils

import "strings"

// GenerateSortName creates a sort-friendly name by moving common articles to the end.
func GenerateSortName(name string) string {
	lower := strings.ToLower(name)
	for _, article := range []string{"the ", "a ", "an "} {
		if strings.HasPrefix(lower, article) {
			prefix := name[:len(article)]
			rest := name[len(article):]
			return rest + ", " + strings.TrimSpace(prefix)
		}
	}
	return name
}

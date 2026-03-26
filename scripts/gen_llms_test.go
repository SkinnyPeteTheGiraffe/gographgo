package main

import (
	"strings"
	"testing"
)

func TestStripSection_HeadingAtStart(t *testing.T) {
	src := "## License\ntext\n## Keep\ncontent\n"
	got := stripSection(src, "## License")
	if strings.Contains(got, "## License") || strings.Contains(got, "text") {
		t.Fatalf("stripSection should remove heading section, got %q", got)
	}
	if !strings.Contains(got, "## Keep\ncontent\n") {
		t.Fatalf("stripSection should keep following sections, got %q", got)
	}
}

func TestStripSection_HeadingMissingReturnsSource(t *testing.T) {
	src := "# Title\n\n## Keep\ncontent\n"
	got := stripSection(src, "## Missing")
	if got != src {
		t.Fatalf("stripSection should keep source when heading missing\nwant: %q\ngot:  %q", src, got)
	}
}

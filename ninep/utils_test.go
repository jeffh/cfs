package ninep

import (
	"testing"
)

func TestNextDir(t *testing.T) {
	var tcs = []struct {
		path     string
		expected string
	}{
		{".", "."},
		{"/", "."},
		{"/a", "a"},
		{"a/", "a"},
		{"a", "a"},
		{"a/b", "a"},
		{"/a/b/c", "a"},
		{"./a/b/c/", "a"},
	}
	for _, tc := range tcs {
		actual := NextSegment(tc.path)
		if actual != tc.expected {
			t.Errorf("NextDir(%q) => %q, expected %q", tc.path, actual, tc.expected)
		}
	}
}

package ninep

import (
	"fmt"
	"regexp"
	"strings"
)

type Nothing struct{}

type MatchWith[X any] struct {
	Id    string
	Vars  []string
	Value X
}

type Match = MatchWith[Nothing]

type Mux[X any] struct {
	patterns []pattern[X]
}

// NewMux creates a new Mux to map paths to ids
func NewMux() *Mux[Nothing] { return NewMuxWith[Nothing]() }

// NewMuxWith creates a with a specific type associated to each pattern.
func NewMuxWith[X any]() *Mux[X] {
	return &Mux[X]{}
}

// Define defines a new pattern. Use As() to save the path.
func (m *Mux[X]) Define() *PatternBuilder[X] {
	return &PatternBuilder[X]{
		m: m,
	}
}

type PatternBuilder[X any] struct {
	tpl                   string
	optionalTrailingSlash bool
	value                 X
	id                    string
	groups                []string
	m                     *Mux[X]
}

// Path compiles a template into a regexp.
// It supports named variables in the form of {name} that will be captured in the Match's Vars.
//
// The syntax supports the following:
//   - {name} captures the segment as a variable (e.g. /foo/{name}/bar)
//   - {name*} captures zero or more segments as a variable (matches empty string as well)
//   - {name+} captures one or more segments as a variable
func (b *PatternBuilder[X]) Path(tpl string) *PatternBuilder[X] {
	var re strings.Builder
	var groups []string
	re.Grow(len(tpl) * 2)
	start := 0
	for i := 0; i < len(tpl); i++ {
		if i+1 < len(tpl) && tpl[i] == '{' {
			re.WriteString(regexp.QuoteMeta(tpl[start:i]))
			start = i + 1
			for j := i + 1; j < len(tpl); j++ {
				if tpl[j] == '}' {
					name := tpl[i+1 : j]
					if strings.HasSuffix(name, "*") {
						// name = name[:len(name)-1]
						re.WriteString("(.*)")
					} else if strings.HasSuffix(name, "+") {
						re.WriteString("(.+)")
					} else {
						re.WriteString("([^/]+)")
					}
					start = j + 1
					i = j
					groups = append(groups, name)
					break
				}
			}
		}
	}
	re.WriteString(regexp.QuoteMeta(tpl[start:]))
	b.tpl = re.String()
	b.groups = groups
	return b
}

// PathRegexp compiles a template into a regexp directly. Any groups are stored as Vars.
func (b *PatternBuilder[X]) PathRegexp(tpl string) *PatternBuilder[X] {
	b.tpl = tpl
	return b
}

// TrailSlash makes the pattern allow an optional trailing slash.
func (b *PatternBuilder[X]) TrailSlash() *PatternBuilder[X] {
	b.optionalTrailingSlash = true
	return b
}

func (b *PatternBuilder[X]) With(value X) *PatternBuilder[X] {
	b.value = value
	return b
}

// As saves the pattern to Mux as the given id.
func (b *PatternBuilder[X]) As(id string) *Mux[X] {
	if b.tpl == "" {
		panic("Path() must be defined")
	}
	tpl := b.tpl
	if b.optionalTrailingSlash {
		tpl += "/?"
	}
	b.id = id
	b.m.patterns = append(b.m.patterns, pattern[X]{
		regexp: regexp.MustCompile("^" + tpl + "$"),
		id:     b.id,
		value:  b.value,
		groups: b.groups,
	})
	return b.m
}

type pattern[X any] struct {
	regexp *regexp.Regexp
	id     string
	value  X
	groups []string
}

func (m *Mux[X]) List(path string) []MatchWith[X] {
	path = strings.TrimSuffix(path, "/") + "/"
	var result []MatchWith[X]
	for _, p := range m.patterns {
		// Convert regexp pattern to prefix by:
		// 1. Removing ^ from start and $ from end
		// 2. Taking everything up to first special character
		pattern := p.regexp.String()
		pattern = strings.TrimPrefix(pattern, "^")
		pattern = strings.TrimSuffix(pattern, "$")
		pattern = strings.TrimSuffix(pattern, "/?")

		// Find first special character
		specialIdx := strings.IndexAny(pattern, "({[.*+?\\")
		if specialIdx >= 0 {
			pattern = pattern[:specialIdx]
		}

		// Check if this pattern matches the prefix
		fmt.Printf("List.Compare: %q, %q => %v\n", pattern, path, strings.HasPrefix(pattern, path))
		if strings.HasPrefix(pattern, path) {
			if p.regexp.FindStringSubmatch(path) == nil {
				result = append(result, MatchWith[X]{
					Id:    p.id,
					Vars:  p.groups,
					Value: p.value,
				})
			}
		}
	}
	return result
}

// Match returns true if the path matches any pattern.
func (m *Mux[X]) Match(path string, result *MatchWith[X]) bool {
	for _, p := range m.patterns {
		matches := p.regexp.FindStringSubmatch(path)
		if matches != nil {
			if result != nil {
				result.Id = p.id
				result.Vars = matches[1:]
				result.Value = p.value
			}
			return true
		}
	}
	return false
}

// Lookup returns the id and vars for the given path. If no match is found, it returns the defaultId.
func (m *Mux[X]) Lookup(path string, defaultId string) *MatchWith[X] {
	var result MatchWith[X]
	if m.Match(path, &result) {
		return &result
	}
	return nil
}

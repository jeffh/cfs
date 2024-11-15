package ninep

import (
	"regexp"
	"strings"
)

type Match struct {
	Id    string
	Vars  []string
	Tags  []string
	Attrs map[string]string
}

type Mux struct {
	patterns []pattern
}

// NewMux creates a new Mux to map paths to ids
func NewMux() *Mux {
	return &Mux{}
}

// Define defines a new pattern. Use As() to save the path.
func (m *Mux) Define() *PatternBuilder {
	return &PatternBuilder{
		m: m,
	}
}

type PatternBuilder struct {
	tpl                   string
	optionalTrailingSlash bool
	tags                  []string
	attrs                 map[string]string
	id                    string
	m                     *Mux
}

// Path compiles a template into a regexp.
// It supports named variables in the form of {name} that will be captured in the Match's Vars.
//
// The syntax supports the following:
//   - {name} captures the segment as a variable (e.g. /foo/{name}/bar)
//   - {name*} captures zero or more segments as a variable (matches empty string as well)
//   - {name+} captures one or more segments as a variable
func (b *PatternBuilder) Path(tpl string) *PatternBuilder {
	var re strings.Builder
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
					break
				}
			}
		}
	}
	re.WriteString(regexp.QuoteMeta(tpl[start:]))
	b.tpl = re.String()
	return b
}

// PathRegexp compiles a template into a regexp directly. Any groups are stored as Vars.
func (b *PatternBuilder) PathRegexp(tpl string) *PatternBuilder {
	b.tpl = tpl
	return b
}

// TrailSlash makes the pattern allow an optional trailing slash.
func (b *PatternBuilder) TrailSlash() *PatternBuilder {
	b.optionalTrailingSlash = true
	return b
}

func (b *PatternBuilder) Tag(tag string) *PatternBuilder {
	b.tags = append(b.tags, tag)
	return b
}

func (b *PatternBuilder) Attr(key, value string) *PatternBuilder {
	if b.attrs == nil {
		b.attrs = make(map[string]string)
	}
	b.attrs[key] = value
	return b
}

// As saves the pattern to Mux as the given id.
func (b *PatternBuilder) As(id string) *Mux {
	if b.tpl == "" {
		panic("Path() must be defined")
	}
	tpl := b.tpl
	if b.optionalTrailingSlash {
		tpl += "/?"
	}
	b.id = id
	b.m.patterns = append(b.m.patterns, pattern{
		regexp: regexp.MustCompile("^" + tpl + "$"),
		id:     b.id,
		tags:   b.tags,
		attrs:  b.attrs,
	})
	return b.m
}

type pattern struct {
	regexp *regexp.Regexp
	id     string
	tags   []string
	attrs  map[string]string
}

// Match returns true if the path matches any pattern.
func (m *Mux) Match(path string, result *Match) bool {
	for _, p := range m.patterns {
		matches := p.regexp.FindStringSubmatch(path)
		if matches != nil {
			if result != nil {
				result.Id = p.id
				result.Vars = matches[1:]
				result.Tags = p.tags
			}
			return true
		}
	}
	return false
}

// Lookup returns the id and vars for the given path. If no match is found, it returns the defaultId.
func (m *Mux) Lookup(path string, defaultId string) (id string, vars []string) {
	var result Match
	if m.Match(path, &result) {
		return result.Id, result.Vars
	}
	return defaultId, nil
}

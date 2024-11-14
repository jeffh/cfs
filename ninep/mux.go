package ninep

import (
	"regexp"
	"strings"
)

type Match struct {
	Id   string
	Vars []string
}

type Mux struct {
	patterns []pattern
}

func NewMux() *Mux {
	return &Mux{}
}

func (m *Mux) Define() *PatternBuilder {
	return &PatternBuilder{
		m: m,
	}
}

type PatternBuilder struct {
	tpl *regexp.Regexp
	id  string
	m   *Mux
}

// Path compiles a template into a regexp.
// It supports named variables in the form of {name} that will be captured in the Match's Vars.
func (b *PatternBuilder) Path(tpl string) *PatternBuilder {
	var re strings.Builder
	re.Grow(len(tpl) * 2)
	re.WriteString("^")
	start := 0
	for i := 0; i < len(tpl); i++ {
		if i+1 < len(tpl) && tpl[i] == '{' {
			re.WriteString(regexp.QuoteMeta(tpl[start:i]))
			start = i + 1
			for j := i + 1; j < len(tpl); j++ {
				if tpl[j] == '}' {
					re.WriteString("([^/]+)")
					start = j + 1
					i = j
					break
				}
			}
		}
	}
	re.WriteString(regexp.QuoteMeta(tpl[start:]))
	re.WriteString("$")
	b.tpl = regexp.MustCompile(re.String())
	return b
}

func (b *PatternBuilder) PathRegexp(tpl string) *PatternBuilder {
	b.tpl = regexp.MustCompile(tpl)
	return b
}

func (b *PatternBuilder) As(id string) *Mux {
	if b.tpl == nil {
		panic("Path() must be defined")
	}
	b.id = id
	b.m.patterns = append(b.m.patterns, pattern{
		regexp: b.tpl,
		id:     b.id,
	})
	return b.m
}

type pattern struct {
	regexp *regexp.Regexp
	id     string
}

func (m *Mux) Match(path string, result *Match) bool {
	for _, p := range m.patterns {
		matches := p.regexp.FindStringSubmatch(path)
		if matches != nil {
			result.Id = p.id
			result.Vars = matches[1:]
			return true
		}
	}
	return false
}

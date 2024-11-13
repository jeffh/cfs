package ndb

import (
	"slices"
	"strconv"
	"strings"
)

type Tuple struct {
	Attr, Val string
}

type Record []Tuple

func (r Record) Keys() []string {
	keys := make([]string, 0, len(r))
	for _, t := range r {
		if !slices.Contains(keys, t.Attr) {
			keys = append(keys, t.Attr)
		}
	}
	return keys
}

func (r Record) AsMap() map[string][]string {
	m := make(map[string][]string, len(r))
	for _, t := range r {
		m[t.Attr] = append(m[t.Attr], t.Val)
	}
	return m
}

func (r Record) Lookup(attr string) (string, bool) {
	for _, t := range r {
		if t.Attr == attr {
			return t.Val, true
		}
	}
	return "", false
}

func (r Record) Get(attr string) string {
	for _, t := range r {
		if t.Attr == attr {
			return t.Val
		}
	}
	return ""
}

func (r Record) GetAll(attr string) []string {
	var results []string
	for _, t := range r {
		if t.Attr == attr {
			results = append(results, t.Val)
		}
	}
	return results
}

func (r Record) String() string {
	var sb strings.Builder
	for i, t := range r {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(t.Attr)
		sb.WriteString("=")
		sb.WriteString(strconv.Quote(t.Val))
	}
	return sb.String()
}

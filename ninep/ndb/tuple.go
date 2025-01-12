package ndb

import (
	"slices"
	"sort"
	"strconv"
	"strings"
)

type Tuple struct {
	Attr, Val string
}

type Record []Tuple

func (r *Record) zero() { *r = (*r)[:0] }

// MapToRecord converts a map of strings to a Record. The keys of the map are the attributes and the values are the values.
// Ordering is by sorted keys.
func MapToRecord(m map[string]string) Record {
	r := make(Record, 0, len(m))
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r = append(r, Tuple{k, m[k]})
	}
	return r
}

// MapSliceToRecord converts a map of strings to a Record. The keys of the map are the attributes and the values are the values.
// Ordering is by sorted keys.
func MapSliceToRecord(m map[string][]string) Record {
	r := make(Record, 0, len(m))
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vs := m[k]
		for _, v := range vs {
			r = append(r, Tuple{k, v})
		}
	}
	return r
}

// SliceToRecord creates a Record from a list of attribute-value pairs. The number of arguments must be even.
func SliceToRecord(s []string) Record {
	if len(s)%2 == 1 {
		panic("SliceToRecord() requires an even number of arguments")
	}
	r := make(Record, len(s)/2)
	j := 0
	for i := 0; i < len(s); i += 2 {
		r[j] = Tuple{s[i], s[i+1]}
		j++
	}
	return r
}

// MakeRecord creates a Record from a list of attribute-value pairs. The number of arguments must be even.
func MakeRecord(avPairs ...string) Record {
	if len(avPairs)%2 == 1 {
		panic("MakeRecord() requires an even number of arguments")
	}
	r := make(Record, len(avPairs)/2)
	j := 0
	for i := 0; i < len(avPairs); i += 2 {
		r[j] = Tuple{avPairs[i], avPairs[i+1]}
		j++
	}
	return r
}

// ParseRecord parses a single record from a string. The string should be a line containing all the attributes and values.
func ParseRecord(line string) (Record, error) {
	var results Record
	err := parseRecord([]byte(line), &results)
	return results, err
}

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
		if needsQuote(t.Val) {
			sb.WriteString(strconv.Quote(t.Val))
		} else {
			sb.WriteString(t.Val)
		}
	}
	return sb.String()
}

func needsQuote(s string) bool {
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '=' || r > 128 {
			return true
		}
	}
	return false
}

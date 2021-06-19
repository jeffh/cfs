package kvp

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

type Map map[string][]string

func (kv Map) SortedKeyPairs() [][2]string {
	cnt := 0
	keys := make([]string, 0, len(kv))
	for k, vs := range kv {
		keys = append(keys, k)
		cnt += len(vs)
	}
	sort.Strings(keys)
	pairs := make([][2]string, 0, cnt)

	for _, k := range keys {
		vs := kv[k]
		for _, v := range vs {
			pairs = append(pairs, [2]string{k, v})
		}
	}
	return pairs
}

func (kv Map) KeyPairs() [][2]string {
	cnt := 0
	for _, vs := range kv {
		cnt += len(vs)
	}
	pairs := make([][2]string, 0, cnt)
	for k, vs := range kv {
		for _, v := range vs {
			pairs = append(pairs, [2]string{k, v})
		}
	}
	return pairs
}

func (kv Map) GetAll(k string) []string {
	v, ok := kv[k]
	if ok {
		return v
	}
	return nil
}

func (kv Map) GetAllInt64s(k string) []int64 {
	v, ok := kv[k]
	if ok {
		i := make([]int64, 0, len(v))
		for _, value := range v {
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				n = 0
			}
			i = append(i, n)
		}
		return i
	}
	return nil
}
func (kv Map) GetAllInts(k string) []int {
	v, ok := kv[k]
	if ok {
		i := make([]int, 0, len(v))
		for _, value := range v {
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				n = 0
			}
			i = append(i, int(n))
		}
		return i
	}
	return nil
}

func (kv Map) GetOne(k string) string {
	v, ok := kv[k]
	if ok && len(v) > 0 {
		return v[0]
	}
	return ""
}

func strBool(s string) bool {
	return s == "true" || s == "t" || s == "yes" || s == "y" || s == "ok"
}

func (kv Map) Has(k string) bool { _, ok := kv[k]; return ok }

func (kv Map) GetOneBool(k string) bool { return strBool(kv.GetOne(k)) }
func (kv Map) GetOneInt64(k string) int64 {
	n, err := strconv.ParseInt(kv.GetOne(k), 10, 64)
	if err != nil {
		n = 0
	}
	return n
}
func (kv Map) GetOnePrefix(prefix string) map[string]string {
	m := make(map[string]string)
	size := len(prefix)
	for k, v := range kv {
		if strings.HasPrefix(k, prefix) {
			key := k[size:]
			if len(v) > 0 {
				m[key] = v[0]
			}
		}
	}
	return m
}
func (kv Map) GetAllPrefix(prefix string) Map {
	m := make(Map)
	size := len(prefix)
	for k, v := range kv {
		if strings.HasPrefix(k, prefix) {
			key := k[size:]
			m[key] = v
		}
	}
	return m
}
func (kv Map) GetOnePrefixOrNil(prefix string) map[string]string {
	var m map[string]string
	size := len(prefix)
	for k, v := range kv {
		if strings.HasPrefix(k, prefix) {
			if m == nil {
				m = make(map[string]string)
			}
			key := k[size:]
			if len(v) > 0 {
				m[key] = v[0]
			}
		}
	}
	return m
}
func (kv Map) GetAllPrefixOrNil(prefix string) Map {
	var m Map
	size := len(prefix)
	for k, v := range kv {
		if strings.HasPrefix(k, prefix) {
			if m == nil {
				m = make(Map)
			}
			key := k[size:]
			m[key] = v
		}
	}
	return m
}
func (kv Map) Flatten() map[string]string {
	m := make(map[string]string)
	for k, v := range kv {
		if len(v) > 0 {
			m[k] = v[0]
		}
	}
	return m
}

func removeQuotesIfNeeded(v string) string {
	// v = strings.TrimSpace(v)
	Lk := len(v)
	if Lk >= 2 && v[0] == '"' && v[Lk-1] == '"' {
		idx := strings.IndexAny(v[1:Lk-1], "\"\\")

		if idx == -1 {
			return v[1 : Lk-1]
		} else {
			sb := make([]rune, 0, Lk-2)
			isEscaping := false
			for _, r := range v[1 : Lk-1] {
				// technically, we should handle parsing: \"
				// but this is "good enough"
				if r == '\\' {
					if isEscaping {
						isEscaping = false
					} else {
						isEscaping = true
						continue
					}
				}

				sb = append(sb, r)
			}
			return string(sb)
		}
	}
	return v
}

func (cfg *Config) parseString(value string) ([]string, error) {
	type span struct {
		start, end int
	}
	lastQuote := rune(0)
	isSeparator := cfg.IsPairSeparator
	start := 0
	var spans []span
	var err error
	for i, c := range value {
		switch {
		case c == lastQuote:
			if i == 0 || value[i-1] != '\\' {
				lastQuote = rune(0)
			}
		case lastQuote != rune(0):
		case unicode.In(c, unicode.Quotation_Mark):
			lastQuote = c
		default:
			if isSeparator(c) {
				if start == -1 {
					start = i
				} else {
					spans = append(spans, span{start, i})
					start = i + 1
				}
			}

		}
	}
	if start != len(value) {
		spans = append(spans, span{start, len(value)})

		if lastQuote != rune(0) {
			err = fmt.Errorf("unclosed quote %#v at offset %d of %#v", string(lastQuote), start, value)
		}
	}

	strings := make([]string, len(spans))
	for i, sp := range spans {
		strings[i] = value[sp.start:sp.end]
	}

	return strings, err
}
func (cfg *Config) mustParseString(value string) []string {
	r, err := cfg.parseString(value)
	if err != nil {
		log.Printf("[warn]: %s", err)
	}
	return r
}

func (cfg *Config) ParseKeyPairs(value string) [][2]string {
	items := cfg.mustParseString(value)

	pairs := make([][2]string, 0, len(items))
	for _, item := range items {
		x := strings.Split(item, string(cfg.KVSeparator))
		var v string
		if len(x) > 1 {
			v = removeQuotesIfNeeded(x[1])
		}
		pairs = append(pairs, [2]string{removeQuotesIfNeeded(x[0]), v})
	}

	return pairs
}
func ParseKeyPairs(value string) [][2]string { return DefaultConfig.ParseKeyPairs(value) }

func (cfg *Config) ParseKeyValues(value string) Map {
	lastQuote := rune(0)
	isSeparator := cfg.IsPairSeparator

	items := strings.FieldsFunc(value, func(c rune) bool {
		switch {
		case c == '\\':
			return false
		case c == lastQuote:
			lastQuote = rune(0)
			return false
		case lastQuote != rune(0):
			return false
		case unicode.In(c, unicode.Quotation_Mark):
			return false
		default:
			return isSeparator(c)

		}
	})

	m := make(Map)
	for _, item := range items {
		x := strings.Split(item, string(cfg.KVSeparator))
		k := removeQuotesIfNeeded(x[0])
		var v string
		if len(x) > 1 {
			v = removeQuotesIfNeeded(x[1])
		}

		m[k] = append(m[k], v)
	}

	return m
}
func ParseKeyValues(value string) Map { return DefaultConfig.ParseKeyValues(value) }

func (cfg *Config) ProcessKeyValuesLineLoop(r io.Reader, maxLineLen int, do func(k Map, err error) (ok bool)) {
	if maxLineLen <= 0 {
		maxLineLen = 4096
	}
	in := bufio.NewReaderSize(r, maxLineLen)
	for {
		line, isPrefix, err := in.ReadLine()
		if err != nil {
			if err != io.EOF {
				if do(nil, err) {
					continue
				}
			}
			return
		}
		if isPrefix {
			// the line is too long, skip
			for isPrefix {
				_, isPrefix, err = in.ReadLine()
				if err != nil {
					if err != io.EOF {
						if do(nil, err) {
							continue
						}
					}
					return
				}
			}
		} else {
			kv := cfg.ParseKeyValues(string(line))
			if do(kv, nil) {
				continue
			}
			return
		}
	}
}

func ProcessKeyValuesLineLoop(r io.Reader, maxLineLen int, do func(k Map, err error) (ok bool)) {
	DefaultConfig.ProcessKeyValuesLineLoop(r, maxLineLen, do)
}

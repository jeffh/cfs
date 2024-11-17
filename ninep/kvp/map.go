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

func (kv Map) SortedKeys() []string {
	cnt := 0
	emptyKeys := make([]string, 0, len(kv))
	keys := make([]string, 0, len(kv))
	for k, vs := range kv {
		if len(vs) == 1 && vs[0] == "" {
			emptyKeys = append(emptyKeys, k)
		} else {
			keys = append(keys, k)
		}
		cnt += len(vs)
	}
	sort.Strings(emptyKeys)
	sort.Strings(keys)
	pairs := make([]string, 0, cnt)

	pairs = append(pairs, emptyKeys...)
	pairs = append(pairs, keys...)
	return pairs
}

func (kv Map) SortedKeyPairs() [][2]string {
	cnt := 0
	emptyKeys := make([]string, 0, len(kv))
	keys := make([]string, 0, len(kv))
	for k, vs := range kv {
		if len(vs) == 1 && vs[0] == "" {
			emptyKeys = append(emptyKeys, k)
		} else {
			keys = append(keys, k)
		}
		cnt += len(vs)
	}
	sort.Strings(emptyKeys)
	sort.Strings(keys)
	pairs := make([][2]string, 0, cnt)

	for _, k := range emptyKeys {
		vs := kv[k]
		for _, v := range vs {
			pairs = append(pairs, [2]string{k, v})
		}
	}
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
			if err == nil {
				i = append(i, n)
			}
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

func (kv Map) HasOne(k string) bool {
	v, ok := kv[k]
	if ok && len(v) > 0 {
		return true
	}
	return false
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

func unescape(v string) string {
	Lk := len(v)
	if Lk >= 2 && unicode.In(rune(v[0]), unicode.Quotation_Mark) && v[Lk-1] == v[0] {
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

func (cfg *Config) ParseKeyPairs(value string) ([][2]string, error) {
	items, err := cfg.parseString(value)

	pairs := make([][2]string, 0, len(items))
	for _, item := range items {
		x := strings.Split(item, string(cfg.KVSeparator))
		var v string
		if len(x) > 1 {
			v = unescape(x[1])
		}
		pairs = append(pairs, [2]string{unescape(x[0]), v})
	}

	return pairs, err
}

func (cfg *Config) MustParseKeyPairs(value string) [][2]string {
	items := cfg.mustParseString(value)

	pairs := make([][2]string, 0, len(items))
	for _, item := range items {
		x := strings.Split(item, string(cfg.KVSeparator))
		var v string
		if len(x) > 1 {
			v = unescape(x[1])
		}
		pairs = append(pairs, [2]string{unescape(x[0]), v})
	}

	return pairs
}

func ParseKeyPairs(value string) ([][2]string, error) { return DefaultConfig.ParseKeyPairs(value) }
func MustParseKeyPairs(value string) [][2]string      { return DefaultConfig.MustParseKeyPairs(value) }

func (cfg *Config) ParseKeyValues(value string) (Map, error) {
	items, err := cfg.parseString(value)

	m := make(Map)
	for _, item := range items {
		x := strings.Split(item, string(cfg.KVSeparator))
		k := unescape(x[0])
		var v string
		if len(x) > 1 {
			v = unescape(x[1])
		}

		m[k] = append(m[k], v)
	}

	return m, err
}
func (cfg *Config) MustParseKeyValues(value string) Map {
	items := cfg.mustParseString(value)

	m := make(Map)
	for _, item := range items {
		x := strings.Split(item, string(cfg.KVSeparator))
		k := unescape(x[0])
		var v string
		if len(x) > 1 {
			v = unescape(x[1])
		}

		m[k] = append(m[k], v)
	}

	return m
}
func ParseKeyValues(value string) (Map, error) { return DefaultConfig.ParseKeyValues(value) }
func MustParseKeyValues(value string) Map      { return DefaultConfig.MustParseKeyValues(value) }

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
			kv := cfg.MustParseKeyValues(string(line))
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

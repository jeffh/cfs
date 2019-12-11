package ninep

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"
)

func keyValueNeedsEscape(s string) bool {
	for _, r := range s {
		if r == ' ' || r == '=' || r == '"' {
			return true
		}
	}
	return false
}

func KeyPair(key, value string) string {
	if keyValueNeedsEscape(key) || keyValueNeedsEscape(value) {
		return fmt.Sprintf("%#v=%#v", key, value)
	} else {
		return fmt.Sprintf("%v=%v", key, value)
	}
}

func KeyPairs(pairs [][2]string) string {
	res := make([]string, len(pairs))
	for i, p := range pairs {
		if keyValueNeedsEscape(p[0]) || keyValueNeedsEscape(p[1]) {
			res[i] = fmt.Sprintf("%#v=%#v", p[0], p[1])
		} else {
			res[i] = fmt.Sprintf("%v=%v", p[0], p[1])
		}
	}
	return strings.Join(res, " ")
}

func NonEmptyKeyPairs(pairs [][2]string) string {
	res := make([]string, 0, len(pairs))
	for _, p := range pairs {
		if p[1] != "" {
			if keyValueNeedsEscape(p[0]) || keyValueNeedsEscape(p[1]) {
				res = append(res, fmt.Sprintf("%#v=%#v", p[0], p[1]))
			} else {
				res = append(res, fmt.Sprintf("%v=%v", p[0], p[1]))
			}
		}
	}
	return strings.Join(res, " ")
}

func KeyValues(kvs map[string]string) string {
	res := make([]string, 0, len(kvs))
	for k, v := range kvs {
		if keyValueNeedsEscape(k) || keyValueNeedsEscape(v) {
			res = append(res, fmt.Sprintf("%#v=%#v", k, v))
		} else {
			res = append(res, fmt.Sprintf("%v=%v", k, v))
		}
	}
	return strings.Join(res, " ")
}

type KVMap map[string][]string

func (kv KVMap) GetAll(k string) []string {
	v, _ := kv[k]
	return v
}

func (kv KVMap) GetAllInt64s(k string) []int64 {
	v, _ := kv[k]
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
func (kv KVMap) GetAllInts(k string) []int {
	v, _ := kv[k]
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

func (kv KVMap) GetOne(k string) string {
	v, _ := kv[k]
	if len(v) > 0 {
		return v[0]
	}
	return ""
}

func strBool(s string) bool {
	return s == "true" || s == "t" || s == "yes" || s == "y" || s == "ok"
}

func (kv KVMap) Has(k string) bool { _, ok := kv[k]; return ok }

func (kv KVMap) GetOneBool(k string) bool { return strBool(kv.GetOne(k)) }
func (kv KVMap) GetOneInt64(k string) int64 {
	n, err := strconv.ParseInt(kv.GetOne(k), 10, 64)
	if err != nil {
		n = 0
	}
	return n
}
func (kv KVMap) GetOnePrefix(prefix string) map[string]string {
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
func (kv KVMap) GetAllPrefix(prefix string) KVMap {
	m := make(KVMap)
	size := len(prefix)
	for k, v := range kv {
		if strings.HasPrefix(k, prefix) {
			key := k[size:]
			m[key] = v
		}
	}
	return m
}
func (kv KVMap) GetOnePrefixOrNil(prefix string) map[string]string {
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
func (kv KVMap) GetAllPrefixOrNil(prefix string) KVMap {
	var m KVMap
	size := len(prefix)
	for k, v := range kv {
		if strings.HasPrefix(k, prefix) {
			if m == nil {
				m = make(KVMap)
			}
			key := k[size:]
			m[key] = v
		}
	}
	return m
}
func (kv KVMap) Flatten() map[string]string {
	m := make(map[string]string)
	for k, v := range kv {
		if len(v) > 0 {
			m[k] = v[0]
		}
	}
	return m
}

func ParseKeyValues(value string) KVMap {
	lastQuote := rune(0)

	items := strings.FieldsFunc(value, func(c rune) bool {
		switch {
		case c == lastQuote:
			lastQuote = rune(0)
			return false
		case lastQuote != rune(0):
			return false
		case unicode.In(c, unicode.Quotation_Mark):
			lastQuote = c
			return false
		default:
			return unicode.IsSpace(c)

		}
	})

	m := make(KVMap)
	for _, item := range items {
		x := strings.Split(item, "=")
		k := x[0]
		m[k] = append(m[k], x[1])
	}

	return m
}

func ProcessKeyValuesLineLoop(r io.Reader, maxLineLen int, do func(k KVMap, err error) (ok bool)) {
	if maxLineLen == 0 {
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
			kv := ParseKeyValues(string(line))
			if do(kv, nil) {
				continue
			}
			return
		}
	}
}

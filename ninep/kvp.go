package ninep

import (
	"fmt"
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

func (kv KVMap) GetOne(k string) string {
	v, _ := kv[k]
	if len(v) > 0 {
		return v[0]
	}
	return ""
}

func strBool(s string) bool {
	return s == "true" || s == "t" || s == "yes" || s == "y"
}

func (kv KVMap) GetOneBool(k string) bool { return strBool(kv.GetOne(k)) }
func (kv KVMap) GetOneInt64(k string) int64 {
	n, err := strconv.ParseInt(kv.GetOne(k), 10, 64)
	if err != nil {
		n = 0
	}
	return n
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

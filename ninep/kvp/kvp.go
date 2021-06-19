package kvp

import (
	"fmt"
	"strings"
	"unicode"
)

type Config struct {
	OutputPairSeparator rune
	KVSeparator         rune
	IsPairSeparator     func(r rune) bool
}

var DefaultConfig = Config{
	OutputPairSeparator: ' ',
	KVSeparator:         '=',
	IsPairSeparator:     unicode.IsSpace,
}

func (cfg *Config) keyValueNeedsEscape(s string) bool {
	pairSep, kvSep := cfg.OutputPairSeparator, cfg.KVSeparator
	for _, r := range s {
		if r == pairSep || r == kvSep || r == '"' {
			return true
		}
	}
	return false
}

func (cfg *Config) KeyPair(key, value string) string {
	if cfg.keyValueNeedsEscape(key) || cfg.keyValueNeedsEscape(value) {
		return fmt.Sprintf("%#v%c%#v", key, cfg.KVSeparator, value)
	} else {
		return fmt.Sprintf("%v%c%v", key, cfg.KVSeparator, value)
	}
}

func (cfg *Config) KeyPairs(pairs [][2]string) string {
	res := make([]string, len(pairs))
	for i, p := range pairs {
		if cfg.keyValueNeedsEscape(p[0]) || cfg.keyValueNeedsEscape(p[1]) {
			res[i] = fmt.Sprintf("%#v%c%#v", p[0], cfg.KVSeparator, p[1])
		} else {
			res[i] = fmt.Sprintf("%v%c%v", p[0], cfg.KVSeparator, p[1])
		}
	}
	return strings.Join(res, string(cfg.OutputPairSeparator))
}

func (cfg *Config) NonEmptyKeyPairs(pairs [][2]string) string {
	res := make([]string, 0, len(pairs))
	for _, p := range pairs {
		if p[1] != "" {
			if cfg.keyValueNeedsEscape(p[0]) || cfg.keyValueNeedsEscape(p[1]) {
				res = append(res, fmt.Sprintf("%#v%c%#v", p[0], cfg.KVSeparator, p[1]))
			} else {
				res = append(res, fmt.Sprintf("%v%c%v", p[0], cfg.KVSeparator, p[1]))
			}
		}
	}
	return strings.Join(res, string(cfg.OutputPairSeparator))
}

func (cfg *Config) KeyValues(kvs map[string]string) string {
	res := make([]string, 0, len(kvs))
	for k, v := range kvs {
		if cfg.keyValueNeedsEscape(k) || cfg.keyValueNeedsEscape(v) {
			res = append(res, fmt.Sprintf("%#v%c%#v", k, cfg.KVSeparator, v))
		} else {
			res = append(res, fmt.Sprintf("%v%c%v", k, cfg.KVSeparator, v))
		}
	}
	return strings.Join(res, string(cfg.OutputPairSeparator))
}

func KeyPair(key, value string) string          { return DefaultConfig.KeyPair(key, value) }
func KeyPairs(pairs [][2]string) string         { return DefaultConfig.KeyPairs(pairs) }
func NonEmptyKeyPairs(pairs [][2]string) string { return DefaultConfig.NonEmptyKeyPairs(pairs) }
func KeyValues(kvs map[string]string) string    { return DefaultConfig.KeyValues(kvs) }

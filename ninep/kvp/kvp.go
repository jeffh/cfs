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
	keyNeedsEscape := cfg.keyValueNeedsEscape(key)
	if len(value) != 0 {
		valueNeedsEscape := cfg.keyValueNeedsEscape(value)
		if keyNeedsEscape && valueNeedsEscape {
			return fmt.Sprintf("%#v%c%#v", key, cfg.KVSeparator, value)
		} else if keyNeedsEscape {
			return fmt.Sprintf("%#v%c%s", key, cfg.KVSeparator, value)
		} else if valueNeedsEscape {
			return fmt.Sprintf("%s%c%#v", key, cfg.KVSeparator, value)
		} else {
			return fmt.Sprintf("%s%c%s", key, cfg.KVSeparator, value)
		}
	} else {
		if keyNeedsEscape {
			return fmt.Sprintf("%#v", key)
		} else {
			return key
		}
	}
}

func (cfg *Config) KeyPairs(pairs [][2]string) string {
	res := make([]string, len(pairs))
	for i, p := range pairs {
		res[i] = cfg.KeyPair(p[0], p[1])
	}
	return strings.Join(res, string(cfg.OutputPairSeparator))
}

func (cfg *Config) NonEmptyKeyPairs(pairs [][2]string) string {
	res := make([]string, 0, len(pairs))
	for _, p := range pairs {
		if p[1] != "" {
			res = append(res, cfg.KeyPair(p[0], p[1]))
		}
	}
	return strings.Join(res, string(cfg.OutputPairSeparator))
}

func (cfg *Config) KeyValues(kvs map[string]string) string {
	res := make([]string, 0, len(kvs))
	for k, v := range kvs {
		res = append(res, cfg.KeyPair(k, v))
	}
	return strings.Join(res, string(cfg.OutputPairSeparator))
}

func KeyPair(key, value string) string          { return DefaultConfig.KeyPair(key, value) }
func KeyPairs(pairs [][2]string) string         { return DefaultConfig.KeyPairs(pairs) }
func NonEmptyKeyPairs(pairs [][2]string) string { return DefaultConfig.NonEmptyKeyPairs(pairs) }
func KeyValues(kvs map[string]string) string    { return DefaultConfig.KeyValues(kvs) }

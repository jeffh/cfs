package kvp

import (
	"reflect"
	"testing"
)

const Y = true
const N = false

var testCases = []struct {
	Passes       bool
	ExactReverse bool
	Input        string
	Output       [][2]string
	Desc         string
}{
	{Y, Y, "a=1", [][2]string{{"a", "1"}}, "single key"},
	{Y, Y, "a", [][2]string{{"a", ""}}, "single no-value key"},
	{Y, N, "a=", [][2]string{{"a", ""}}, "single no-value key"},
	{Y, Y, "a=1 b=2", [][2]string{{"a", "1"}, {"b", "2"}}, "multiple keys"},
	{Y, Y, "a=1 b=3 b=2", [][2]string{{"a", "1"}, {"b", "3"}, {"b", "2"}}, "repeated keys"},
	{Y, N, "a=1 b= b=2 c", [][2]string{{"a", "1"}, {"b", ""}, {"b", "2"}, {"c", ""}}, "no-value key"},
	{Y, Y, "a c", [][2]string{{"a", ""}, {"c", ""}}, "no-value key"},
	{Y, Y, "cat", [][2]string{{"cat", ""}}, "no-value key"},
	{Y, Y, "cookies apple=1 b=3 b=2", [][2]string{{"cookies", ""}, {"apple", "1"}, {"b", "3"}, {"b", "2"}}, "longer keys"},
	{Y, Y, "\"stock footage\"", [][2]string{{"stock footage", ""}}, "quoted keys"},
	{Y, Y, "\"is public\"=yes \"stock exchange\"=NASDAQ", [][2]string{{"is public", "yes"}, {"stock exchange", "NASDAQ"}}, "many quoted keys"},
	{Y, Y, "name=\"John Doe\"", [][2]string{{"name", "John Doe"}}, "quoted values"},
	{Y, N, "\"full name\"=\"John Doe\" ", [][2]string{{"full name", "John Doe"}}, "quoted values"},
	{N, N, "$=1 _=42 \"\\\"\"=' ", [][2]string{{"$", "1"}, {"_", "42"}, {"\"", "' "}}, "weird keys"},
	{Y, N, "'stock footage'", [][2]string{{"stock footage", ""}}, "quoted keys"},
	{Y, N, "'is public'=yes 'stock exchange'=NASDAQ", [][2]string{{"is public", "yes"}, {"stock exchange", "NASDAQ"}}, "many quoted keys"},
	{Y, N, "name='John Doe'", [][2]string{{"name", "John Doe"}}, "quoted values"},
	{Y, N, "'full name'='John Doe' ", [][2]string{{"full name", "John Doe"}}, "quoted values"},
	{N, N, "$=1 _=42 '\"'=' ", [][2]string{{"$", "1"}, {"_", "42"}, {"\"", "' "}}, "weird keys"},
}

func TestSerializationOfKeyPairs(t *testing.T) {
	for i, tc := range testCases {
		raw := tc.Input
		reason := tc.Desc
		if tc.ExactReverse {
			actual := KeyPairs(MustParseKeyPairs(raw))
			if !reflect.DeepEqual(actual, raw) {
				t.Fatalf("[%d] %s: Expected %#v, got %#v", i, reason, raw, actual)
			}
		}
	}
}

func TestSerializationOfKeyMap(t *testing.T) {
	for i, tc := range testCases {
		raw := tc.Input
		reason := tc.Desc
		if tc.ExactReverse {
			actual := KeyPairs(MustParseKeyValues(raw).SortedKeyPairs())
			if !reflect.DeepEqual(actual, raw) {
				t.Fatalf("[%d] %s: Expected %#v, got %#v", i, reason, raw, actual)
			}
		}
	}
}

func TestParsingKeyPairs(t *testing.T) {
	for _, tc := range testCases {
		raw := tc.Input
		reason := tc.Desc
		expected := tc.Output
		if tc.Passes {
			actual := MustParseKeyPairs(raw)
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("%s: Expected %#v, got %#v (input=%#v)", reason, expected, actual, raw)
			}
		} else {
			actual, err := ParseKeyPairs(raw)
			if err == nil {
				t.Fatalf("Expected parse error, got: %s", err)
			}
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("%s: Expected %#v, got %#v (input=%#v)", reason, expected, actual, raw)
			}
		}
	}
}

func TestParsingKeyValues(t *testing.T) {
	for _, tc := range testCases {
		raw := tc.Input
		reason := tc.Desc
		expected := make(Map)
		for _, pairs := range tc.Output {
			strs := expected[pairs[0]]
			expected[pairs[0]] = append(strs, pairs[1])
		}
		if tc.Passes {
			actual := MustParseKeyValues(raw)
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("%s: Expected %#v, got %#v (input=%#v)", reason, expected, actual, raw)
			}
		} else {
			actual, err := ParseKeyValues(raw)
			if err == nil {
				t.Fatalf("Expected parse error, got: %s", err)
			}
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("%s: Expected %#v, got %#v (input=%#v)", reason, expected, actual, raw)
			}
		}
	}
}

func TestParsingValues(t *testing.T) {
	m := Map{
		"number":      []string{"1"},
		"strings":     []string{"a", "b"},
		"numbers":     []string{"2", "3"},
		"bool1":       []string{"true"},
		"bool2":       []string{"yes"},
		"bool3":       []string{"t"},
		"bool4":       []string{"y"},
		"bool5":       []string{"ok"},
		"no-value":    []string{""},
		"person.name": []string{"John", "Smith"},
		"person.age":  []string{"42"},
	}

	t.Run("Has Key", func(t *testing.T) {
		for k := range m {
			if !m.Has(k) {
				t.Fatalf("Expected to have key %#v", k)
			}
		}
		if m.Has("MISSING_KEY") {
			t.Fatalf("Expected to not have key %#v", "MISSING_KEY")
		}
	})

	t.Run("Prefix", func(t *testing.T) {
		{
			key := "person."
			actual := m.GetAllPrefix(key)
			expected := Map{
				"name": []string{"John", "Smith"},
				"age":  []string{"42"},
			}
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("Expected GetAllPrefix(%#v) to return %#v, got %#v", key, expected, actual)
			}
		}
		{
			key := "person."
			actual := m.GetOnePrefix(key)
			expected := map[string]string{
				"name": "John",
				"age":  "42",
			}
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("Expected GetOnePrefix(%#v) to return %#v, got %#v", key, expected, actual)
			}
		}
	})

	t.Run("Strings", func(t *testing.T) {
		{
			actual := m.GetAll("strings")
			expected := []string{"a", "b"}
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("Expected GetAll(%#v) to return %#v, got %#v", "strings", expected, actual)
			}
		}
		{
			actual := m.GetOne("strings")
			expected := "a"
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("Expected GetOne(%#v) to return %#v, got %#v", "strings", expected, actual)
			}
		}
	})

	t.Run("Booleans", func(t *testing.T) {
		{
			keys := []string{"bool1", "bool2", "bool3", "bool4", "bool5"}
			for _, key := range keys {
				actual := m.GetOneBool(key)
				if !actual {
					t.Fatalf("Expected GetOneBool(%#v) to be true, got: %v", key, actual)
				}
			}
		}

		{
			keys := []string{"number", "strings", "numbers"}
			for _, key := range keys {
				actual := m.GetOneBool(key)
				if actual {
					t.Fatalf("Expected GetOneBool(%#v) to be false, got: %v", key, actual)
				}
			}
		}

		{
			values := m.GetAllInt64s("strings")
			if len(values) != 0 {
				t.Fatalf("Expected no items when calling GetAllInt64s(%#v), got: %#v", "strings", values)
			}
		}
	})

	t.Run("Numbers", func(t *testing.T) {
		{
			actual := m.GetOneInt64("number")
			if actual != 1 {
				t.Fatalf("Expected %d, got %d for GetOneInt64(%#v)", actual, 1, "number")
			}
			actual = m.GetOneInt64("numbers")
			if actual != 2 {
				t.Fatalf("Expected %d, got %d for GetOneInt64(%#v)", actual, 1, "number")
			}
		}
		{
			actual := m.GetAllInt64s("numbers")
			expected := []int64{2, 3}
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("Expected %d, got %d for GetAllInt64s(%#v)", actual, expected, "numbers")
			}
		}
	})
}

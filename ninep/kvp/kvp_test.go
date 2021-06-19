package kvp

import (
	"reflect"
	"testing"
)

func assertPairs(t *testing.T, reason, raw string, expected [][2]string) {
	t.Helper()
	actual := ParseKeyPairs(raw)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("%s: Expected %#v, got %#v (input=%#v)", reason, expected, actual, raw)
	}
}

func assertParseError(t *testing.T, reason, raw string, expected [][2]string) {
	t.Helper()
	actual := ParseKeyPairs(raw)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("%s: Expected %#v, got %#v (input=%#v)", reason, expected, actual, raw)
	}
}

var testCases = []struct {
	Passes bool
	Input  string
	Output [][2]string
	Desc   string
}{
	{true, "a=1", [][2]string{{"a", "1"}}, "single key"},
	{true, "a", [][2]string{{"a", ""}}, "single no-value key"},
	{true, "a=", [][2]string{{"a", ""}}, "single no-value key"},
	{true, "a=1 b=2", [][2]string{{"a", "1"}, {"b", "2"}}, "multiple keys"},
	{true, "a=1 b=3 b=2", [][2]string{{"a", "1"}, {"b", "3"}, {"b", "2"}}, "repeated keys"},
	{true, "c a=1 b= b=2", [][2]string{{"c", ""}, {"a", "1"}, {"b", ""}, {"b", "2"}}, "no-value key"},
	{true, "c a", [][2]string{{"c", ""}, {"a", ""}}, "no-value key"},
	{true, "cat", [][2]string{{"cat", ""}}, "no-value key"},
	{true, "cookies apple=1 b=3 b=2", [][2]string{{"cookies", ""}, {"apple", "1"}, {"b", "3"}, {"b", "2"}}, "longer keys"},
	{true, "\"stock footage\"", [][2]string{{"stock footage", ""}}, "quoted keys"},
	{true, "\"stock exchange\"=NASDAQ \"is public\"=yes", [][2]string{{"stock exchange", "NASDAQ"}, {"is public", "yes"}}, "many quoted keys"},
	{true, "name=\"John Doe\"", [][2]string{{"name", "John Doe"}}, "quoted values"},
	{true, "\"full name\"=\"John Doe\" ", [][2]string{{"full name", "John Doe"}}, "quoted values"},
	{false, "$=1 _=42 \"\\\"\"=' ", [][2]string{{"$", "1"}, {"_", "42"}, {"\"", "' "}}, "weird keys"},
	{true, "'stock footage'", [][2]string{{"stock footage", ""}}, "quoted keys"},
	{true, "'stock exchange'=NASDAQ 'is public'=yes", [][2]string{{"stock exchange", "NASDAQ"}, {"is public", "yes"}}, "many quoted keys"},
	{true, "name='John Doe'", [][2]string{{"name", "John Doe"}}, "quoted values"},
	{true, "'full name'='John Doe' ", [][2]string{{"full name", "John Doe"}}, "quoted values"},
	{false, "$=1 _=42 '\"'=' ", [][2]string{{"$", "1"}, {"_", "42"}, {"\"", "' "}}, "weird keys"},
}

func TestParsingKeyPairs(t *testing.T) {
	for _, tc := range testCases {
		if tc.Passes {
			assertPairs(t, tc.Desc, tc.Input, tc.Output)
		} else {
			assertParseError(t, tc.Desc, tc.Input, tc.Output)
		}
	}
}

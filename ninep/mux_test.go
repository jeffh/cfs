package ninep

import (
	"reflect"
	"testing"
)

func TestMux(t *testing.T) {
	m := NewMux()
	m.Define().Path("/plain").As("plain").
		Define().Path("/dynamic/{id}").As("dynamic").
		Define().Path("/dynamic/{id}/postfix").As("postfix").
		Define().Path("/dynamic/{id}/prefix/{name}").As("double").
		Define().Path("/").As("root")

	cases := []struct {
		path string
		ok   bool
		want string
		args []string
	}{
		{"/plain", true, "plain", []string{}},
		{"/dynamic/123", true, "dynamic", []string{"123"}},
		{"/dynamic/asdf", true, "dynamic", []string{"asdf"}},
		{"/dynamic/asdf/qwer", false, "", []string{}},
		{"/dynamic/asdf/postfix", true, "postfix", []string{"asdf"}},
		{"/dynamic/asdf/prefix/cake", true, "double", []string{"asdf", "cake"}},
		{"/", true, "root", []string{}},
		{"/asdf", false, "", []string{}},
	}

	for _, c := range cases {
		var res Match
		ok := m.Match(c.path, &res)
		if ok != c.ok {
			t.Errorf("Match(%q) = %v, want %v", c.path, ok, c.ok)
		} else if ok {
			if res.Id != c.want {
				t.Errorf("Match(%q) = %q, want %q", c.path, res.Id, c.want)
			}
			if !reflect.DeepEqual(res.Vars, c.args) {
				t.Errorf("Match(%q) = %v, want %v", c.path, res.Vars, c.args)
			}
		}
	}
}

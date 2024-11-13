package ndb

import (
	"io"
	"io/fs"
	"strings"
	"testing"
	"time"
)

type simpleFileInfo struct {
	name    string
	modTime time.Time
}

func (s simpleFileInfo) Name() string       { return s.name }
func (s simpleFileInfo) Size() int64        { return 0 }
func (s simpleFileInfo) Mode() fs.FileMode  { return 0 }
func (s simpleFileInfo) ModTime() time.Time { return s.modTime }
func (s simpleFileInfo) IsDir() bool        { return false }
func (s simpleFileInfo) Sys() interface{}   { return nil }

type memSys struct {
	tree map[string]string
}

func (m *memSys) Open(path string) (io.ReadCloser, error) {
	if s, ok := m.tree[path]; ok {
		return io.NopCloser(strings.NewReader(s)), nil
	}
	return nil, fs.ErrNotExist
}

func (m *memSys) Stat(path string) (fs.FileInfo, error) {
	if _, ok := m.tree[path]; ok {
		return simpleFileInfo{name: path, modTime: time.Now()}, nil
	}
	return nil, fs.ErrNotExist
}

func TestSimpleParse(t *testing.T) {
	m := &memSys{
		tree: map[string]string{
			"test.ndb": `givenName=John familyName=Doe # a comment`,
			"multiple.ndb": `givenName=John familyName=Doe
givenName=Jane familyName=Doe`,
			"multiline.ndb": `givenName=John
	   familyName=Doe
givenName=Jane familyName=Doe`,
			"noValue.ndb": `givenName=John familyName=Doe person`,
		},
	}
	t.Run("test.ndb", func(t *testing.T) {
		db := mustOpenOne(t, m, "test.ndb")
		records := db.SearchSlice("givenName", "John")
		if len(records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(records))
		}
		if records[0].Get("familyName") != "Doe" {
			t.Fatalf("expected familyName to be Doe, got %s", records[0].Get("familyName"))
		}
		if records[0].Get("givenName") != "John" {
			t.Fatalf("expected givenName to be John, got %s", records[0].Get("givenName"))
		}
	})

	t.Run("multiple.ndb", func(t *testing.T) {
		db := mustOpenOne(t, m, "multiple.ndb")
		records := db.SearchSlice("givenName", "Jane")
		if len(records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(records))
		}
		records = db.SearchSlice("familyName", "Doe")
		if len(records) != 2 {
			t.Fatalf("expected 2 records, got %d", len(records))
		}
	})

	t.Run("multiline.ndb", func(t *testing.T) {
		db := mustOpenOne(t, m, "multiline.ndb")
		records := db.SearchSlice("givenName", "John")
		if len(records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(records))
		}
		if records[0].Get("familyName") != "Doe" {
			t.Fatalf("expected familyName to be Doe, got %s (%#v)", records[0].Get("familyName"), records)
		}
		records = db.SearchSlice("familyName", "Doe")
		if len(records) != 2 {
			t.Fatalf("expected 2 records, got %d", len(records))
		}
	})

	t.Run("noValue.ndb", func(t *testing.T) {
		db := mustOpenOne(t, m, "noValue.ndb")
		records := db.SearchSlice("person", "")
		if len(records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(records))
		}
		if records[0].Get("familyName") != "Doe" {
			t.Fatalf("expected familyName to be Doe, got %#v", records[0].Get("familyName"))
		}
	})
}

func TestDatabaseParse(t *testing.T) {
	m := &memSys{
		tree: map[string]string{
			"start.ndb": `database=
	file=doe.ndb
	file=appleseed.ndb`,
			"doe.ndb": `givenName=John familyName=Doe
givenName=Jane familyName=Doe`,
			"appleseed.ndb": `givenName=John familyName=Appleseed`,
		},
	}
	db := mustOpen(t, m, "start.ndb")
	records := db.SearchSlice("givenName", "John")
	if len(records) != 2 {
		t.Fatalf("expected 2 record, got %d", len(records))
	}
	if records[0].Get("familyName") != "Doe" {
		t.Fatalf("expected familyName to be Doe, got %s", records[0].Get("familyName"))
	}
	if records[0].Get("givenName") != "John" {
		t.Fatalf("expected givenName to be John, got %s", records[0].Get("givenName"))
	}
}
func mustOpen(t *testing.T, sys System, path string) *Ndb {
	t.Helper()
	db, err := Open(sys, path)
	must(t, err)
	return db
}
func mustOpenOne(t *testing.T, sys System, path string) *Ndb {
	t.Helper()
	db, err := OpenOne(sys, path)
	must(t, err)
	return db
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

package ndb

import (
	"embed"
	"testing"

	"github.com/jeffh/cfs/fs"
	nfs "github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/ninep"
)

//go:embed example
var exampleFiles embed.FS

func TestReadingFromEmbedFS(t *testing.T) {
	m := fs.ReadOnlyFS(exampleFiles)
	t.Run("Open properly opens recursively", func(t *testing.T) {
		db := mustOpen(t, m, "example/start.ndb")
		records := db.SearchSlice(HasAttrValue("givenName", "John"))
		if len(records) != 1 {
			t.Fatalf("expected 1 record, got %d (%#v)", len(records), db.AllSlice())
		}
	})
}

func TestSimpleParse(t *testing.T) {
	m := nfs.NewMemFSWithFiles(
		map[string]string{
			"test.ndb": `givenName=John familyName=Doe # a comment`,
			"multiple.ndb": `givenName=John familyName=Doe
givenName=Jane familyName=Doe`,
			"multiline.ndb": `givenName=John
	   familyName=Doe
givenName=Jane familyName=Doe`,
			"noValue.ndb": `givenName=John familyName=Doe person`,
			"quoted.mdb":  `person name="John Doe"`,
			"similar.mdb": `
provider=openai model=gpt-4o
    price_input=2.500

provider=openai model=gpt-4o tokens
    max_input_tokens=128000
`,
		},
	)
	t.Run("test.ndb", func(t *testing.T) {
		db := mustOpenOne(t, m, "test.ndb")
		records := db.SearchSlice(HasAttrValue("givenName", "John"))
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
		records := db.SearchSlice(HasAttrValue("givenName", "Jane"))
		if len(records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(records))
		}
		records = db.SearchSlice(HasAttrValue("familyName", "Doe"))
		if len(records) != 2 {
			t.Fatalf("expected 2 records, got %d", len(records))
		}

		records = db.SearchSlice(HasAttr("givenName"))
		if len(records) != 2 {
			t.Fatalf("expected 2 records, got %d", len(records))
		}
	})

	t.Run("multiline.ndb", func(t *testing.T) {
		db := mustOpenOne(t, m, "multiline.ndb")
		records := db.SearchSlice(HasAttrValue("givenName", "John"))
		if len(records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(records))
		}
		if records[0].Get("familyName") != "Doe" {
			t.Fatalf("expected familyName to be Doe, got %s (%#v)", records[0].Get("familyName"), records)
		}
		records = db.SearchSlice(HasAttrValue("familyName", "Doe"))
		if len(records) != 2 {
			t.Fatalf("expected 2 records, got %d", len(records))
		}
	})

	t.Run("noValue.ndb", func(t *testing.T) {
		db := mustOpenOne(t, m, "noValue.ndb")
		records := db.SearchSlice(HasAttrValue("person", ""))
		if len(records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(records))
		}
		if records[0].Get("familyName") != "Doe" {
			t.Fatalf("expected familyName to be Doe, got %#v", records[0].Get("familyName"))
		}
	})

	t.Run("quoted.mdb", func(t *testing.T) {
		db := mustOpenOne(t, m, "quoted.mdb")
		records := db.SearchSlice(HasAttrValue("name", "John Doe"))
		if len(records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(records))
		}
		if records[0].Get("name") != "John Doe" {
			t.Fatalf("expected name to be John Doe, got %s (%#v)", records[0].Get("name"), records[0])
		}
	})

	t.Run("similar.mdb", func(t *testing.T) {
		db := mustOpenOne(t, m, "similar.mdb")
		values := []string{
			"provider=openai model=gpt-4o price_input=2.500",
			"provider=openai model=gpt-4o tokens= max_input_tokens=128000",
		}
		i := 0
		for record := range db.Search(HasAttrValue("provider", "openai")) {
			if record.String() != values[i] {
				t.Fatalf("expected %q, got %q", values[i], record.String())
			}
			i++
		}
	})
}

func TestDatabaseParse(t *testing.T) {
	m := nfs.NewMemFSWithFiles(map[string]string{
		"start.ndb": `database=
	file=doe.ndb
	file=appleseed.ndb`,
		"doe.ndb": `givenName=John familyName=Doe
givenName=Jane familyName=Doe`,
		"appleseed.ndb": `givenName=John familyName=Appleseed`,
	})
	db := mustOpen(t, m, "start.ndb")
	records := db.SearchSlice(HasAttrValue("givenName", "John"))
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

func mustOpen(t *testing.T, sys ninep.FileSystem, path string) *Ndb {
	t.Helper()
	db, err := Open(sys, path)
	must(t, err)
	return db
}
func mustOpenOne(t *testing.T, sys ninep.FileSystem, path string) *Ndb {
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

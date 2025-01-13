package ndb

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"slices"
	"strconv"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/jeffh/cfs/ninep"
)

type Ndb struct {
	data  [][]byte
	mods  []time.Time
	files []string
	sys   ninep.FileSystem
}

// Open opens a new Ndb database from the given file path. It will recursively resolve any
// reference databases in the filepath.
//
// Referenced databases can be done with a database attribute followed by file
// attributes in one entry:
//
// ```
// database file="other.ndb" file="another.ndb" file="more.ndb"
// ```
//
// Search resolves follows the ordering of files as they are specified.
func Open(sys ninep.FileSystem, filepath string) (*Ndb, error) {
	if sys == nil {
		panic("sys is required")
	}
	db := &Ndb{
		files: []string{filepath},
		data:  make([][]byte, 1),
		mods:  []time.Time{{}},
		sys:   sys,
	}
	count := 0
	for {
		n, err := db.readFiles(count)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
		count += n
		for record := range db.Search(HasAttrValue("database", "")) {
			for _, file := range record.GetAll("file") {
				if !slices.Contains(db.files, file) {
					db.files = append(db.files, file)
					db.data = append(db.data, []byte{})
					db.mods = append(db.mods, time.Time{})
				}
			}
		}
	}
	return db, nil
}

// OpenOne opens a single file and returns a database. It will not recursively
// open other database references.
func OpenOne(sys ninep.FileSystem, filepath string) (*Ndb, error) {
	if sys == nil {
		panic("sys is required")
	}
	db := &Ndb{
		files: []string{filepath},
		data:  make([][]byte, 1),
		mods:  []time.Time{{}},
		sys:   sys,
	}
	if _, err := db.readFiles(0); err != nil {
		return nil, err
	}
	return db, nil
}

func ParseOne(p []byte) (*Ndb, error) {
	db := &Ndb{
		files: []string{"inline"},
		data:  [][]byte{p},
		mods:  []time.Time{{}},
		sys:   nil,
	}
	return db, nil
}

func ParseOneString(s string) (*Ndb, error) {
	return ParseOne([]byte(s))
}

func (n *Ndb) readFile(fileToRead string, lastSeen time.Time) ([]byte, time.Time, error) {
	ctx := context.Background()
	fi, err := n.sys.Stat(ctx, fileToRead)
	if err != nil {
		slog.Warn("ndb: file not found", "file", fileToRead, "error", err)
		return nil, time.Time{}, err
	}
	modTime := fi.ModTime()
	// this check is to incase the underlying filesystem doesn't support modtime,
	// then we should read at least once
	if modTime.IsZero() {
		modTime = time.Now()
	}
	if !modTime.After(lastSeen) {
		slog.Debug("ndb: file remains unchanged", "file", fileToRead)
		return nil, lastSeen, nil
	}

	f, err := n.sys.OpenFile(ctx, fileToRead, ninep.OREAD)
	if err != nil {
		return nil, modTime, err
	}
	slog.Debug("ndb: load", "file", fileToRead)
	buf, err := io.ReadAll(ninep.Reader(f))
	f.Close()
	if err != nil {
		return nil, modTime, err
	}
	// TODO: validate syntax
	return buf, modTime, nil
}

func (n *Ndb) readFiles(skip int) (int, error) {
	if n.sys == nil {
		return 0, nil
	}
	count := 0
	for i, fileToRead := range n.files[skip:] {
		idx := i + skip
		lastSeen := n.mods[idx]
		buf, ts, err := n.readFile(fileToRead, lastSeen)
		if err != nil {
			return count, err
		}
		if buf == nil {
			count++
			continue
		}
		n.data[i] = buf
		n.mods[i] = ts
		count++
		// TODO: validate syntax
	}
	return count, nil
}

// Changed reopens database files if they have been modified since last read.
// Returns true if the database has been changed.
func (n *Ndb) Changed() bool {
	changed, _ := n.readFiles(0)
	return changed > 0
}

// All returns an iterator that yields all records in the database.
// This isn't particularly efficient to use in production, but may be useful when
// debugging issues.
//
// Use Search to find records matching a specific attribute and value instead.
func (n *Ndb) All() iter.Seq[Record] {
	return n.byPredicate(func(rec []byte) bool { return true })
}

// AllSlice returns a slice of all records in the database. This isn't
// efficient to use in production, but may be useful when debugging.
//
// Use SearchSlice to find records matching a specific attribute and value instead.
func (n *Ndb) AllSlice() []Record { return toSlice(n.All()) }

// SearchSlice returns a slice of records matching the given attribute and value.
func (n *Ndb) SearchSlice(preds ...SearchPredicate) []Record { return toSlice(n.Search(preds...)) }

// First returns the first record that matches the given attribute and value.
func (n *Ndb) First(attr, val string) Record {
	return first(n.Search(HasAttrValue(attr, val)))
}

// Search returns an iterator that yields records matching the given attribute and value.
//
// Example:
//
//	for rec := range db.Search("person", "") {
//	   rec.Get("name")
//	}
//
// This will yield all records with the attribute "person" like:
//
//	person name="John Doe"
func (n *Ndb) Search(preds ...SearchPredicate) iter.Seq[Record] {
	return n.byPredicate(func(rec []byte) bool {
		for _, pred := range preds {
			if !pred.match(rec) {
				return false
			}
		}
		return true
	})
}

type SearchPredicate interface{ match(rec []byte) bool }
type searchPredicate func(rec []byte) bool

func (sp searchPredicate) match(rec []byte) bool { return sp(rec) }

// HasAttr returns a predicate that matches records with the given attribute.
func HasAttr(attr string) SearchPredicate {
	return searchPredicate(func(rec []byte) bool {
		return hasAttr(rec, attr)
	})
}

// HasAttrValue returns a predicate that matches records with the given attribute and value.
func HasAttrValue(attr, value string) SearchPredicate {
	return searchPredicate(func(rec []byte) bool {
		return hasAttrVal(rec, attr, value)
	})
}

func (n *Ndb) byPredicate(allow func(rec []byte) bool) iter.Seq[Record] {
	var results Record
	return func(yield func(Record) bool) {
		recBytes := []byte{}
	loop:
		for i := range n.data {
			scanner := bufio.NewScanner(bytes.NewReader(n.data[i]))
			for scanner.Scan() {
				line := scanner.Bytes()
				cIndex := bytes.IndexByte(line, '#')
				if cIndex != -1 {
					line = line[:cIndex]
				}
				if len(line) == 0 {
					continue
				}
				first, _ := utf8.DecodeRune(line)

				// is this line a new record?
				if !unicode.IsSpace(first) {
					if len(recBytes) > 0 {
						if allow(recBytes) {
							err := parseRecord(recBytes, &results)
							if err != nil {
								// fmt.Printf("parseRecord error: %v\n", err)
								continue
							}
							if !yield(results) {
								recBytes = recBytes[:0]
								break loop
							}
						}
					}
					results.zero()
					recBytes = recBytes[:0]
				}
				// append attributes as one line for parsing
				line = bytes.TrimSpace(line)
				if len(line) > 0 {
					if len(recBytes) == 0 {
						recBytes = append(recBytes, ' ')
					}
					recBytes = append(recBytes, line...)
					recBytes = append(recBytes, ' ')
				}
			}
			if len(recBytes) > 0 {
				if allow(recBytes) {
					err := parseRecord(recBytes, &results)
					if err == nil {
						if !yield(results) {
							recBytes = recBytes[:0]
							break
						}
					} else {
						// fmt.Printf("parseRecord error: %v\n", err)
					}
				}
			}
			recBytes = recBytes[:0]
		}
	}
}

func hasAttr(recBytes []byte, attr string) bool {
	attrKey := []byte(" " + attr + "=")
	off := 0
	for off < len(recBytes) {
		idx := bytes.Index(recBytes[off:], attrKey)
		if idx == -1 {
			return bytes.Index(recBytes, []byte(" "+attr+" ")) != -1
		}

		return true
	}
	return false
}

func hasAttrVal(recBytes []byte, attr, value string) bool {
	attrKey := []byte(" " + attr + "=")
	off := 0
	for off < len(recBytes) {
		idx := bytes.Index(recBytes[off:], attrKey)
		if idx == -1 {
			return len(value) == 0 && bytes.Index(recBytes, []byte(" "+attr+" ")) != -1
		}

		valueStart := idx + len(attrKey)
		if len(recBytes) <= valueStart {
			return value == ""
		}
		first, _ := utf8.DecodeRune(recBytes[valueStart:])
		if first == '"' {
			length := bytes.IndexAny(recBytes[valueStart+1:], "\"")
			if length == -1 {
				length = len(recBytes) - valueStart
			} else {
				length += 2 // 1 for starting quote, and 1 for ending quote
			}
			off += idx + valueStart + length

			// TODO: avoid string allocation
			actualValue, err := strconv.Unquote(string(recBytes[valueStart : valueStart+length]))
			if err == nil && value == actualValue {
				return true
			}
		} else {
			length := bytes.IndexAny(recBytes[valueStart:], " \t\r\n")
			if length == -1 {
				length = len(recBytes) - valueStart
			}
			off += idx + valueStart + length

			if bytes.Equal([]byte(value), recBytes[valueStart:valueStart+length]) {
				return true
			}
		}
	}
	return false
}

func parseRecord(recBytes []byte, results *Record) error {
	if results != nil {
		if *results == nil {
			*results = make(Record, 0, 10)
		}
	}
	r := recBytes
	for len(r) > 0 {
		ch, size := utf8.DecodeRune(r)
		if ch == utf8.RuneError {
			r = r[size:]
			return fmt.Errorf("invalid utf8 rune")
		}
		if unicode.IsSpace(ch) {
			r = r[size:]
			continue
		}
		tup, n, err := parseTuple(r)
		if err == nil {
			if results != nil {
				*results = append(*results, tup)
			}
		} else {
			return err
		}
		r = r[n:]
	}
	return nil
}

func parseTuple(p []byte) (Tuple, int, error) {
	end := bytes.IndexAny(p, "= \t\r\n")
	if end == -1 {
		return Tuple{string(p), ""}, len(p), nil
	}
	attr := string(p[:end])
	if p[end] != '=' {
		return Tuple{attr, ""}, end, nil
	} else {
		valueStart := end + 1
		firstValue, _ := utf8.DecodeRune(p[valueStart:])
		if firstValue == '"' {
			length := bytes.IndexAny(p[valueStart+1:], "\"")
			if length == -1 {
				length = len(p) - valueStart
			} else {
				length += 2 // 1 for starting quote, and 1 for ending quote
			}

			actualValue, err := strconv.Unquote(string(p[valueStart : valueStart+length]))
			if err != nil {
				return Tuple{}, valueStart + length, err
			}
			return Tuple{attr, actualValue}, valueStart + length, nil
		} else {
			length := bytes.IndexAny(p[valueStart:], " \t\r\n")
			if length == -1 {
				length = len(p) - valueStart
			}

			return Tuple{attr, string(p[valueStart : valueStart+length])}, valueStart + length, nil
		}
	}
}

func toSlice(it iter.Seq[Record]) []Record {
	var results []Record
	for rec := range it {
		newRecord := make(Record, len(rec))
		copy(newRecord, rec)
		results = append(results, newRecord)
	}
	return results
}

func first(it iter.Seq[Record]) Record {
	for rec := range it {
		return rec
	}
	return nil
}

package ndb

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"iter"
	"os"
	"slices"
	"strconv"
	"unicode"
	"unicode/utf8"
)

type Ndb struct {
	data  [][]byte
	files []string
	sys   System
}

type System interface {
	Open(path string) (io.ReadCloser, error)
}

type osSys struct{}

func (osSys) Open(path string) (io.ReadCloser, error) {
	return os.Open(path)
}

func Open(sys System, filepath string) (*Ndb, error) {
	if sys == nil {
		sys = osSys{}
	}
	db := &Ndb{files: []string{filepath}, sys: sys}
	for {
		n, err := db.readFiles()
		if err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
		for record := range db.Search("database", "") {
			for _, file := range record.GetAll("file") {
				if !slices.Contains(db.files, file) {
					db.files = append(db.files, file)
				}
			}
		}
	}
	return db, nil
}

func OpenOne(sys System, filepath string) (*Ndb, error) {
	if sys == nil {
		sys = osSys{}
	}
	db := &Ndb{files: []string{filepath}, sys: sys}
	if _, err := db.readFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

func (n *Ndb) readFile(fileToRead string) ([]byte, error) {
	f, err := n.sys.Open(fileToRead)
	if err != nil {
		return nil, err
	}
	buf, err := io.ReadAll(f)
	f.Close()
	if err != nil {
		return nil, err
	}
	// TODO: validate syntax
	return buf, nil
}

func (n *Ndb) readFiles() (int, error) {
	if len(n.files) == len(n.data) {
		return 0, nil
	}
	count := 0
	for _, fileToRead := range n.files[len(n.data):] {
		buf, err := n.readFile(fileToRead)
		if err != nil {
			return count, err
		}
		n.data = append(n.data, buf)
		count++
		// TODO: validate syntax
	}
	return count, nil
}

func (n *Ndb) SearchSlice(attr, val string) []Record {
	var results []Record
	for rec := range n.Search(attr, val) {
		newRecord := make(Record, len(rec))
		copy(newRecord, rec)
		results = append(results, newRecord)
	}
	return results
}

func (n *Ndb) Search(attr, val string) iter.Seq[Record] {
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

				if !unicode.IsSpace(first) {
					if len(recBytes) > 0 {
						if hasAttr(recBytes, attr, val) {
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
					recBytes = recBytes[:0]
				}
				line = bytes.TrimSpace(line)
				if len(line) > 0 {
					recBytes = append(recBytes, ' ')
					recBytes = append(recBytes, line...)
					recBytes = append(recBytes, ' ')
				}
			}
			if len(recBytes) > 0 {
				if hasAttr(recBytes, attr, val) {
					err := parseRecord(recBytes, &results)
					if err == nil {
						yield(results)
					} else {
						// fmt.Printf("parseRecord error: %v\n", err)
					}
				}
			}
			recBytes = recBytes[:0]
		}
	}
}

func hasAttr(recBytes []byte, attr, value string) bool {
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
			length := bytes.IndexAny(recBytes[valueStart:], "\"")
			if length == -1 {
				length = len(recBytes) - valueStart
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
	equals := bytes.IndexByte(p, '=')
	if equals == -1 {
		// look for next whitespace
		ws := bytes.IndexAny(p, " \t\r\n")
		if ws == -1 {
			return Tuple{string(p), ""}, len(p), nil
		} else {
			return Tuple{string(p[:ws]), ""}, ws + 1, nil
		}
	}
	attr := string(p[:equals])
	valueStart := equals + 1
	firstValue, _ := utf8.DecodeRune(p[valueStart:])
	if firstValue == '"' {
		length := bytes.IndexAny(p[valueStart:], "\"")
		if length == -1 {
			length = len(p) - valueStart
		}

		actualValue, err := strconv.Unquote(string(p[valueStart : valueStart+length]))
		if err != nil {
			return Tuple{}, 0, err
		}
		return Tuple{attr, actualValue}, valueStart + length + 1, nil
	} else {
		length := bytes.IndexAny(p[valueStart:], " \t\r\n")
		if length == -1 {
			length = len(p) - valueStart
		}

		return Tuple{attr, string(p[valueStart : valueStart+length])}, valueStart + length + 1, nil
	}
}

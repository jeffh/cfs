package b2fs

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/jeffh/b2client/b2"
)

type keysIterator struct {
	C            *b2.RetryClient
	I            intent
	Ctx          context.Context
	bucketID     string
	limit        int
	prefixOffset int

	buf          []b2.File
	rem          []b2.File
	nextFileName string
	completed    bool
}

func (it *keysIterator) getLimit() int {
	if it.limit == 0 {
		return 5000
	}
	return it.limit
}

func (it *keysIterator) NextFileInfo() (os.FileInfo, error) {
	for {
		if len(it.rem) == 0 && !it.completed {
			key := it.I.key
			if key != "" {
				key += "/"
			}
			res, err := it.C.ListFileNames(it.Ctx, it.bucketID, &b2.ListFileNamesOptions{
				StartFileName: it.nextFileName,
				MaxFileCount:  it.getLimit(),
				Prefix:        key,
				Delimiter:     "/",
			})
			if err != nil {
				return nil, err
			}
			it.buf = res.Files
			it.rem = it.buf
			it.nextFileName = res.NextFileName
			if it.nextFileName == "" {
				it.completed = true
			}
		}

		if len(it.rem) > 0 {
			key := it.I.key
			if key != "" && !strings.HasSuffix(key, "/") {
				key += "/"
			}
			for len(it.rem) > 0 {
				f := it.rem[0]
				it.rem = it.rem[1:]
				if key == "" || strings.HasPrefix(f.FileName, key) {
					fi := &fileInfo{f, it.prefixOffset, ""}
					return fi, nil
				}
			}
		} else {
			return nil, io.EOF
		}
	}
}

func (it *keysIterator) Reset() error {
	it.buf = nil
	it.rem = nil
	it.nextFileName = ""
	it.completed = false
	return nil
}

func (it *keysIterator) Close() error { return it.Reset() }

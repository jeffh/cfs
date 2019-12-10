package b2fs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"mime"
	"os"
	"strings"
	"time"

	"github.com/jeffh/cfs/ninep"
	"github.com/kurin/blazer/b2"
)

const DEFAULT_PRESIGN_DURATION = 1 * time.Hour

func keysMatch(objKey string, wantedKey string) bool {
	return objKey == wantedKey || objKey == wantedKey+"/"
}

func objectInfo(nameOffset int, object *b2.Object, attrs *b2.Attrs, fallbackKey string) os.FileInfo {
	var (
		name    string
		key     string
		modTime time.Time
		size    int64
	)
	if object != nil {
		key = object.Name()
		modTime = attrs.LastModified
		size = attrs.Size
	} else {
		key = fallbackKey
	}
	name = key[nameOffset:]
	if strings.HasPrefix(name, "/") {
		name = name[1:]
	}
	if strings.HasSuffix(name, "/") {
		name = name[:len(name)-1]
	}
	if name == "" {
		name = "/"
	}

	// b2 uses .bzEmpty files to indicate a directory
	isDir := strings.HasSuffix(key, "/.bzEmpty")

	var dirMode os.FileMode
	if isDir {
		dirMode = os.ModeDir
		name = name[:len(name)-len(".bzEmpty")]
		if name == "" {
			name = "."
		}
	}
	return ninep.FileInfoWithUsers(
		&ninep.SimpleFileInfo{
			FIName:    name,
			FIMode:    0777 | dirMode,
			FIModTime: modTime,
			FISize:    size,
		},
		"", // uid
		"", // gid
		"", // muid
	)
}

func objectFileHandle(bucket *b2.Bucket, object *b2.Object, objectKey string, op objectOperation, m ninep.OpenMode) (ninep.FileHandle, error) {
	ctx := context.Background()

	timeBasedFileHandle := func(h *ninep.RWFileHandle, read func(w io.Writer, d time.Duration, kv ninep.KVMap)) error {
		wr, ww := io.Pipe()
		rr, rw := io.Pipe()
		h.R = rr
		h.W = ww
		if m.IsReadable() && !m.IsWriteable() {
			// just assume 1 hour
			wr.Close()
			ww.Close()
			h.W = nil
			go func() {
				read(rw, DEFAULT_PRESIGN_DURATION, nil)
				rw.Close()
			}()
		} else if !m.IsReadable() && m.IsWriteable() {
			wr.Close()
			rw.Close()
			rr.Close()
			ww.Close()
			return ErrMustOpenForReading
		} else {
			go func() {
				defer wr.Close()
				defer rw.Close()
				for {
					in := bufio.NewReaderSize(wr, 4096)
					line, isPrefix, err := in.ReadLine()
					if err != nil {
						if err != io.EOF {
							fmt.Fprintf(rw, "error: %s\n", err)
						}
						return
					}
					if isPrefix {
						// the line is too long, skip
						for isPrefix {
							_, isPrefix, err = in.ReadLine()
							if err != nil {
								if err != io.EOF {
									fmt.Fprintf(rw, "error: %s\n", err)
								}
								return
							}
						}
						fmt.Fprintf(rw, "error: line too long\n")
					} else {
						kv := ninep.ParseKeyValues(string(line))
						total := interpretTimeKeyValues(kv)
						if total <= 0 {
							fmt.Fprintf(rw, "error: duration cannot be less than or equal to zero seconds")
						} else {
							read(rw, total, kv)
							rw.Close()
						}
					}
				}
			}()
		}
		return nil
	}

	h := &ninep.RWFileHandle{}
	switch op {
	case opData:
		if m.IsReadable() {
			r, w := io.Pipe()
			go func() {
				if object == nil {
					object = bucket.Object(objectKey)
				}
				rdr := object.NewReader(ctx)
				_, err := io.Copy(w, rdr)
				w.CloseWithError(mapB2ErrToNinep(err))
				rdr.Close()
			}()
			h.R = r
		}
		if m.IsWriteable() {
			r, w := io.Pipe()
			go func() {
				var err error
				if object == nil {
					object = bucket.Object(objectKey)
				}
				name := object.Name()
				i := strings.LastIndex(name, ".")
				var contentType string
				if i >= 0 {
					contentType = mime.TypeByExtension(name[i:])
				}
				wtr := object.NewWriter(ctx, b2.WithAttrsOption(&b2.Attrs{
					ContentType: contentType,
				}))

				_, err = io.Copy(wtr, r)
				if err != nil {
					wtr.Close()
					w.CloseWithError(err)
					r.Close()
					return
				}
				err = wtr.Close()

				w.CloseWithError(err)
				r.Close()
			}()
			h.W = w
		}
	case opPresignedDownloadUrl:
		err := timeBasedFileHandle(h, func(w io.Writer, d time.Duration, kv ninep.KVMap) {
			contentDisposition := kv.GetOne("content-disposition")
			if object == nil {
				object = bucket.Object(objectKey)
			}

			url, err := object.AuthURL(ctx, d, contentDisposition)
			fmt.Printf("[B2] AuthURL(%v, %v) -> %#v %v\n", d, contentDisposition, url.String(), err)
			if err != nil {
				fmt.Fprintf(w, "error creating url: %s\n", err)
			} else {
				fmt.Fprintf(w, "%s\n", url.String())
			}
		})
		return h, err
	case opMetadata:
		if m.IsReadable() {
			r, w := io.Pipe()
			go func() {
				if object == nil {
					object = bucket.Object(objectKey)
				}
				attrs, err := object.Attrs(ctx)
				if err == nil {
					if err = writeKeyString(w, "name", attrs.Name); err != nil {
						goto end
					}
					if err = writeKeyInt64(w, "size", attrs.Size); err != nil {
						goto end
					}
					if err = writeKeyString(w, "content-type", attrs.ContentType); err != nil {
						goto end
					}
					switch attrs.Status {
					case b2.Unknown:
						err = writeKeyString(w, "status", "unknown")
					case b2.Started:
						err = writeKeyString(w, "status", "started")
					case b2.Uploaded:
						err = writeKeyString(w, "status", "uploaded")
					case b2.Hider:
						err = writeKeyString(w, "status", "hider")
					case b2.Folder:
						err = writeKeyString(w, "status", "folder")
					}
					if err != nil {
						goto end
					}
					if err = writeKeyString(w, "sha1", attrs.SHA1); err != nil {
						goto end
					}
					if err = writeKeyTime(w, "uploaded-at", attrs.UploadTimestamp); err != nil {
						goto end
					}
					if err = writeKeyTime(w, "last-modified", attrs.LastModified); err != nil {
						goto end
					}
				}
			end:
				w.CloseWithError(mapB2ErrToNinep(err))
			}()
			h.R = r
		}
	}
	return h, nil
}

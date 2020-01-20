package b2fs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jeffh/cfs/ninep"
	"github.com/kurin/blazer/b2"
)

type objectOperation int

const (
	opData objectOperation = iota
	opMetadata
	opPresignedDownloadUrl
	opVersions
	opUnfinishedUploads
)

// represents either a known object or an object in potential
type objectNode struct {
	// required
	b2c    *B2Ctx
	bucket *b2.Bucket // it's unfortunate that we need this
	op     objectOperation

	// optional
	nameOffset int // index into getKey()

	// either this
	key string

	// or this
	obj *b2.Object

	// fill this if you want to override the Info value
	info os.FileInfo
}

func (o *objectNode) getName() string {
	name := o.getKey()[o.nameOffset:]
	if strings.HasSuffix(name, "/.bzEmpty") {
		return name[:len(name)-len(".bzEmpty")]
	}
	return name
}

func (o *objectNode) getKey() string {
	if o.obj != nil {
		return o.obj.Name()
	}
	return o.key
}

func (o *objectNode) listObjects(ctx context.Context, key string) *b2.ObjectIterator {
	return listObjects(ctx, o.bucket, o.op, key)
}

func (o *objectNode) List() (ninep.NodeIterator, error) {
	ctx := context.Background()
	itr := o.listObjects(ctx, o.key)
	fmt.Printf("[B2] ListObjects(_, %#v) | %v\n", o.key, o.getName())
	it := &objectsItr{
		b2c:        o.b2c,
		bucket:     o.bucket,
		nameOffset: o.nameOffset + len(o.getName()),
		prefix:     o.key,
		itr:        itr,
	}
	return it, nil
}

func (o *objectNode) Walk(subpath []string) ([]ninep.Node, error) {
	if len(subpath) == 0 {
		return nil, nil
	}
	key := filepath.Join(o.getKey(), filepath.Join(subpath...))
	ctx := context.Background()
	itr := o.listObjects(ctx, key)
	fmt.Printf("[B2.objectNode.Walk] List(%#v, %#v)\n", o.bucket.Name(), key)
	nodes := make([]ninep.Node, 0, len(subpath))
	for itr.Next() {
		object := itr.Object()
		if keysMatch(object.Name(), key) {
			parentKey := filepath.Join(o.getKey(), filepath.Join(subpath[:len(subpath)-1]...))
			if !strings.HasSuffix(key, "/") {
				key += "/"
			}
			attrs, err := object.Attrs(ctx)
			if err != nil {
				return nil, err
			}

			objInfo := objectInfo(len(parentKey), object, attrs, key)
			for i, part := range subpath[:len(subpath)-1] {
				name := part
				if name == "." && i > 0 {
					name = subpath[i-1]
				}
				joinedSubpath := filepath.Join(subpath[:i]...)
				key := filepath.Join(o.getKey(), joinedSubpath)
				nodes = append(nodes, &objectNode{
					b2c:        o.b2c,
					bucket:     o.bucket,
					nameOffset: o.nameOffset + len(joinedSubpath) - len(name),
					key:        key,
					op:         o.op,
					info: &ninep.SimpleFileInfo{
						FIName:    name,
						FIMode:    0777 | os.ModeDir,
						FIModTime: attrs.LastModified,
						FISize:    attrs.Size,
					},
				})
			}
			// prefix := o.getPath(subpath[:len(subpath)-1])
			nodes = append(nodes, &objectNode{
				b2c:        o.b2c,
				bucket:     o.bucket,
				nameOffset: o.nameOffset + len(filepath.Join(subpath[:len(subpath)-1]...)),
				key:        filepath.Join(o.getKey(), filepath.Join(subpath...)),
				obj:        object,
				info:       objInfo,
				op:         o.op,
			})
			return nodes, nil
		} else {
			for i, part := range subpath {
				var dirMode os.FileMode
				if strings.HasSuffix(part, "/") || objectHasPrefix(object.Name(), key) {
					dirMode = os.ModeDir
				}
				name := part
				if name == "." && i > 0 {
					name = subpath[i-1]
				}
				nodes = append(nodes, &objectNode{
					b2c:        o.b2c,
					bucket:     o.bucket,
					nameOffset: o.nameOffset + len(filepath.Join(subpath[:i]...)),
					key:        filepath.Join(o.getKey(), filepath.Join(subpath...)),
					op:         o.op,
					info: &ninep.SimpleFileInfo{
						FIName: name,
						FIMode: 0777 | dirMode,
					},
				})
			}
		}
		// TODO: we need a better approach to this
		if len(nodes) > len(subpath) {
			break
		}
	}
	// nodeStr := []string{}
	// for _, n := range nodes {
	// 	in, err := n.Info()
	// 	if err == nil {
	// 		nodeStr = append(nodeStr, in.Name())
	// 	}
	// }
	// fmt.Printf("[B2.objectNode.Walk] NODES: %#v\n", nodeStr)
	return nodes, itr.Err()
}

func (o *objectNode) CreateFile(name string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return objectFileHandle(o.bucket, nil, filepath.Join(o.getKey(), name), o.op, flag)
}
func (o *objectNode) CreateDir(name string, mode ninep.Mode) error {
	key := filepath.Join(o.getKey(), name, ".bzEmpty")
	obj := o.bucket.Object(key)
	ctx := context.Background()
	w := obj.NewWriter(ctx, b2.WithAttrsOption(&b2.Attrs{
		SHA1:        "da39a3ee5e6b4b0d3255bfef95601890afd80709", // empty sha
		ContentType: "plain/text",
	}))
	return w.Close()
}

func (o *objectNode) DeleteWithMode(name string, m ninep.Mode) error {
	key := filepath.Join(o.getKey(), name)
	ctx := context.Background()
	if m.IsDir() {
		key += "/"
		itr := o.listObjects(ctx, key)
		for itr.Next() {
			obj := itr.Object()
			if deleteErr := obj.Delete(ctx); deleteErr != nil {
				fmt.Printf("DeleteWithMode(%#v, %s) -> %v\n", name, m, deleteErr)
				return mapB2ErrToNinep(deleteErr)
			}
		}

		if err := itr.Err(); err != nil {
			fmt.Printf("DeleteWithMode(%#v, %s) -> %v\n", name, m, err)
			return mapB2ErrToNinep(err)
		}
	} else {
		object := o.obj
		if object == nil {
			object = o.bucket.Object(key)
		}
		err := object.Delete(ctx)
		fmt.Printf("DeleteWithMode(%#v, %s) -> %v %s\n", name, m, key, err)
		return mapB2ErrToNinep(err)
	}
	return nil
}

func (o *objectNode) Delete(name string) error {
	var err error
	ctx := context.Background()
	object := o.obj
	key := filepath.Join(o.getKey(), name)
	if object == nil {
		object = o.bucket.Object(key)
	}
	err = object.Delete(ctx)
	fmt.Printf("Delete(%#v) | %#v -> %v\n", name, key, err)
	return mapB2ErrToNinep(err)
}

func (o *objectNode) Info() (os.FileInfo, error) {
	if o.info != nil {
		return o.info, nil
	}

	ctx := context.Background()
	key := o.key
	object := o.obj
	if o.obj == nil {
		object = o.bucket.Object(key)
	}
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}
	attrs, err := object.Attrs(ctx)
	if err != nil {
		return nil, err
	}
	return objectInfo(o.nameOffset, object, attrs, key), nil
}
func (o *objectNode) WriteInfo(in os.FileInfo) error { return ninep.ErrUnsupported }
func (o *objectNode) Open(m ninep.OpenMode) (ninep.FileHandle, error) {
	return objectFileHandle(o.bucket, o.obj, o.getKey(), o.op, m)
}

func objectsOrObjectForBucket(b2c *B2Ctx, bkt *b2.Bucket, keyPrefix []string, op objectOperation) ([]ninep.Node, error) {
	keyPrefixPath := filepath.Clean(filepath.Join(keyPrefix...))
	if keyPrefixPath == "." {
		keyPrefixPath = ""
	}

	object := &objectNode{
		b2c:    b2c,
		bucket: bkt,
		op:     op,
		key:    keyPrefix[0],
	}
	var rest []ninep.Node
	if len(keyPrefix) > 1 {
		var err error
		rest, err = object.Walk(keyPrefix[1:])
		if err != nil {
			return nil, err
		}
	}
	traversal := make([]ninep.Node, 0, len(keyPrefix))
	traversal = append(traversal, object)
	traversal = append(traversal, rest...)
	return traversal, nil
}

func objectsForBucket(b2c *B2Ctx, dirname string, bkt *b2.Bucket, op objectOperation, now time.Time) ninep.Node {
	return &objectNode{
		b2c:        b2c,
		bucket:     bkt,
		nameOffset: 0,
		key:        "",
		op:         op,
		info: &ninep.SimpleFileInfo{
			FIName:    dirname,
			FIMode:    os.ModeDir | 755,
			FIModTime: now,
		},
	}
}

////////////////////////////////

func objectHasPrefix(objectName, keyPrefix string) bool {
	key := keyPrefix
	if key == "." || key == "" {
		key = ""
	} else {
		key += "/"
	}
	return strings.HasPrefix(objectName, key)
}

type objectsItr struct {
	b2c        *B2Ctx
	bucket     *b2.Bucket
	prefix     string
	nameOffset int
	op         objectOperation
	itr        *b2.ObjectIterator
}

func (itr *objectsItr) NextNode() (ninep.Node, error) {
	for itr.itr.Next() {
		obj := itr.itr.Object()
		if !objectHasPrefix(obj.Name(), itr.prefix) {
			continue
		}
		file := &objectNode{
			b2c:        itr.b2c,
			bucket:     itr.bucket,
			nameOffset: itr.nameOffset,
			key:        obj.Name(),
			op:         itr.op,
			obj:        obj,
		}
		return file, nil
	}
	err := itr.itr.Err()
	if err == nil {
		return nil, io.EOF
	}
	return nil, err
}

func listObjects(ctx context.Context, b *b2.Bucket, op objectOperation, key string) *b2.ObjectIterator {
	if key == "." {
		key = ""
	}
	switch op {
	case opData, opPresignedDownloadUrl, opMetadata:
		return b.List(ctx, b2.ListPrefix(key))
	case opVersions:
		return b.List(ctx, b2.ListPrefix(key), b2.ListHidden())
	case opUnfinishedUploads:
		return b.List(ctx, b2.ListPrefix(key), b2.ListUnfinished())
	}
	panic("Unsupported")
}

func (itr *objectsItr) Reset() error {
	ctx := context.Background()
	itr.itr = listObjects(ctx, itr.bucket, itr.op, itr.prefix)
	return nil
}

func (itr *objectsItr) Close() error {
	itr.itr = nil
	return nil
}

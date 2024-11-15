package s3fs

import (
	"io/fs"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jeffh/cfs/ninep"
)

func keysMatch(objKey *string, wantedKey string) bool {
	return objKey != nil && (*objKey == wantedKey || *objKey == wantedKey+"/")
}

func objectInfo(nameOffset int, object *s3.Object, fallbackKey string) fs.FileInfo {
	var (
		uid     string
		name    string
		key     string
		modTime time.Time
		size    int64
	)
	if object != nil {
		if owner := object.Owner; owner != nil {
			if displayName := owner.DisplayName; displayName != nil {
				uid = *displayName
			} else if id := owner.ID; id != nil {
				uid = *id
			}
		}
		key = *object.Key
		modTime = *object.LastModified
		size = *object.Size
	} else {
		key = fallbackKey
	}
	name = key[nameOffset:]
	name = strings.Trim(name, "/")
	if name == "" {
		name = "/"
	}

	// From Docs: https://docs.aws.amazon.com/AmazonS3/latest/user-guide/using-folders.html
	//
	// "The Amazon S3 console treats all objects that have a
	// forward slash ("/") character as the last (trailing)
	// character in the key name as a folder, for example
	// examplekeyname/."
	isDir := strings.HasSuffix(key, "/")

	var dirMode fs.FileMode
	if isDir {
		dirMode = fs.ModeDir
	}
	return ninep.FileInfoWithUsers(
		&ninep.SimpleFileInfo{
			FIName:    name,
			FIMode:    0o777 | dirMode,
			FIModTime: modTime,
			FISize:    size,
		},
		uid,
		"",  // gid
		uid, // muid
	)
}

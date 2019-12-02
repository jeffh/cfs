package recoverfs

import "github.com/jeffh/cfs/ninep"

type ClientMaker interface {
	MakeClient() (*ninep.Client, error)
}

func NewFs(cf ClientFactory) ninep.FileSystem {
	return &fileSys{cf: cf}
}

type fileSys struct {
	cf ClientFactory
}

package ninep

import (
	"sync"
)

type fidState struct {
	m            sync.Mutex
	mappedFid    Fid
	serverFid    Fid
	serverNewFid Fid
	serverAfid   Fid
	path         []string
	qtype        QidType
	flag         OpenMode
	mode         Mode
	opened       bool
	uname, aname string
	COMMENT      string
	// TODO: someday: we should remember dir offsets for reconnects
}

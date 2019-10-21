package fs

import (
	"os/user"
	"strconv"
	"sync"
)

type idCache struct {
	m    sync.Mutex
	uids map[int]string
	gids map[int]string
}

func (c *idCache) Username(uid int) string {
	c.m.Lock()
	if c.uids == nil {
		c.uids = make(map[int]string)
	}
	username, ok := c.uids[uid]

	if !ok {
		uidstr := strconv.Itoa(uid)
		usr, _ := user.LookupId(uidstr)
		c.uids[uid] = usr.Username
		username = usr.Username
	}
	c.m.Unlock()
	return username
}

func (c *idCache) Groupname(gid int) string {
	c.m.Lock()
	if c.gids == nil {
		c.gids = make(map[int]string)
	}
	groupname, ok := c.gids[gid]

	if !ok {
		gidstr := strconv.Itoa(gid)
		grp, _ := user.LookupGroupId(gidstr)
		c.gids[gid] = grp.Name
		groupname = grp.Name
	}
	c.m.Unlock()
	return groupname
}

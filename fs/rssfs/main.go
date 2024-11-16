package rssfs

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"iter"
	"maps"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeffh/cfs/ninep"
	"github.com/jeffh/cfs/ninep/kvp"
	"github.com/mmcdole/gofeed"
)

type controlFile struct {
	Info   *ninep.SimpleFileInfo
	Handle func(f *fsys, feedID uint64) (ninep.FileHandle, error)
}

type feed struct {
	feed      *gofeed.Feed
	url       string
	fetchedAt time.Time
}

type fsys struct {
	sync.RWMutex
	feeds  map[uint64]*feed
	nextID atomic.Uint64
}

var controls = map[string]controlFile{
	"title":        {Handle: titleFile},
	"description":  {Handle: descriptionFile},
	"link":         {Handle: linkFile},
	"feed_link":    {Handle: feedLinkFile},
	"updated":      {Handle: updatedFile},
	"published":    {Handle: publishedFile},
	"author":       {Handle: authorFile},
	"language":     {Handle: languageFile},
	"copyright":    {Handle: copyrightFile},
	"generator":    {Handle: generatorFile},
	"categories":   {Handle: categoriesFile},
	"feed_type":    {Handle: feedTypeFile},
	"feed_version": {Handle: feedVersionFile},
}

var mx = ninep.NewMux().
	Define().Path("/").As("root").
	Define().Path("/ctl").As("ctl").
	Define().Path("/{feedID}").TrailSlash().As("feed").
	Define().Path("/{feedID}/items").TrailSlash().As("items").
	Define().Path("/{feedID}/items/{id}").TrailSlash().As("item").
	Define().Path("/{feedID}/items/{id}/{field}").As("itemField").
	Define().Path("/{feedID}/{field}").As("field")

func NewFs() ninep.FileSystem {
	return &fsys{
		feeds: make(map[uint64]*feed),
	}
}

func (f *fsys) addFeed(url string) error {
	fp := gofeed.NewParser()
	fd, err := fp.ParseURL(url)
	if err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()

	id := f.nextID.Add(1)
	f.feeds[id] = &feed{
		feed:      fd,
		url:       url,
		fetchedAt: time.Now(),
	}
	return nil
}

func (f *fsys) getFeed(feedIDStr string) (*feed, error) {
	id, err := strconv.ParseUint(feedIDStr, 10, 64)
	if err != nil {
		return nil, fs.ErrNotExist
	}
	f.RLock()
	feed, ok := f.feeds[id]
	f.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	return feed, nil
}

func (f *fsys) getFeedTime(feedIDStr string) (time.Time, error) {
	feed, err := f.getFeed(feedIDStr)
	if err != nil {
		return time.Time{}, err
	}
	if feed.feed.UpdatedParsed != nil {
		return *feed.feed.UpdatedParsed, nil
	}
	if feed.feed.PublishedParsed != nil {
		return *feed.feed.PublishedParsed, nil
	}
	return feed.fetchedAt, nil
}

func (f *fsys) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	var res ninep.Match
	ok := mx.Match(path, &res)
	if !ok {
		return nil, fs.ErrNotExist
	}

	switch res.Id {
	case "ctl":
		h, r, w := ninep.DeviceHandle(flag)
		if r != nil {
			if w != nil {
				defer w.Close()
			}
			go func() {
				if r != nil {
					defer r.Close()
				}
				scanner := bufio.NewScanner(r)
				for scanner.Scan() {
					line := scanner.Text()
					kv, err := kvp.ParseKeyValues(line)
					if err != nil {
						return
					}

					switch kv.GetOne("op") {
					case "add_feed":
						url := kv.GetOne("url")
						if url == "" {
							continue
						}
						if err := f.addFeed(url); err != nil {
							continue
						}
					}
				}
			}()
		}
		return h, nil
	case "field":
		feedID, err := strconv.ParseUint(res.Vars[0], 10, 64)
		if err != nil {
			return nil, fs.ErrNotExist
		}
		field := res.Vars[1]
		control, ok := controls[field]
		if !ok {
			return nil, fs.ErrNotExist
		}
		return control.Handle(f, feedID)
	case "itemField":
		feedID, err := strconv.ParseUint(res.Vars[0], 10, 64)
		if err != nil {
			return nil, fs.ErrNotExist
		}
		itemID, err := strconv.Atoi(res.Vars[1])
		if err != nil {
			return nil, fs.ErrNotExist
		}
		field := res.Vars[2]
		return f.handleItemField(feedID, itemID, field)
	default:
		return nil, fs.ErrNotExist
	}
}

// Update the file handlers to accept feedID
func titleFile(f *fsys, feedID uint64) (ninep.FileHandle, error) {
	f.RLock()
	feed, ok := f.feeds[feedID]
	f.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(feed.feed.Title)}, nil
}

// Update ListDir to handle the new structure
func (f *fsys) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	var res ninep.Match
	ok := mx.Match(path, &res)
	if !ok {
		return ninep.FileInfoErrorIterator(fs.ErrNotExist)
	}

	switch res.Id {
	case "root":
		return func(yield func(fs.FileInfo, error) bool) {
			// List ctl file
			info := &ninep.SimpleFileInfo{
				FIName: "ctl",
				FIMode: fs.ModeDevice | 0666,
			}
			if !yield(info, nil) {
				return
			}

			// List feed directories
			f.RLock()
			defer f.RUnlock()
			for id := range f.feeds {
				info := &ninep.SimpleFileInfo{
					FIName: strconv.FormatUint(id, 10),
					FIMode: os.ModeDir | 0555,
				}
				if !yield(info, nil) {
					return
				}
			}
		}
	case "feed":
		t, err := f.getFeedTime(res.Vars[0])
		if err != nil {
			return ninep.FileInfoErrorIterator(fs.ErrNotExist)
		}

		infos := make([]fs.FileInfo, 0, len(controls)+1)
		keys := slices.Collect(maps.Keys(controls))
		sort.Strings(keys)
		for _, k := range keys {
			infos = append(infos, &ninep.SimpleFileInfo{FIName: k, FIModTime: t, FIMode: 0444})
		}
		infos = append(infos, &ninep.SimpleFileInfo{FIName: "items", FIModTime: t, FIMode: fs.ModeDir | 0555})
		fmt.Printf("FEED: %#v %#v\n", res, keys)
		return ninep.FileInfoSliceIterator(infos)
	case "items":
		return func(yield func(fs.FileInfo, error) bool) {
			t, err := f.getFeedTime(res.Vars[0])
			if err != nil {
				yield(nil, fs.ErrNotExist)
				return
			}
			f.RLock()
			feedID, err := strconv.ParseUint(res.Vars[0], 10, 64)
			if err != nil {
				yield(nil, fs.ErrNotExist)
				f.RUnlock()
				return
			}
			for i := range f.feeds[feedID].feed.Items {
				info := &ninep.SimpleFileInfo{
					FIName:    strconv.FormatUint(uint64(i), 10),
					FIModTime: t,
					FIMode:    os.ModeDir | 0555,
				}
				f.RUnlock()
				if !yield(info, nil) {
					return
				}
				f.RLock()
			}
			f.RUnlock()
		}
	case "item":
		return func(yield func(fs.FileInfo, error) bool) {
			t, err := f.getFeedTime(res.Vars[0])
			if err != nil {
				yield(nil, fs.ErrNotExist)
				return
			}
			_, err = strconv.ParseUint(res.Vars[1], 10, 64)
			if err != nil {
				yield(nil, fs.ErrNotExist)
				return
			}
			fields := []string{"title", "description", "content", "link", "updated", "published", "author", "guid", "categories"}
			info := &ninep.SimpleFileInfo{
				FIModTime: t,
				FIMode:    0444,
			}
			for _, field := range fields {
				info.FIName = field
				if !yield(info, nil) {
					return
				}
			}
		}
	default:
		return ninep.FileInfoErrorIterator(fs.ErrNotExist)
	}
}

// Implementation of file handlers
func descriptionFile(f *fsys, feedID uint64) (ninep.FileHandle, error) {
	f.RLock()
	feed, ok := f.feeds[feedID]
	f.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(feed.feed.Description)}, nil
}

func linkFile(f *fsys, feedID uint64) (ninep.FileHandle, error) {
	f.RLock()
	feed, ok := f.feeds[feedID]
	f.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(feed.feed.Link)}, nil
}

func feedLinkFile(f *fsys, feedID uint64) (ninep.FileHandle, error) {
	f.RLock()
	feed, ok := f.feeds[feedID]
	f.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(feed.feed.FeedLink)}, nil
}

func updatedFile(f *fsys, feedID uint64) (ninep.FileHandle, error) {
	f.RLock()
	feed, ok := f.feeds[feedID]
	f.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(feed.feed.Updated)}, nil
}

func publishedFile(f *fsys, feedID uint64) (ninep.FileHandle, error) {
	f.RLock()
	feed, ok := f.feeds[feedID]
	f.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(feed.feed.Published)}, nil
}

func authorFile(f *fsys, feedID uint64) (ninep.FileHandle, error) {
	f.RLock()
	feed, ok := f.feeds[feedID]
	f.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	var content string
	if feed.feed.Author != nil {
		content = fmt.Sprintf("%s\n%s", feed.feed.Author.Name, feed.feed.Author.Email)
	}
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(content)}, nil
}

func languageFile(f *fsys, feedID uint64) (ninep.FileHandle, error) {
	f.RLock()
	feed, ok := f.feeds[feedID]
	f.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(feed.feed.Language)}, nil
}

func copyrightFile(f *fsys, feedID uint64) (ninep.FileHandle, error) {
	f.RLock()
	feed, ok := f.feeds[feedID]
	f.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(feed.feed.Copyright)}, nil
}

func generatorFile(f *fsys, feedID uint64) (ninep.FileHandle, error) {
	f.RLock()
	feed, ok := f.feeds[feedID]
	f.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(feed.feed.Generator)}, nil
}

func categoriesFile(f *fsys, feedID uint64) (ninep.FileHandle, error) {
	f.RLock()
	feed, ok := f.feeds[feedID]
	f.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(strings.Join(feed.feed.Categories, "\n"))}, nil
}

func feedTypeFile(f *fsys, feedID uint64) (ninep.FileHandle, error) {
	f.RLock()
	feed, ok := f.feeds[feedID]
	f.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(feed.feed.FeedType)}, nil
}

func feedVersionFile(f *fsys, feedID uint64) (ninep.FileHandle, error) {
	f.RLock()
	feed, ok := f.feeds[feedID]
	f.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(feed.feed.FeedVersion)}, nil
}

func (f *fsys) handleItemField(feedID uint64, itemID int, field string) (ninep.FileHandle, error) {
	f.RLock()
	feed, ok := f.feeds[feedID]
	f.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	item := feed.feed.Items[itemID]
	var content string

	switch field {
	case "title":
		content = item.Title
	case "description":
		content = item.Description
	case "content":
		content = item.Content
	case "link":
		content = item.Link
	case "updated":
		content = item.Updated
	case "published":
		content = item.Published
	case "author":
		if item.Author != nil {
			content = fmt.Sprintf("%s\n%s", item.Author.Name, item.Author.Email)
		}
	case "guid":
		content = item.GUID
	case "categories":
		content = strings.Join(item.Categories, "\n")
	default:
		return nil, fs.ErrNotExist
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(content)}, nil
}

// Implement remaining required interfaces...
func (f *fsys) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	var res ninep.Match
	ok := mx.Match(path, &res)
	if !ok {
		return nil, fs.ErrNotExist
	}

	switch res.Id {
	case "root":
		return &ninep.SimpleFileInfo{
			FIName: "/",
			FIMode: fs.ModeDir | 0555,
		}, nil
	case "ctl":
		return &ninep.SimpleFileInfo{
			FIName: "ctl",
			FIMode: fs.ModeDevice | 0666,
		}, nil
	case "feed":
		t, err := f.getFeedTime(res.Vars[0])
		if err != nil {
			return nil, err
		}
		return &ninep.SimpleFileInfo{
			FIName:    res.Vars[0],
			FIModTime: t,
			FIMode:    fs.ModeDir | 0555,
		}, nil
	case "items":
		t, err := f.getFeedTime(res.Vars[0])
		if err != nil {
			return nil, err
		}
		return &ninep.SimpleFileInfo{
			FIName:    "items",
			FIModTime: t,
			FIMode:    fs.ModeDir | 0555,
		}, nil
	case "item":
		t, err := f.getFeedTime(res.Vars[0])
		if err != nil {
			return nil, err
		}
		id := res.Vars[1]
		if _, err := strconv.Atoi(id); err != nil {
			return nil, fs.ErrNotExist
		}
		return &ninep.SimpleFileInfo{
			FIName:    id,
			FIModTime: t,
			FIMode:    fs.ModeDir | 0555,
		}, nil
	case "field", "itemField":
		t, err := f.getFeedTime(res.Vars[0])
		if err != nil {
			return nil, err
		}
		return &ninep.SimpleFileInfo{
			FIName:    path[strings.LastIndex(path, "/")+1:],
			FIModTime: t,
			FIMode:    0444,
		}, nil
	default:
		return nil, fs.ErrNotExist
	}
}

func (f *fsys) WriteStat(ctx context.Context, path string, stat ninep.Stat) error {
	var res ninep.Match
	ok := mx.Match(path, &res)
	if !ok {
		return fs.ErrNotExist
	}

	switch res.Id {
	case "feed":
		return ninep.ErrWriteNotAllowed
	default:
		return ninep.ErrWriteNotAllowed
	}
}

func (f *fsys) Delete(ctx context.Context, path string) error {
	var res ninep.Match
	ok := mx.Match(path, &res)
	if !ok {
		return fs.ErrNotExist
	}

	switch res.Id {
	case "feed":
		feedID, err := strconv.ParseUint(res.Vars[0], 10, 64)
		if err != nil {
			return fs.ErrNotExist
		}

		f.Lock()
		defer f.Unlock()

		if _, exists := f.feeds[feedID]; !exists {
			return fs.ErrNotExist
		}

		delete(f.feeds, feedID)
		return nil
	default:
		return ninep.ErrWriteNotAllowed
	}
}

func (f *fsys) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ninep.ErrWriteNotAllowed
}

func (f *fsys) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	return ninep.ErrWriteNotAllowed
}

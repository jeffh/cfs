package rssfs

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jeffh/cfs/ninep"
	"github.com/mmcdole/gofeed"
)

func staticStringFile(name string, modTime time.Time, contents string) *ninep.SimpleFile {
	return ninep.StaticReadOnlyFile(name, 0444, modTime, []byte(contents))
}

func dynamicDirTree(name string, resolve func() ([]ninep.Node, error)) *ninep.DynamicReadOnlyDirTree {
	return &ninep.DynamicReadOnlyDirTree{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName: name,
			FIMode: os.ModeDir | 0777,
		},
		GetFlatTree: resolve,
	}
}

func staticDir(name string, modTime time.Time, children ...ninep.Node) *ninep.StaticReadOnlyDir {
	return &ninep.StaticReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName:    name,
			FIMode:    os.ModeDir | 0777,
			FIModTime: modTime,
		},
		Children: children,
	}
}

func imageDir(name string, modTime time.Time, img *gofeed.Image) *ninep.StaticReadOnlyDir {
	if img != nil {
		return staticDir(name, modTime,
			staticStringFile("title", modTime, img.Title),
			staticStringFile("url", modTime, img.URL),
		)
	} else {
		return staticDir(name, modTime)
	}
}

func NewFsFromURL(url string) (ninep.FileSystem, error) {
	fp := gofeed.NewParser()
	feed, err := fp.ParseURL(url)
	if err != nil {
		return nil, err
	}

	now := time.Now()

	feedAuthor := ""
	if feed.Author != nil {
		feedAuthor = fmt.Sprintf("%s\n%s", feed.Author.Name, feed.Author.Email)
	}
	return &ninep.SimpleFileSystem{
		Root: ninep.StaticRootDir(
			staticStringFile("title", now, feed.Title),
			staticStringFile("description", now, feed.Description),
			staticStringFile("link", now, feed.Link),
			staticStringFile("feed_link", now, feed.FeedLink),
			staticStringFile("updated", now, feed.Updated),
			staticStringFile("published", now, feed.Published),
			staticStringFile("author", now, feedAuthor),
			staticStringFile("language", now, feed.Language),
			imageDir("image", now, feed.Image),
			staticStringFile("copyright", now, feed.Copyright),
			staticStringFile("generator", now, feed.Generator),
			staticStringFile("categories", now, strings.Join(feed.Categories, "\n")),
			staticStringFile("feed_type", now, feed.FeedType),
			staticStringFile("feed_version", now, feed.FeedVersion),
			dynamicDirTree("items", func() ([]ninep.Node, error) {
				var res []ninep.Node
				for i, item := range feed.Items {
					itemAuthor := ""
					if item.Author != nil {
						itemAuthor = fmt.Sprintf("%s\n%s", item.Author.Name, item.Author.Email)
					}
					res = append(res, staticDir(
						strconv.Itoa(i),
						now,
						staticStringFile("title", now, item.Title),
						staticStringFile("description", now, item.Description),
						staticStringFile("content", now, item.Content),
						staticStringFile("link", now, item.Link),
						staticStringFile("updated", now, item.Updated),
						staticStringFile("published", now, item.Published),
						staticStringFile("author", now, itemAuthor),
						staticStringFile("guid", now, item.GUID),
						imageDir("image", now, item.Image),
						staticStringFile("categories", now, strings.Join(item.Categories, "\n")),
					))
				}
				return res, nil
			}),
		),
	}, nil
}

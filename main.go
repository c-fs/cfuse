// A Go mirror of libfuse's hello.c

package main

import (
	"flag"
	"github.com/c-fs/cfs/client"
	"log"
	"time"
	// pb "github.com/c-fs/cfs/proto"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	ctx "golang.org/x/net/context"
)

type CFile struct {
	client *client.Client
	name   string
	nodefs.File
}

func NewCFile(client *client.Client, name string) nodefs.File {
	f := new(CFile)
	f.client = client
	f.File = nodefs.NewDefaultFile()
	f.name = name
	return f
}

func (f *CFile) String() string {
	return "CFile"
}

func (f *CFile) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	log.Printf("Reading data from %s, len: %v", f.name, len(buf))
	l, buf, _, err := f.client.Read(ctx.TODO(), f.name, off, int64(len(buf)), 0)
	if err != nil {
		log.Fatalf("Failed to read")
	}
	log.Printf("Read Data from %s %v, %v", f.name, buf, l)
	return fuse.ReadResultData(buf), fuse.OK
}

func (f *CFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	w, err := f.client.Write(ctx.TODO(), f.name, off, data, false)
	if err != nil {
		log.Fatalf("Failed to write")
	}
	log.Printf("Wrtten Data to %s %v", f.name, w)
	return uint32(w), fuse.OK
}

func (f *CFile) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	return fuse.OK
}

func (f *CFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	return fuse.OK
}

func (f *CFile) Truncate(size uint64) fuse.Status {
	return fuse.OK
}

func (f *CFile) GetAttr(out *fuse.Attr) fuse.Status {
	stat, _ := f.client.Stat(ctx.TODO(), f.name)
	out.Mode = fuse.S_IFREG | 0644
	out.Size = uint64(stat.TotalSize)
	return fuse.OK
}
func (f *CFile) Fsync(flags int) (code fuse.Status) {
	return fuse.OK
}

type CFuse struct {
	pathfs.FileSystem
	client *client.Client
}

func (me *CFuse) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {

	switch name {
	case "":
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	default:
		stat, err := me.client.Stat(ctx.TODO(), "/cfs0/test/"+name)
		if err != nil {
			log.Println("Failed to stat")
			return &fuse.Attr{
				Mode: fuse.S_IFDIR | 0755,
			}, fuse.OK
		}
		return &fuse.Attr{
			Mode: fuse.S_IFREG | 0644, Size: uint64(stat.TotalSize),
		}, fuse.OK
	}
}

func (me *CFuse) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	log.Printf("opening dir: %s", name)
	entries, err := me.client.ReadDir(ctx.TODO(), "/cfs0/test/"+name)
	if err != nil {
		log.Fatalf("error reading dir %v", err)
	}
	log.Printf("content %v", entries)
	if name == "" {
		c = []fuse.DirEntry{}
		for _, entry := range entries {
			var mode uint32
			if entry.IsDir {
				mode = fuse.S_IFDIR
			} else {
				mode = fuse.S_IFREG
			}
			c = append(c, fuse.DirEntry{Name: entry.Name, Mode: mode})
		}

		return c, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (me *CFuse) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	log.Printf("opening file %s", name)
	return NewCFile(me.client, "/cfs0/test/"+name), fuse.OK
}

func main() {
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("Usage:\n  ./main MOUNTPOINT")
	}
	cli, err := client.New(1, "localhost:15524")
	if err != nil {
		log.Fatalf("cannot connect to cfs server %v\n", err)
	}
	log.Println("Connected to server")
	nfs := pathfs.NewPathNodeFs(&CFuse{FileSystem: pathfs.NewDefaultFileSystem(),
		client: cli}, nil)
	server, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), nil)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Serve()
}

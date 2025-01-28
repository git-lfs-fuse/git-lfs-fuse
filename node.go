package gitlfsfuse

import (
	"context"
	errstd "errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"
	"time"

	"github.com/git-lfs/git-lfs/v3/config"
	"github.com/git-lfs/git-lfs/v3/errors"
	"github.com/git-lfs/git-lfs/v3/lfs"
	"github.com/git-lfs/git-lfs/v3/lfsapi"
	"github.com/git-lfs/git-lfs/v3/tq"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/maypok86/otter"
)

type FSNode struct {
	fs.LoopbackNode
}

type FSNodeData struct {
	DownloadFn func(dst string, ptr *lfs.Pointer) error
	DownloadRn func(ctx context.Context, ptr *lfs.Pointer, buf []byte, off int64) error
	Ignore     bool
}

var _ = (fs.NodeStatfser)((*FSNode)(nil))

func (n *FSNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	return n.LoopbackNode.Statfs(ctx, out)
}

var _ = (fs.NodeLookuper)((*FSNode)(nil))

func (n *FSNode) fixAttr(out *fuse.Attr, path string) {
	if out.Size >= 1024 {
		return
	}
	if r, err := os.Open(path); err == nil {
		if ptr, _ := lfs.DecodePointer(r); ptr != nil {
			out.Size = uint64(ptr.Size)
			if out.Blksize != 0 {
				out.Blocks = (out.Size + uint64(out.Blksize) - 1) / uint64(out.Blksize)
			}
		}
		_ = r.Close()
	}
}

func (n *FSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if !n.Metadata.(*FSNodeData).Ignore {
		defer n.fixAttr(&out.Attr, filepath.Join(n.path(), name))
	}
	return n.LoopbackNode.Lookup(ctx, name, out)
}

var _ = (fs.NodeMknoder)((*FSNode)(nil))

func (n *FSNode) Mknod(ctx context.Context, name string, mode, rdev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if !n.Metadata.(*FSNodeData).Ignore {
		defer n.fixAttr(&out.Attr, filepath.Join(n.path(), name))
	}
	return n.LoopbackNode.Mknod(ctx, name, mode, rdev, out)
}

var _ = (fs.NodeMkdirer)((*FSNode)(nil))

func (n *FSNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return n.LoopbackNode.Mkdir(ctx, name, mode, out)
}

var _ = (fs.NodeRmdirer)((*FSNode)(nil))

func (n *FSNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	return n.LoopbackNode.Rmdir(ctx, name)
}

var _ = (fs.NodeUnlinker)((*FSNode)(nil))

func (n *FSNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return n.LoopbackNode.Unlink(ctx, name)
}

var _ = (fs.NodeRenamer)((*FSNode)(nil))

func (n *FSNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return n.LoopbackNode.Rename(ctx, name, newParent, newName, flags)
}

var _ = (fs.NodeCreater)((*FSNode)(nil))

func (n *FSNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return n.LoopbackNode.Create(ctx, name, flags, mode, out)
}

var _ = (fs.NodeSymlinker)((*FSNode)(nil))

func (n *FSNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return n.LoopbackNode.Symlink(ctx, target, name, out)
}

var _ = (fs.NodeLinker)((*FSNode)(nil))

func (n *FSNode) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return n.LoopbackNode.Link(ctx, target, name, out)
}

var _ = (fs.NodeReadlinker)((*FSNode)(nil))

func (n *FSNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	return n.LoopbackNode.Readlink(ctx)
}

var _ = (fs.NodeOpener)((*FSNode)(nil))

func (n *FSNode) root() *fs.Inode {
	if n.RootData.RootNode != nil {
		return n.RootData.RootNode.EmbeddedInode()
	}
	return n.Root()
}

func (n *FSNode) path() string {
	return filepath.Join(n.RootData.Path, n.Path(n.root()))
}

func (n *FSNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	metadata := n.LoopbackNode.Metadata.(*FSNodeData)
	if metadata.Ignore || flags&uint32(os.O_CREATE) == uint32(os.O_CREATE) {
		return n.LoopbackNode.Open(ctx, flags)
	}

	p := n.path()

	ptr, _ := lfs.DecodePointerFromFile(p)
	if ptr == nil {
		return n.LoopbackNode.Open(ctx, flags)
	}

	if flags&uint32(os.O_RDONLY) != uint32(os.O_RDONLY) {
		if err := metadata.DownloadFn(p, ptr); err != nil {
			w, err := os.Create(p)
			if err != nil {
				return nil, 0, fs.ToErrno(err)
			}
			_, err = lfs.EncodePointer(w, ptr)
			_ = w.Close()
			return nil, 0, fs.ToErrno(err)
		}
		return n.LoopbackNode.Open(ctx, flags)
	}

	f, err := syscall.Open(p, int(flags), 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	lf := NewRemoteFile(ptr, metadata.DownloadRn, f)
	return lf, 0, 0
}

var _ = (fs.NodeReader)((*FSNode)(nil))

func (n *FSNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if f == nil {
		return nil, fs.ToErrno(errors.New("f is nil " + n.path()))
	}
	if reader, ok := f.(fs.FileReader); ok {
		return reader.Read(ctx, dest, off)
	}
	return nil, fs.ToErrno(errors.New("f is not a FileReader " + n.path()))
}

var _ = (fs.NodeOpendirHandler)((*FSNode)(nil))

func (n *FSNode) OpendirHandle(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return n.LoopbackNode.OpendirHandle(ctx, flags)
}

var _ = (fs.NodeReaddirer)((*FSNode)(nil))

func (n *FSNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return n.LoopbackNode.Readdir(ctx)
}

var _ = (fs.NodeGetattrer)((*FSNode)(nil))

func (n *FSNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if f == nil && !n.Metadata.(*FSNodeData).Ignore {
		defer n.fixAttr(&out.Attr, n.path())
	} else if f != nil {
		if _, ok := f.(*RemoteFile); !ok {
			defer n.fixAttr(&out.Attr, n.path())
		}
	}
	return n.LoopbackNode.Getattr(ctx, f, out)
}

var _ = (fs.NodeSetattrer)((*FSNode)(nil))

func (n *FSNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if f == nil && !n.Metadata.(*FSNodeData).Ignore {
		defer n.fixAttr(&out.Attr, n.path())
	} else if f != nil {
		if _, ok := f.(*RemoteFile); !ok {
			defer n.fixAttr(&out.Attr, n.path())
		}
	}
	return n.LoopbackNode.Setattr(ctx, f, in, out)
}

var _ = (fs.NodeGetxattrer)((*FSNode)(nil))

func (n *FSNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	return n.LoopbackNode.Getxattr(ctx, attr, dest)
}

var _ = (fs.NodeSetxattrer)((*FSNode)(nil))

func (n *FSNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	return n.LoopbackNode.Setxattr(ctx, attr, data, flags)
}

var _ = (fs.NodeRemovexattrer)((*FSNode)(nil))

func (n *FSNode) Removexattr(ctx context.Context, attr string) syscall.Errno {
	return n.LoopbackNode.Removexattr(ctx, attr)
}

var _ = (fs.NodeCopyFileRanger)((*FSNode)(nil))

func (n *FSNode) CopyFileRange(ctx context.Context, fhIn fs.FileHandle,
	offIn uint64, out *fs.Inode, fhOut fs.FileHandle, offOut uint64,
	len uint64, flags uint64) (uint32, syscall.Errno) {
	return n.LoopbackNode.CopyFileRange(ctx, fhIn, offIn, out, fhOut, offOut, len, flags)
}

const chunksize = 4 * 1024 * 1024

func NewGitLFSFuseRoot(rootPath string, cfg *config.Configuration) (fs.InodeEmbedder, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(rootPath, &st); err != nil {
		return nil, err
	}

	gf := lfs.NewGitFilter(cfg)
	gref := gf.RemoteRef()
	client, err := lfsapi.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	manifest := tq.NewManifest(cfg.Filesystem(), client, "download", cfg.Remote())
	manifest.Upgrade()

	downloadFn := func(dst string, ptr *lfs.Pointer) error {
		err := gf.SmudgeToFile(dst, ptr, true, manifest, nil)
		if err != nil {
			log.Println(err)
		}
		return err
	}

	hrefs, err := otter.MustBuilder[string, string](10_000).
		Cost(func(key string, value string) uint32 { return 1 }).
		WithVariableTTL().
		Build()
	if err != nil {
		panic(err)
	}

	chunksRoot := filepath.Join(rootPath, ".git", "fuse")

	downloadRn := func(ctx context.Context, ptr *lfs.Pointer, buf []byte, off int64) error {
	next:
		chunk := off / chunksize
		chunkOff := chunk * chunksize
		chunkEnd := chunkOff + chunksize
		chunkStr := strconv.Itoa(int(chunk))
		if chunkEnd > ptr.Size {
			chunkEnd = ptr.Size
		}

		chunkFile, err := os.Open(filepath.Join(chunksRoot, ptr.Oid, chunkStr))
		if errstd.Is(err, os.ErrNotExist) {
			if err := os.MkdirAll(filepath.Join(chunksRoot, ptr.Oid), 0755); err != nil {
				return err
			}
			chunkFile, err = os.Create(filepath.Join(chunksRoot, ptr.Oid, chunkStr))
			if err != nil {
				return err
			}

			href, ok := hrefs.Get(ptr.Oid)
			if !ok {
				br, err := tq.Batch(manifest, tq.Download, cfg.Remote(), gref, []*tq.Transfer{{Oid: ptr.Oid, Size: ptr.Size}})
				if err != nil {
					return err
				}
				if len(br.Objects) == 0 {
					return errors.New("no objects found")
				}
				rel, err := br.Objects[0].Rel("download")
				if err != nil {
					return err
				}
				href = rel.Href
				if !rel.ExpiresAt.IsZero() {
					hrefs.Set(ptr.Oid, href, time.Since(rel.ExpiresAt))
				} else if rel.ExpiresIn > 0 {
					hrefs.Set(ptr.Oid, href, time.Duration(rel.ExpiresIn)*time.Second)
				} else {
					hrefs.Set(ptr.Oid, href, time.Minute*5)
				}
			}
			req, err := http.NewRequestWithContext(ctx, "GET", href, nil)
			if err != nil {
				return err
			}
			download := func(chunkFile *os.File, req *http.Request, chunkOff, chunkEnd int64) (int64, time.Duration, error) {
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", chunkOff, chunkEnd-1))
				resp, err := client.DoAPIRequestWithAuth(cfg.Remote(), req)
				if err != nil {
					return 0, 0, err
				}
				defer func() {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}()
				if resp.StatusCode == http.StatusTooManyRequests {
					retryAfter := resp.Header.Get("Retry-After")
					if seconds, err := strconv.Atoi(retryAfter); err == nil {
						return 0, time.Duration(seconds) * time.Second, nil
					}
					if date, err := time.Parse(time.RFC1123, retryAfter); err == nil {
						return 0, time.Until(date), nil
					}
					return 0, time.Second, nil
				}
				if resp.StatusCode != http.StatusPartialContent {
					return 0, 0, errors.New("unexpected status code: " + resp.Status)
				}
				rangeHdr := resp.Header.Get("Content-Range")
				regex := regexp.MustCompile(`bytes (\d+)\-.*`)
				match := regex.FindStringSubmatch(rangeHdr)
				if len(match) < 2 {
					return 0, 0, fmt.Errorf("badly formatted Content-Range header: %q", rangeHdr)
				}
				if contentStart, _ := strconv.ParseInt(match[1], 10, 64); contentStart != chunkOff {
					return 0, 0, fmt.Errorf("Content-Range start byte incorrect: %s expected %d", match[1], chunkOff)
				}
				if _, err := io.CopyN(chunkFile, resp.Body, resp.ContentLength); err != nil {
					return 0, 0, err
				}
				if err := chunkFile.Sync(); err != nil {
					return 0, 0, err
				}
				log.Printf("download: oid=(%s) size=(%s) %6.2f%% err=%v", ptr.Oid, humanReadableSize(ptr.Size), float64(chunkOff+resp.ContentLength)*100/float64(ptr.Size), err)
				return resp.ContentLength, 0, nil
			}
			for downloadOff := chunkOff; downloadOff != chunkEnd; {
				n, dur, err := download(chunkFile, req, downloadOff, chunkEnd)
				if err != nil {
					_ = chunkFile.Close()
					_ = os.Remove(chunkFile.Name())
					return err
				}
				if dur > 0 {
					time.Sleep(dur)
					continue
				}
				downloadOff += n
			}
			if _, err = chunkFile.Seek(0, io.SeekStart); err != nil {
				_ = chunkFile.Close()
				return err
			}
		}
		if err != nil {
			return err
		}

		if shift := off - chunkOff; shift > 0 {
			if _, err := chunkFile.Seek(shift, io.SeekStart); err != nil {
				_ = chunkFile.Close()
				return err
			}
		}
		n, err := io.ReadFull(chunkFile, buf[:min(int64(len(buf)), chunkEnd-off)])
		if err != nil {
			_ = chunkFile.Close()
			return err
		}
		if n < len(buf) {
			buf = buf[n:]
			off += int64(n)
			if off < ptr.Size {
				_ = chunkFile.Close()
				goto next
			}
		}
		_ = chunkFile.Close()
		return nil
	}

	root := &fs.LoopbackRoot{
		Path: rootPath,
		Dev:  uint64(st.Dev),
		NewNode: func(rootData *fs.LoopbackRoot, parent *fs.LoopbackNode, name string, st *syscall.Stat_t) fs.InodeEmbedder {
			node := &FSNode{LoopbackNode: fs.LoopbackNode{RootData: rootData, Metadata: &FSNodeData{DownloadFn: downloadFn, DownloadRn: downloadRn}}}
			if (parent != nil && parent.Metadata.(*FSNodeData).Ignore) || name == ".git" {
				node.Metadata.(*FSNodeData).Ignore = true
			}
			return node
		},
	}
	rootNode := root.NewNode(root, nil, "", &st)
	root.RootNode = rootNode
	return rootNode, nil
}

func humanReadableSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

package gitlfsfuse

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"syscall"

	"github.com/git-lfs/git-lfs/v3/config"
	"github.com/git-lfs/git-lfs/v3/filepathfilter"
	"github.com/git-lfs/git-lfs/v3/lfs"
	"github.com/git-lfs/git-lfs/v3/lfsapi"
	"github.com/git-lfs/git-lfs/v3/tq"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type FSNode struct {
	fs.LoopbackNode
}

type FSNodeData struct {
	DownloadFn func(dst string, ptr *lfs.Pointer) error
	Ignore     bool
}

var _ = (fs.NodeStatfser)((*FSNode)(nil))

func (n *FSNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	return n.LoopbackNode.Statfs(ctx, out)
}

var _ = (fs.NodeLookuper)((*FSNode)(nil))

func (n *FSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return n.LoopbackNode.Lookup(ctx, name, out)
}

var _ = (fs.NodeMknoder)((*FSNode)(nil))

func (n *FSNode) Mknod(ctx context.Context, name string, mode, rdev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
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

	var ptr *lfs.Pointer
	if r, err := os.Open(p); err == nil {
		ptr, _, _ = lfs.DecodeFrom(r)
		_ = r.Close()
	}
	if ptr == nil {
		return n.LoopbackNode.Open(ctx, flags)
	}

	if flags&uint32(os.O_RDONLY) != uint32(os.O_RDONLY) {
		if err := metadata.DownloadFn(p, ptr); err != nil {
			return nil, 0, fs.ToErrno(err)
		}
		return n.LoopbackNode.Open(ctx, flags)
	}

	f, err := syscall.Open(p, int(flags), 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	lf := NewRemoteFile(ptr, f)
	return lf, 0, 0
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
	return n.LoopbackNode.Getattr(ctx, f, out)
}

var _ = (fs.NodeSetattrer)((*FSNode)(nil))

func (n *FSNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
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

func NewGitLFSFuseRoot(rootPath string, cfg *config.Configuration) (fs.InodeEmbedder, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(rootPath, &st); err != nil {
		return nil, err
	}

	gf := lfs.NewGitFilter(cfg)
	client, err := lfsapi.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	downloadFn := func(dst string, ptr *lfs.Pointer) error {
		filter := filepathfilter.New(cfg.FetchIncludePaths(), cfg.FetchExcludePaths(), filepathfilter.GitIgnore)
		if !filter.Allows(dst) {
			return nil
		}
		manifest := tq.NewManifest(cfg.Filesystem(), client, "download", cfg.Remote())
		err := gf.SmudgeToFile(dst, ptr, true, manifest, nil)
		if err != nil {
			log.Println(err)
		}
		return err
	}

	root := &fs.LoopbackRoot{
		Path: rootPath,
		Dev:  uint64(st.Dev),
		NewNode: func(rootData *fs.LoopbackRoot, parent *fs.LoopbackNode, name string, st *syscall.Stat_t) fs.InodeEmbedder {
			node := &FSNode{LoopbackNode: fs.LoopbackNode{RootData: rootData, Metadata: &FSNodeData{DownloadFn: downloadFn}}}
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

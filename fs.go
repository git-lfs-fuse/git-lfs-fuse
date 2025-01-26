package gitlfsfuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type FsRoot struct {
	lbr fs.LoopbackRoot
}

type FSNode struct {
	*fs.LoopbackNode
	Ignore bool
}

func (n *FSNode) path() string {
	return n.LoopbackNode.Path(n.LoopbackNode.Root())
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

func (n *FSNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return n.LoopbackNode.Open(ctx, flags)
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

func NewGitLFSFuseRoot(rootPath string) (fs.InodeEmbedder, error) {
	node, err := fs.NewLoopbackRoot(rootPath)
	if err != nil {
		return nil, err
	}
	root := node.(*fs.LoopbackNode).RootData
	root.NewNode = func(rootData *fs.LoopbackRoot, parent *fs.Inode, name string, st *syscall.Stat_t) fs.InodeEmbedder {
		var ignore bool
		for ; parent != nil && !ignore; name, parent = parent.Parent() {
			if name == ".git" {
				ignore = true
			}
		}
		return &FSNode{LoopbackNode: &fs.LoopbackNode{RootData: rootData}, Ignore: ignore}
	}
	return &FSNode{LoopbackNode: node.(*fs.LoopbackNode)}, nil
}

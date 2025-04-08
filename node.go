package gitlfsfuse

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/git-lfs/git-lfs/v3/config"
	"github.com/git-lfs/git-lfs/v3/errors"
	"github.com/git-lfs/git-lfs/v3/lfs"
	"github.com/git-lfs/git-lfs/v3/lfsapi"
	"github.com/git-lfs/git-lfs/v3/tq"
	"github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/maypok86/otter"
)

type FSNode struct {
	fs.LoopbackNode
}

type FSNodeData struct {
	NewRemoteFile func(ptr *lfs.Pointer, fd int) (*RemoteFile, error)
	Ignore        bool
}

var _ = (fs.NodeStatfser)((*FSNode)(nil))
var _ = (fs.NodeLookuper)((*FSNode)(nil))
var _ = (fs.NodeMknoder)((*FSNode)(nil))
var _ = (fs.NodeMkdirer)((*FSNode)(nil))
var _ = (fs.NodeRmdirer)((*FSNode)(nil))
var _ = (fs.NodeUnlinker)((*FSNode)(nil))
var _ = (fs.NodeRenamer)((*FSNode)(nil))
var _ = (fs.NodeCreater)((*FSNode)(nil))
var _ = (fs.NodeSymlinker)((*FSNode)(nil))
var _ = (fs.NodeLinker)((*FSNode)(nil))
var _ = (fs.NodeReadlinker)((*FSNode)(nil))
var _ = (fs.NodeOpener)((*FSNode)(nil))
var _ = (fs.NodeReader)((*FSNode)(nil))
var _ = (fs.NodeWriter)((*FSNode)(nil))
var _ = (fs.NodeFsyncer)((*FSNode)(nil))
var _ = (fs.NodeFlusher)((*FSNode)(nil))
var _ = (fs.NodeReleaser)((*FSNode)(nil))
var _ = (fs.NodeOpendirHandler)((*FSNode)(nil))
var _ = (fs.NodeReaddirer)((*FSNode)(nil))
var _ = (fs.NodeGetattrer)((*FSNode)(nil))
var _ = (fs.NodeSetattrer)((*FSNode)(nil))
var _ = (fs.NodeGetxattrer)((*FSNode)(nil))
var _ = (fs.NodeSetxattrer)((*FSNode)(nil))
var _ = (fs.NodeRemovexattrer)((*FSNode)(nil))
var _ = (fs.NodeListxattrer)((*FSNode)(nil))
var _ = (fs.NodeCopyFileRanger)((*FSNode)(nil))

func (n *FSNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	return n.LoopbackNode.Statfs(ctx, out)
}

func (n *FSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	defer n.fixAttr(&out.Attr, name)
	return n.LoopbackNode.Lookup(ctx, name, out)
}

func (n *FSNode) Mknod(ctx context.Context, name string, mode, rdev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	defer n.fixAttr(&out.Attr, name)
	return n.LoopbackNode.Mknod(ctx, name, mode, rdev, out)
}

func (n *FSNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return n.LoopbackNode.Mkdir(ctx, name, mode, out)
}

func (n *FSNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	return n.LoopbackNode.Rmdir(ctx, name)
}

func (n *FSNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return n.LoopbackNode.Unlink(ctx, name)
}

func (n *FSNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return n.LoopbackNode.Rename(ctx, name, newParent, newName, flags)
}

func (n *FSNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return n.LoopbackNode.Create(ctx, name, flags, mode, out)
}

func (n *FSNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return n.LoopbackNode.Symlink(ctx, target, name, out)
}

func (n *FSNode) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return n.LoopbackNode.Link(ctx, target, name, out)
}

func (n *FSNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	return n.LoopbackNode.Readlink(ctx)
}

func (n *FSNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	metadata := n.LoopbackNode.Metadata.(*FSNodeData)
	if metadata.Ignore ||
		flags&uint32(os.O_CREATE) == uint32(os.O_CREATE) ||
		flags&uint32(os.O_TRUNC) == uint32(os.O_TRUNC) {
		return n.LoopbackNode.Open(ctx, flags)
	}

	p := n.path()

	ptr, _ := lfs.DecodePointerFromFile(p)
	if ptr == nil {
		return n.LoopbackNode.Open(ctx, flags)
	}

	f, err := syscall.Open(p, int(flags), 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}

	file, err := metadata.NewRemoteFile(ptr, f)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	return file, 0, 0
}

func (n *FSNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if f != nil {
		if reader, ok := f.(fs.FileReader); ok {
			return reader.Read(ctx, dest, off)
		}
	}
	return nil, fs.ToErrno(errors.New("f is not a FileReader " + n.path()))
}

func (n *FSNode) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
	if f != nil {
		if writer, ok := f.(fs.FileWriter); ok {
			return writer.Write(ctx, data, off)
		}
	}
	return 0, fs.ToErrno(errors.New("f is not a FileWriter " + n.path()))
}

func (n *FSNode) Fsync(ctx context.Context, f fs.FileHandle, flags uint32) syscall.Errno {
	if f != nil {
		if writer, ok := f.(fs.FileFsyncer); ok {
			return writer.Fsync(ctx, flags)
		}
	}
	return fs.ToErrno(errors.New("f is not a FileFsyncer " + n.path()))
}

func (n *FSNode) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {
	if f != nil {
		if writer, ok := f.(fs.FileFlusher); ok {
			return writer.Flush(ctx)
		}
	}
	return fs.ToErrno(errors.New("f is not a FileFlusher " + n.path()))
}

func (n *FSNode) Release(ctx context.Context, f fs.FileHandle) syscall.Errno {
	if f != nil {
		if writer, ok := f.(fs.FileReleaser); ok {
			return writer.Release(ctx)
		}
	}
	return fs.ToErrno(errors.New("f is not a FileReleaser " + n.path()))
}

func (n *FSNode) OpendirHandle(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return n.LoopbackNode.OpendirHandle(ctx, flags)
}

func (n *FSNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return n.LoopbackNode.Readdir(ctx)
}

func (n *FSNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if f == nil {
		defer n.fixAttr(&out.Attr, "")
	} else if _, ok := f.(*RemoteFile); !ok {
		defer n.fixAttr(&out.Attr, "")
	}
	return n.LoopbackNode.Getattr(ctx, f, out)
}

func (n *FSNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if f == nil {
		defer n.fixAttr(&out.Attr, "")
	} else if _, ok := f.(*RemoteFile); !ok {
		defer n.fixAttr(&out.Attr, "")
	}
	return n.LoopbackNode.Setattr(ctx, f, in, out)
}

func (n *FSNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	return n.LoopbackNode.Getxattr(ctx, attr, dest)
}

func (n *FSNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	return n.LoopbackNode.Setxattr(ctx, attr, data, flags)
}

func (n *FSNode) Removexattr(ctx context.Context, attr string) syscall.Errno {
	return n.LoopbackNode.Removexattr(ctx, attr)
}

func (n *FSNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	return n.LoopbackNode.Listxattr(ctx, dest)
}

func (n *FSNode) CopyFileRange(ctx context.Context, fhIn fs.FileHandle,
	offIn uint64, out *fs.Inode, fhOut fs.FileHandle, offOut uint64,
	len uint64, flags uint64) (uint32, syscall.Errno) {
	return n.LoopbackNode.CopyFileRange(ctx, fhIn, offIn, out, fhOut, offOut, len, flags)
}

func (n *FSNode) fixAttr(out *fuse.Attr, name string) {
	if n.Metadata.(*FSNodeData).Ignore || out.Size >= 1024 {
		return
	}
	if r, err := os.Open(filepath.Join(n.path(), name)); err == nil {
		if ptr, _ := lfs.DecodePointer(r); ptr != nil {
			// TODO: move this step to a custom smudge filter
			move := ""
			idp := filepath.Join(n.path(), ".git", "index")
			if f, _ := os.Open(idp); f != nil {
				idx := index.Index{}
				if err := index.NewDecoder(f).Decode(&idx); err == nil {
					i := sort.Search(len(idx.Entries), func(i int) bool {
						return bytes.Compare([]byte(idx.Entries[i].Name), []byte(name)) >= 0
					})
					if i >= 0 && uint64(idx.Entries[i].Size) == out.Size &&
						idx.Entries[i].CreatedAt.Equal(time.Unix(int64(out.Ctime), int64(out.Ctimensec))) &&
						idx.Entries[i].ModifiedAt.Equal(time.Unix(int64(out.Mtime), int64(out.Mtimensec))) {
						idx.Entries[i].Size = uint32(ptr.Size)
						move = filepath.Join(n.path(), ".git", "index-fuse")
						if f2, _ := os.Create(move); f2 != nil {
							if err := index.NewEncoder(f2).Encode(&idx); err != nil {
								move = ""
							}
							_ = f2.Close()
						}
					}
				}
				_ = f.Close()
				if move != "" {
					_ = os.Rename(move, idp)
				}
			}
			out.Size = uint64(ptr.Size)
		}
		_ = r.Close()
	}
}

func (n *FSNode) root() *fs.Inode {
	if n.RootData.RootNode != nil {
		return n.RootData.RootNode.EmbeddedInode()
	}
	return n.Root()
}

func (n *FSNode) path() string {
	return filepath.Join(n.RootData.Path, n.Path(n.root()))
}

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

	actions, err := otter.MustBuilder[string, action](10_000).
		Cost(func(key string, t action) uint32 { return 1 }).
		WithVariableTTL().
		Build()
	if err != nil {
		return nil, err
	}

	pr := filepath.Join(rootPath, ".git", "fuse")
	if err := os.MkdirAll(pr, 0755); err != nil {
		return nil, err
	}

	pf := &pageFetcher{
		remote:    cfg.Remote(),
		actions:   &actions,
		remoteRef: gref,
		manifest:  manifest,
	}

	newRemoteFile := func(ptr *lfs.Pointer, fd int) (*RemoteFile, error) {
		rf, err := NewRemoteFile(ptr, pf, pr, fd)
		if err != nil {
			return nil, err
		}
		return rf, nil
	}

	root := &fs.LoopbackRoot{
		Path: rootPath,
		Dev:  uint64(st.Dev),
		NewNode: func(rootData *fs.LoopbackRoot, parent *fs.LoopbackNode, name string, st *syscall.Stat_t) fs.InodeEmbedder {
			node := &FSNode{LoopbackNode: fs.LoopbackNode{RootData: rootData, Metadata: &FSNodeData{NewRemoteFile: newRemoteFile}}}
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

type Server struct {
	svc *fuse.Server
}

func (s *Server) Close() {
	go func() {
		for err := s.svc.Unmount(); err != nil; err = s.svc.Unmount() {
			time.Sleep(time.Millisecond * 500)
		}
	}()
	s.svc.Wait()
}

func CloneMount(remote, mountPoint string, directMount bool, gitOptions []string) (string, string, *Server, error) {
	dst := strings.TrimSuffix(filepath.Base(remote), ".git")
	dir, err := filepath.Abs(".")
	if mountPoint != "" {
		dst = filepath.Base(mountPoint)
		dir, err = filepath.Abs(filepath.Dir(mountPoint))
	}
	if err != nil {
		return "", "", nil, err
	}
	hid := filepath.Join(dir, "."+dst)
	mnt := filepath.Join(dir, dst)

	info, err := os.Stat(hid)
	if os.IsNotExist(err) {
		err = nil
	}
	if err != nil {
		return "", "", nil, err
	}
	cfg := config.NewIn(hid, "")
	if info == nil {
		git := exec.Command("git", "clone")
		git.Args = append(git.Args, gitOptions...)
		git.Args = append(git.Args, "--", remote, hid)
		git.Stdout = os.Stdout
		git.Stderr = os.Stderr
		git.Env = os.Environ()
		git.Env = append(git.Env, "GIT_LFS_SKIP_SMUDGE=1")
		if err := git.Run(); err != nil {
			return "", "", nil, err
		}
		lfo := lfs.FilterOptions{
			GitConfig:  cfg.GitConfig(),
			Force:      true,
			Local:      true,
			SkipSmudge: true,
		}
		if err := lfo.Install(); err != nil {
			return "", "", nil, err
		}
	} else if !info.IsDir() {
		return "", "", nil, fmt.Errorf("%s is not a directory", hid)
	}

	pxy, err := NewGitLFSFuseRoot(hid, cfg)
	if err != nil {
		return "", "", nil, err
	}
	if err := os.MkdirAll(mnt, 0755); err != nil {
		return "", "", nil, err
	}
	svc, err := fs.Mount(mnt, pxy, &fs.Options{
		NullPermissions: true, // Leave file permissions on "000" files as-is
		MountOptions: fuse.MountOptions{
			DirectMount: directMount,
			FsName:      dst,
			Name:        "git-lfs",
		},
	})
	if err != nil {
		return "", "", nil, err
	}
	return hid, mnt, &Server{svc: svc}, nil
}

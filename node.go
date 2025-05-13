package gitlfsfuse

import (
	"context"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/git-lfs/git-lfs/v3/config"
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
	rf *RemoteFile
	mu sync.RWMutex
}

type FSNodeData struct {
	NewRemoteFile func(ptr *lfs.Pointer, ino uint64, fd int) *RemoteFile
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

// TODO: we may need to implement these:
//var _ = (fs.NodeCopyFileRanger)((*FSNode)(nil))

var RecordNodeOperations = false

func recording(name string, msg func() string) (ret func()) {
	ret = func() {}
	if RecordNodeOperations {
		now := time.Now()
		ret = func() {
			elapsed := time.Since(now)
			log.Printf("%s %dμs: %s\n", name, elapsed.Microseconds(), msg())
		}
	}
	return ret
}

func (n *FSNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	defer recording("statfs", func() string { return n.path() })()
	n.mu.Lock()
	defer n.mu.Unlock()
	fixGitIndex(n.RootData.Path)
	return n.LoopbackNode.Statfs(ctx, out)
}

func (n *FSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	defer recording("lookup", func() string { return filepath.Join(n.path(), name) })()
	defer n.fixAttr(&out.Attr, name)
	return n.LoopbackNode.Lookup(ctx, name, out)
}

func (n *FSNode) Mknod(ctx context.Context, name string, mode, rdev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	defer recording("mknod", func() string { return filepath.Join(n.path(), name) })()
	return n.LoopbackNode.Mknod(ctx, name, mode, rdev, out)
}

func (n *FSNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	defer recording("mkdir", func() string { return filepath.Join(n.path(), name) })()
	return n.LoopbackNode.Mkdir(ctx, name, mode, out)
}

func (n *FSNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	defer recording("rmdir", func() string { return filepath.Join(n.path(), name) })()
	return n.LoopbackNode.Rmdir(ctx, name)
}

func (n *FSNode) Unlink(ctx context.Context, name string) syscall.Errno {
	defer recording("unlink", func() string { return filepath.Join(n.path(), name) })()
	return n.LoopbackNode.Unlink(ctx, name)
}

func (n *FSNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	defer recording("rename", func() string { return filepath.Join(n.path(), name) })()
	return n.LoopbackNode.Rename(ctx, name, newParent, newName, flags)
}

func (n *FSNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	defer recording("create", func() string { return filepath.Join(n.path(), name) })()
	return n.LoopbackNode.Create(ctx, name, flags, mode, out)
}

func (n *FSNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	defer recording("symlink", func() string { return filepath.Join(n.path(), name) })()
	return n.LoopbackNode.Symlink(ctx, target, name, out)
}

func (n *FSNode) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	defer recording("link", func() string { return filepath.Join(n.path(), name) })()
	return n.LoopbackNode.Link(ctx, target, name, out)
}

func (n *FSNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	defer recording("readlink", func() string { return n.path() })()
	return n.LoopbackNode.Readlink(ctx)
}

func (n *FSNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	defer recording("open", func() string { return n.path() })()
	metadata := n.LoopbackNode.Metadata.(*FSNodeData)
	if metadata.Ignore || flags&uint32(os.O_CREATE) != 0 || flags&uint32(os.O_TRUNC) != 0 {
		return n.LoopbackNode.Open(ctx, flags)
	}

	p := n.path()

	n.mu.Lock()
	defer n.mu.Unlock()
	rf := n.rf
	if rf != nil {
		rf.Refs.Add(1)
		return rf, 0, 0
	}

	ptr, _ := lfs.DecodePointerFromFile(p)
	if ptr == nil {
		return n.LoopbackNode.Open(ctx, flags)
	}

	// always open remote files for read/write
	flags &= ^uint32(os.O_RDONLY | os.O_WRONLY | os.O_RDWR)
	flags |= uint32(os.O_RDWR)

	var f int
	ino, err := generateFid(p)
	if err == nil {
		f, err = syscall.Open(p, int(flags), 0)
	}
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}

	rf = metadata.NewRemoteFile(ptr, ino, f)
	n.rf = rf
	rf.Refs.Add(1)
	return rf, 0, 0
}

func (n *FSNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	defer recording("read", func() string { return n.path() })()
	return f.(fs.FileReader).Read(ctx, dest, off)
}

func (n *FSNode) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
	defer recording("write", func() string { return n.path() })()
	return f.(fs.FileWriter).Write(ctx, data, off)
}

func (n *FSNode) Fsync(ctx context.Context, f fs.FileHandle, flags uint32) syscall.Errno {
	defer recording("fsync", func() string { return n.path() })()
	return f.(fs.FileFsyncer).Fsync(ctx, flags)
}

func (n *FSNode) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {
	defer recording("flush", func() string { return n.path() })()
	return f.(fs.FileFlusher).Flush(ctx)
}

func (n *FSNode) Release(ctx context.Context, f fs.FileHandle) syscall.Errno {
	defer recording("release", func() string { return n.path() })()
	if writer, ok := f.(*RemoteFile); ok {
		n.mu.Lock()
		defer n.mu.Unlock()
		if writer.Refs.Add(-1) == 0 {
			n.rf = nil
			return writer.Release(ctx)
		}
		return 0
	}
	return f.(fs.FileReleaser).Release(ctx)
}

func (n *FSNode) OpendirHandle(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	defer recording("opendir", func() string { return n.path() })()
	return n.LoopbackNode.OpendirHandle(ctx, flags)
}

func (n *FSNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	defer recording("readdir", func() string { return n.path() })()
	return n.LoopbackNode.Readdir(ctx)
}

func (n *FSNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	defer recording("getattr", func() string { return n.path() })()
	n.mu.RLock()
	if rf := n.rf; rf != nil {
		defer n.mu.RUnlock()
		return rf.Getattr(ctx, out)
	}
	n.mu.RUnlock()
	defer n.fixAttr(&out.Attr, "")
	return n.LoopbackNode.Getattr(ctx, f, out)
}

func (n *FSNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	defer recording("setattr", func() string { return n.path() })()
	p := n.path()
	n.mu.Lock()
	defer n.mu.Unlock()
	if rf := n.rf; rf != nil {
		return rf.Setattr(ctx, in, out)
	}
	if metadata := n.Metadata.(*FSNodeData); !n.IsDir() && !metadata.Ignore && in.Size < 1024 {
		if ptr, _ := lfs.DecodePointerFromFile(p); ptr != nil {
			var f int
			ino, err := generateFid(p)
			if err == nil {
				f, err = syscall.Open(p, os.O_RDWR, 0)
			}
			if err != nil {
				return fs.ToErrno(err)
			}
			defer syscall.Close(f)
			rf := metadata.NewRemoteFile(ptr, ino, f)
			return rf.Setattr(ctx, in, out)
		}
	}
	return n.LoopbackNode.Setattr(ctx, f, in, out)
}

func (n *FSNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	defer recording("getxattr", func() string { return n.path() })()
	return n.LoopbackNode.Getxattr(ctx, attr, dest)
}

func (n *FSNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	defer recording("setxattr", func() string { return n.path() })()
	return n.LoopbackNode.Setxattr(ctx, attr, data, flags)
}

func (n *FSNode) Removexattr(ctx context.Context, attr string) syscall.Errno {
	defer recording("removexattr", func() string { return n.path() })()
	return n.LoopbackNode.Removexattr(ctx, attr)
}

func (n *FSNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	defer recording("listxattr", func() string { return n.path() })()
	return n.LoopbackNode.Listxattr(ctx, dest)
}

func (n *FSNode) fixAttr(out *fuse.Attr, name string) {
	if (name == "" && n.IsDir()) || n.Metadata.(*FSNodeData).Ignore || out.Size >= 1024 {
		return
	}
	defer recording("fixAttr", func() string { return filepath.Join(n.path(), name) })()
	if r, err := os.Open(filepath.Join(n.path(), name)); err == nil {
		if ptr, _ := lfs.DecodePointer(r); ptr != nil {
			out.Size = uint64(ptr.Size)
		}
		_ = r.Close()
	}
}

func fixGitIndex(dir string) {
	defer recording("fixindex", func() string { return "" })()
	idp := filepath.Join(dir, ".git", "index")
	if f, _ := os.Open(idp); f != nil {
		idx := index.Index{}
		var md int64 = 0
		if err := index.NewDecoder(f).Decode(&idx); err == nil {
			var ei int64 = -1
			var wg sync.WaitGroup
			wg.Add(runtime.NumCPU())
			for i := 0; i < runtime.NumCPU(); i++ {
				go func(idx index.Index) {
					defer wg.Done()
					for {
						e := atomic.AddInt64(&ei, 1)
						if e >= int64(len(idx.Entries)) {
							break
						}
						entry := idx.Entries[e]
						if entry.Size < 1024 {
							p := filepath.Join(dir, entry.Name)
							s, _ := os.Lstat(p)
							if s != nil {
								ptr, _ := lfs.DecodePointerFromFile(p)
								if ptr != nil {
									notModified := s.ModTime().UnixNano() == entry.ModifiedAt.UnixNano()
									if notModified && idx.Entries[e].Size != uint32(ptr.Size) {
										idx.Entries[e].Size = uint32(ptr.Size)
										atomic.StoreInt64(&md, 1)
									}
								}
							}
						}
					}
				}(idx)
			}
			wg.Wait()
		}
		f.Close()
		if atomic.LoadInt64(&md) > 0 {
			time.Sleep(time.Second) // avoid the git "is_racy_timestamp" check
			move := filepath.Join(dir, ".git", "index-fuse")
			if f2, _ := os.Create(move); f2 != nil {
				err := index.NewEncoder(f2).Encode(&idx)
				_ = f2.Close()
				if err == nil {
					_ = os.Rename(move, idp)
				}
			}
		}
	}
	return
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

func NewGitLFSFuseRoot(rootPath string, cfg *config.Configuration, maxPages int64) (fs.InodeEmbedder, func(), error) {
	var st syscall.Stat_t
	if err := syscall.Stat(rootPath, &st); err != nil {
		return nil, nil, err
	}

	gf := lfs.NewGitFilter(cfg)
	gref := gf.RemoteRef()
	client, err := lfsapi.NewClient(cfg)
	if err != nil {
		return nil, nil, err
	}

	manifest := tq.NewManifest(cfg.Filesystem(), client, "download", cfg.Remote())
	manifest.Upgrade()

	actions, _ := otter.MustBuilder[string, action](10_000).
		Cost(func(key string, t action) uint32 { return 1 }).
		WithVariableTTL().
		Build()

	pr := filepath.Join(rootPath, ".git", "fuse")
	if err := os.MkdirAll(pr, 0755); err != nil {
		return nil, nil, err
	}

	lruLogPath := filepath.Join(pr, "lru2.log")
	lru, err := NewDoubleLRU(lruLogPath)
	if err != nil {
		return nil, nil, err
	}

	pl := &plock{lk: make(map[string]*lock)}

	pf := &pageFetcher{
		remote:    cfg.Remote(),
		actions:   &actions,
		remoteRef: gref,
		manifest:  manifest,
		pl:        pl,
		pr:        pr,
		lru:       lru,
		maxPages:  maxPages,
	}

	newRemoteFile := func(ptr *lfs.Pointer, ino uint64, fd int) *RemoteFile {
		return NewRemoteFile(ptr, pl, pf, pr, ino, fd)
	}

	root := &fs.LoopbackRoot{
		Path: rootPath,
		Dev:  uint64(st.Dev),
		NewNode: func(rootData *fs.LoopbackRoot, parent *fs.LoopbackNode, name string, st *syscall.Stat_t) fs.InodeEmbedder {
			node := &FSNode{rf: nil, LoopbackNode: fs.LoopbackNode{RootData: rootData, Metadata: &FSNodeData{NewRemoteFile: newRemoteFile}}}
			if (parent != nil && parent.Metadata.(*FSNodeData).Ignore) || name == ".git" {
				node.Metadata.(*FSNodeData).Ignore = true
			}
			return node
		},
	}
	rootNode := root.NewNode(root, nil, "", &st)
	root.RootNode = rootNode
	return rootNode, func() { lru.Close() }, nil
}

type Server struct {
	svc    *fuse.Server
	cancel func()
}

func (s *Server) Close() {
	go func() {
		for err := s.svc.Unmount(); err != nil; err = s.svc.Unmount() {
			time.Sleep(time.Millisecond * 500)
		}
	}()
	s.svc.Wait()
	s.cancel()
}

func CloneMount(remote, mountPoint string, directMount bool, gitOptions []string, maxPages int64) (string, string, *Server, error) {
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
		git.Stdin = os.Stdin
		git.Stdout = os.Stdout
		git.Stderr = os.Stderr
		git.Env = os.Environ()
		git.Env = append(git.Env, "GIT_LFS_SKIP_SMUDGE=1")
		err := git.Run()
		if err == nil {
			lfo := lfs.FilterOptions{
				GitConfig:  cfg.GitConfig(),
				Force:      true,
				Local:      true,
				SkipSmudge: true,
			}
			err = lfo.Install()
		}
		if err != nil {
			return "", "", nil, err
		}
	}

	fixGitIndex(hid)

	var server *Server
	pxy, cancel, err := NewGitLFSFuseRoot(hid, cfg, maxPages)
	if err == nil {
		err = os.MkdirAll(mnt, 0755)
	}
	if err == nil {
		var svc *fuse.Server
		svc, err = fs.Mount(mnt, pxy, &fs.Options{
			NullPermissions: true, // Leave file permissions on "000" files as-is
			MountOptions: fuse.MountOptions{
				DirectMount: directMount,
				FsName:      dst,
				Name:        "git-lfs",
			},
		})
		if err == nil {
			server = &Server{svc: svc, cancel: cancel}
		}
	}
	return hid, mnt, server, err
}

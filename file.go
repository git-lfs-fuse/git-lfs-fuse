package gitlfsfuse

import (
	"context"
	"syscall"

	"github.com/git-lfs/git-lfs/v3/lfs"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func NewRemoteFile(ptr *lfs.Pointer, fd int) *RemoteFile {
	return &RemoteFile{ptr: ptr, LoopbackFile: fs.LoopbackFile{Fd: fd}}
}

type RemoteFile struct {
	fs.LoopbackFile
	ptr *lfs.Pointer
}

var _ = (fs.FileHandle)((*RemoteFile)(nil))
var _ = (fs.FileReleaser)((*RemoteFile)(nil))
var _ = (fs.FileGetattrer)((*RemoteFile)(nil))
var _ = (fs.FileReader)((*RemoteFile)(nil))
var _ = (fs.FileWriter)((*RemoteFile)(nil))
var _ = (fs.FileGetlker)((*RemoteFile)(nil))
var _ = (fs.FileSetlker)((*RemoteFile)(nil))
var _ = (fs.FileSetlkwer)((*RemoteFile)(nil))
var _ = (fs.FileLseeker)((*RemoteFile)(nil))
var _ = (fs.FileFlusher)((*RemoteFile)(nil))
var _ = (fs.FileFsyncer)((*RemoteFile)(nil))
var _ = (fs.FileSetattrer)((*RemoteFile)(nil))
var _ = (fs.FileAllocater)((*RemoteFile)(nil))

func (f *RemoteFile) Read(ctx context.Context, buf []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) {
	return f.LoopbackFile.Read(ctx, buf, off)
}

func (f *RemoteFile) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	return f.LoopbackFile.Write(ctx, data, off)
}

func (f *RemoteFile) Release(ctx context.Context) syscall.Errno {
	return f.LoopbackFile.Release(ctx)
}

func (f *RemoteFile) Flush(ctx context.Context) syscall.Errno {
	return f.LoopbackFile.Flush(ctx)
}

func (f *RemoteFile) Fsync(ctx context.Context, flags uint32) (errno syscall.Errno) {
	return f.LoopbackFile.Fsync(ctx, flags)
}

func (f *RemoteFile) Getlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) (errno syscall.Errno) {
	return f.LoopbackFile.Getlk(ctx, owner, lk, flags, out)
}

func (f *RemoteFile) Setlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) (errno syscall.Errno) {
	return f.LoopbackFile.Setlk(ctx, owner, lk, flags)
}

func (f *RemoteFile) Setlkw(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) (errno syscall.Errno) {
	return f.LoopbackFile.Setlkw(ctx, owner, lk, flags)
}

func (f *RemoteFile) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return f.LoopbackFile.Setattr(ctx, in, out)
}

func (f *RemoteFile) Getattr(ctx context.Context, a *fuse.AttrOut) syscall.Errno {
	return f.LoopbackFile.Getattr(ctx, a)
}

func (f *RemoteFile) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	return f.LoopbackFile.Lseek(ctx, off, whence)
}

func (f *RemoteFile) Allocate(ctx context.Context, off uint64, sz uint64, mode uint32) syscall.Errno {
	return f.LoopbackFile.Allocate(ctx, off, sz, mode)
}

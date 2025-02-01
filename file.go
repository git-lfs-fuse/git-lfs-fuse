package gitlfsfuse

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"

	"github.com/git-lfs/git-lfs/v3/lfs"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func NewRemoteFile(ptr *lfs.Pointer, pf *PageFetcher, pr string, fd int) *RemoteFile {
	return &RemoteFile{ptr: ptr, pf: pf, pr: pr, LoopbackFile: fs.LoopbackFile{Fd: fd}}
}

type RemoteFile struct {
	ptr *lfs.Pointer
	pf  *PageFetcher
	pr  string
	mu  sync.RWMutex
	fs.LoopbackFile
}

var _ = (fs.FileHandle)((*RemoteFile)(nil))
var _ = (fs.FileReleaser)((*RemoteFile)(nil))
var _ = (fs.FileGetattrer)((*RemoteFile)(nil))
var _ = (fs.FileReader)((*RemoteFile)(nil))
var _ = (fs.FileWriter)((*RemoteFile)(nil))
var _ = (fs.FileGetlker)((*RemoteFile)(nil))
var _ = (fs.FileSetlker)((*RemoteFile)(nil))
var _ = (fs.FileSetlkwer)((*RemoteFile)(nil))
var _ = (fs.FileFlusher)((*RemoteFile)(nil))
var _ = (fs.FileFsyncer)((*RemoteFile)(nil))
var _ = (fs.FileSetattrer)((*RemoteFile)(nil))
var _ = (fs.FileAllocater)((*RemoteFile)(nil))

const pagesize = 4 * 1024 * 1024

func (f *RemoteFile) getPage(ctx context.Context, off int64) (*os.File, int64, error) {
	pageNum := off / pagesize
	pageOff := pageNum * pagesize
	pageStr := strconv.Itoa(int(pageNum))
	pfn := filepath.Join(f.pr, f.ptr.Oid, pageStr)
	page, err := os.Open(pfn)
	if errors.Is(err, os.ErrNotExist) {
		if err = os.MkdirAll(filepath.Join(f.pr, f.ptr.Oid), 0755); err != nil {
			return nil, 0, err
		}
		page, err = os.Create(pfn)
		if err != nil {
			return nil, 0, err
		}
		if pageOff < f.ptr.Size {
			pageEnd := pageOff + pagesize
			if pageEnd > f.ptr.Size {
				pageEnd = f.ptr.Size
			}
			if err = f.pf.Fetch(ctx, page, f.ptr, pageOff, pageEnd); err != nil {
				_ = page.Close()
				_ = os.Remove(pfn)
				return nil, 0, err
			}
		}
	}
	return page, pageOff, nil
}

func (f *RemoteFile) Read(ctx context.Context, buf []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) {
	f.mu.RUnlock()
	defer f.mu.RLock()

	var readn int
	var bufbk = buf
next:
	page, pageOff, err := f.getPage(ctx, off)
	if err != nil {
		return fuse.ReadResultData(bufbk[:readn]), fs.ToErrno(err)
	}
	n, err := page.ReadAt(buf, off-pageOff)
	readn += n
	if readn == len(bufbk) || off+int64(n) >= f.ptr.Size {
		_ = page.Close()
		return fuse.ReadResultData(bufbk[:readn]), fs.OK
	}
	if errors.Is(err, io.EOF) {
		buf = buf[n:]
		off += int64(n)
		_ = page.Close()
		goto next
	}
	_ = page.Close()
	return fuse.ReadResultData(bufbk[:readn]), fs.ToErrno(err)
}

func (f *RemoteFile) Write(ctx context.Context, buf []byte, off int64) (uint32, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var wn int
	var en = len(buf)
next:
	page, pageOff, err := f.getPage(ctx, off)
	if err != nil {
		return 0, fs.ToErrno(err)
	}
	n, err := page.WriteAt(buf[:min(off+int64(len(buf)), pageOff+pagesize)-off], off-pageOff)
	if err != nil {
		_ = page.Close()
		return 0, fs.ToErrno(err)
	}
	if wn += n; wn < en {
		buf = buf[n:]
		off += int64(n)
		_ = page.Close()
		goto next
	}
	if size := off + int64(wn); size > f.ptr.Size {
		f.ptr.Size = size
	}
	return uint32(wn), fs.OK
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

func (f *RemoteFile) fixAttr(out *fuse.AttrOut) {
	out.Size = uint64(f.ptr.Size)
	if out.Blksize != 0 {
		out.Blocks = (out.Size + uint64(out.Blksize) - 1) / uint64(out.Blksize)
	}
}

func (f *RemoteFile) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	defer f.fixAttr(out)
	bs := []byte(f.ptr.Encoded())
	in.Size = uint64(len(bs))
	n, err := f.LoopbackFile.Write(ctx, bs, 0)
	if uint64(n) == in.Size {
		return f.LoopbackFile.Setattr(ctx, in, out)
	}
	return err
}

func (f *RemoteFile) Getattr(ctx context.Context, a *fuse.AttrOut) syscall.Errno {
	defer f.fixAttr(a)
	return f.LoopbackFile.Getattr(ctx, a)
}

func (f *RemoteFile) Allocate(ctx context.Context, off uint64, sz uint64, mode uint32) syscall.Errno {
	return f.LoopbackFile.Allocate(ctx, off, sz, mode)
}

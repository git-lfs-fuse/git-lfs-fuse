package gitlfsfuse

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"

	"github.com/git-lfs/git-lfs/v3/lfs"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func NewRemoteFile(ptr *lfs.Pointer, pf PageFetcher, pr string, fd int) *RemoteFile {
	pr = filepath.Join(pr, ptr.Oid)
	bs, _ := os.ReadFile(filepath.Join(pr, "tc"))
	tc, err := strconv.ParseInt(string(bs), 10, 64)
	if err != nil {
		tc = ptr.Size
	}
	return &RemoteFile{ptr: ptr, pf: pf, pr: pr, tc: tc, LoopbackFile: fs.LoopbackFile{Fd: fd}}
}

type RemoteFile struct {
	ptr *lfs.Pointer
	pf  PageFetcher
	pr  string // root for pages
	tc  int64  // keep track of truncate operations. This is persisted to the tc file.
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

// TODO: we may need to implement these:
//var _ = (fs.FileAllocater)((*RemoteFile)(nil))
//var _ = (fs.FileLseeker)((*RemoteFile)(nil))

const pagesize = 2 * 1024 * 1024

func (f *RemoteFile) getPage(ctx context.Context, off int64) (*os.File, int64, int64, error) {
	pageNum := off / pagesize
	pageOff := pageNum * pagesize
	pageEnd := pageOff + pagesize
	pageEnd = min(pageEnd, f.ptr.Size)
	pageEnd = max(pageEnd, pageOff)

	pageStr := strconv.Itoa(int(pageNum))
	pfn := filepath.Join(f.pr, pageStr)
	page, err := os.OpenFile(pfn, os.O_RDWR, 0666)
	if errors.Is(err, os.ErrNotExist) {
		page, err = os.Create(pfn)
		if errors.Is(err, os.ErrNotExist) {
			if err = os.MkdirAll(f.pr, 0755); err == nil {
				page, err = os.Create(pfn)
			}
		}
		if err != nil {
			return nil, 0, 0, err
		}
		if pageOff < f.tc {
			if err = f.pf.Fetch(ctx, page, f.ptr, pageOff, min(pageEnd, f.tc)); err != nil {
				// TODO: handle ptr not found error
				_ = page.Close()
				_ = os.Remove(pfn)
				return nil, 0, 0, err
			}
		}
		// make sure every page has the same size.
		if err := page.Truncate(pagesize); err != nil {
			_ = page.Close()
			_ = os.Remove(pfn)
			return nil, 0, 0, err
		}
	}
	return page, pageOff, pageEnd - pageOff, nil
}

func (f *RemoteFile) Read(ctx context.Context, buf []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var readn int
	var bufbk = buf
next:
	page, pageOff, size, err := f.getPage(ctx, off)
	if err != nil {
		return fuse.ReadResultData(bufbk[:readn]), fs.ToErrno(err)
	}
	shiftOff := off - pageOff
	n, err := page.ReadAt(buf[:min(int64(len(buf)), size-shiftOff)], shiftOff)
	readn += n
	if readn == len(bufbk) || off+int64(n) >= f.ptr.Size {
		_ = page.Close()
		return fuse.ReadResultData(bufbk[:readn]), fs.OK
	}
	if err == nil && n > 0 {
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
	page, pageOff, _, err := f.getPage(ctx, off)
	if err != nil {
		return 0, fs.ToErrno(err)
	}
	n, err := page.WriteAt(buf[:min(int64(len(buf)), pageOff+pagesize-off)], off-pageOff)
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
	_ = page.Close()
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
}

func (f *RemoteFile) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.fixAttr(out)

	if ns := int64(in.Size); ns < f.ptr.Size { // truncate operation
		// wipe out affected range
		pages, err := os.ReadDir(f.pr)
		if errors.Is(err, os.ErrNotExist) {
			err = os.MkdirAll(f.pr, 0755)
		}
		if err != nil {
			return fs.ToErrno(err)
		}
		for _, p := range pages {
			if p.IsDir() || p.Name() == "tc" {
				continue
			}
			pageNum, err := strconv.ParseInt(p.Name(), 10, 64)
			if err != nil {
				continue
			}
			path := filepath.Join(f.pr, p.Name())
			if pageNum*pagesize >= ns {
				if err := os.Remove(path); err != nil {
					return fs.ToErrno(err)
				}
			} else if pn := ns / pagesize; pn == pageNum {
				pf, err := os.OpenFile(path, os.O_RDWR, 0666)
				if err != nil {
					return fs.ToErrno(err)
				}
				err = pf.Truncate(ns - pn*pagesize)
				err = pf.Truncate(pagesize) // keep the page in the same size.
				pf.Close()
				if err != nil {
					return fs.ToErrno(err)
				}
			}
		}
		if ns < f.tc {
			if err := os.WriteFile(filepath.Join(f.pr, "tc"), []byte(strconv.FormatInt(ns, 10)), 0666); err != nil {
				return fs.ToErrno(err)
			}
			f.tc = ns
		}
	}
	f.ptr.Size = int64(in.Size)
	bs := []byte(f.ptr.Encoded())
	in.Size = uint64(len(bs))
	n, err := f.LoopbackFile.Write(ctx, bs, 0)
	if uint64(n) == in.Size {
		return f.LoopbackFile.Setattr(ctx, in, out)
	}
	return err
}

func (f *RemoteFile) Getattr(ctx context.Context, a *fuse.AttrOut) syscall.Errno {
	f.mu.RLock()
	defer f.mu.RUnlock()
	defer f.fixAttr(a)
	return f.LoopbackFile.Getattr(ctx, a)
}

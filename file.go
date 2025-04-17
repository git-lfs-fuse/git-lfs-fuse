package gitlfsfuse

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/git-lfs/git-lfs/v3/lfs"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func generateFid(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}

	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("failed to get inode from fi")
	}
	return stat.Ino, nil
}

func NewRemoteFile(ptr *lfs.Pointer, pl *plock, pf PageFetcher, pr string, ino uint64, fd int) *RemoteFile {
	ps := filepath.Join(pr, ptr.Oid, "shared")
	pr = filepath.Join(pr, ptr.Oid, strconv.FormatUint(ino, 10))

	bs, err := os.ReadFile(filepath.Join(pr, "tc"))
	tc, err := strconv.ParseInt(string(bs), 10, 64)
	if err != nil {
		tc = ptr.Size
	}
	bs, err = os.ReadFile(filepath.Join(ps, "tc"))
	sz, err := strconv.ParseInt(string(bs), 10, 64)
	if err != nil {
		sz = ptr.Size
	}
	return &RemoteFile{ptr: ptr, pl: pl, pf: pf, ps: ps, pr: pr, tc: tc, sz: sz, Ino: ino, LoopbackFile: fs.LoopbackFile{Fd: fd}}
}

type RemoteFile struct {
	ptr  *lfs.Pointer
	pl   *plock
	pf   PageFetcher
	ps   string // directory of shared pages
	pr   string // root for pages
	tc   int64  // keep track of truncate operations. This is persisted to the tc file.
	sz   int64  // the original file size
	mu   sync.RWMutex
	Ino  uint64       // inode number
	Refs atomic.Int64 // reference count
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

func createSymlink(path, dest string) (err error) {
	if _, err = os.Lstat(dest); err != nil { // make sure the dest file exists.
		return err
	}
	if err = os.Symlink(dest, path); errors.Is(err, os.ErrNotExist) {
		if err = os.MkdirAll(filepath.Dir(path), 0755); err == nil {
			err = os.Symlink(dest, path)
		}
	}
	return err
}

func createFile(path string) (file *os.File, err error) {
	if file, err = os.Create(path); errors.Is(err, os.ErrNotExist) {
		if err = os.MkdirAll(filepath.Dir(path), 0755); err == nil {
			file, err = os.Create(path)
		}
	}
	return file, err
}

func replaceLinkFile(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return createFile(path)
	} else if err == nil && info.Mode()&os.ModeSymlink == 0 {
		// if the file exists and is not a symlink, no replacement is needed.
		return os.OpenFile(path, os.O_RDWR, 0666)
	} else if err != nil {
		return nil, err
	}

	dest, err := os.Open(path) // open the dest file by following the link.
	if errors.Is(err, os.ErrNotExist) {
		_ = os.Remove(path) // remove the path which is a broken link.
		return createFile(path)
	}
	if err != nil {
		return nil, err
	}
	defer dest.Close()

	_ = os.Remove(path) // remove the link.
	file, err := createFile(path)
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(file, dest); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	return file, nil
}

func (f *RemoteFile) getPage(ctx context.Context, off int64) (*os.File, int64, int64, error) {
	pageNum := off / pagesize
	pageOff := pageNum * pagesize
	pageEnd := pageOff + pagesize

	pageStr := strconv.Itoa(int(pageNum))
	pagePth := filepath.Join(f.pr, pageStr)

	page, err := os.OpenFile(pagePth, os.O_RDWR, 0666)
	if errors.Is(err, os.ErrNotExist) {
		destPth := filepath.Join(f.ps, pageStr)
		f.pl.Lock(destPth)
		defer f.pl.Unlock(destPth)
		if page, err = os.OpenFile(pagePth, os.O_RDWR, 0666); err == nil {
			return page, pageOff, pageEnd - pageOff, nil
		}
		err = createSymlink(pagePth, destPth)
		if pageOff < f.tc && errors.Is(err, os.ErrNotExist) {
			dest, err := createFile(destPth)
			if err != nil {
				return nil, 0, 0, err
			}
			if err = f.pf.Fetch(ctx, dest, f.ptr, pageOff, min(pageEnd, f.sz), pageStr); err != nil {
				// TODO: handle ptr not found error
				_ = dest.Close()
				_ = os.Remove(destPth)
				if errors.Is(err, context.Canceled) { // will this cause a retry loop?
					return nil, 0, 0, syscall.EINTR
				}
				return nil, 0, 0, err
			}
			// make sure every page has the same size.
			if err = dest.Truncate(pagesize); err != nil {
				_ = dest.Close()
				_ = os.Remove(destPth)
				return nil, 0, 0, err
			}
			_ = dest.Close()
			if err = createSymlink(pagePth, destPth); err != nil {
				return nil, 0, 0, err
			}
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, 0, 0, err
		}
		if pageEnd <= f.tc {
			page, err = os.OpenFile(pagePth, os.O_RDWR, 0666)
			if err != nil {
				return nil, 0, 0, err
			}
		} else {
			page, err = replaceLinkFile(pagePth)
			if err != nil {
				return nil, 0, 0, err
			}
			err = page.Truncate(max(f.tc-pageOff, 0))
			err = page.Truncate(pagesize)
			if err != nil {
				_ = page.Close()
				_ = os.Remove(pagePth)
				return nil, 0, 0, err
			}
		}
	}
	return page, pageOff, pageEnd - pageOff, nil
}

func (f *RemoteFile) getPageForWrite(ctx context.Context, off int64) (*os.File, int64, int64, error) {
	page, pageOff, size, err := f.getPage(ctx, off)
	if err != nil {
		return nil, 0, 0, err
	}
	_ = page.Close()

	pageNum := off / pagesize
	pageStr := strconv.Itoa(int(pageNum))
	pagePth := filepath.Join(f.pr, pageStr)
	page, err = replaceLinkFile(pagePth)
	if err != nil {
		return nil, 0, 0, err
	}
	err = page.Truncate(pagesize)
	if err != nil {
		_ = page.Close()
		_ = os.Remove(pagePth)
		return nil, 0, 0, err
	}
	return page, pageOff, size, nil
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
	page, pageOff, _, err := f.getPageForWrite(ctx, off)
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
	var attr fuse.AttrOut
	// TODO: can we cache the attr?
	err := f.LoopbackFile.Getattr(ctx, &attr)
	if err != 0 {
		return err
	}
	bs := []byte(f.ptr.Encoded())
	n, err := f.LoopbackFile.Write(ctx, bs, 0)
	if int(n) != len(bs) {
		return err
	}
	attrIn := &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			Size:      uint64(len(bs)),
			Atime:     attr.Atime,
			Mtime:     attr.Mtime,
			Ctime:     attr.Ctime,
			Atimensec: attr.Atimensec,
			Mtimensec: attr.Mtimensec,
			Ctimensec: attr.Ctimensec,
			Mode:      attr.Mode,
			Owner:     attr.Owner,
		},
	}
	return f.LoopbackFile.Setattr(ctx, attrIn, &attr)
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

	szPath := filepath.Join(f.ps, "tc")
	if _, err := os.Stat(szPath); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(f.ps, 0755); err != nil {
			return fs.ToErrno(err)
		}
		if err = os.WriteFile(szPath, []byte(strconv.FormatInt(f.sz, 10)), 0666); err != nil {
			return fs.ToErrno(err)
		}
	} else if err != nil {
		return fs.ToErrno(err)
	}
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
				page, err := replaceLinkFile(path)
				if err != nil {
					return fs.ToErrno(err)
				}
				err = page.Truncate(ns - pn*pagesize)
				err = page.Truncate(pagesize) // keep the page in the same size.
				_ = page.Close()
				if err != nil {
					_ = os.Remove(path)
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

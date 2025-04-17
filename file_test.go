package gitlfsfuse

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"github.com/git-lfs/git-lfs/v3/lfs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type fetcher struct {
	f *os.File
}

func (f *fetcher) Fetch(ctx context.Context, w io.Writer, ptr *lfs.Pointer, off, end int64, pageNum string) error {
	if _, err := f.f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	_, err := io.CopyN(w, f.f, end-off)
	return err
}

func createRandomRemoteFile(size int64) (*RemoteFile, func()) {
	if size%8 != 0 {
		panic("size must be a multiple of 8")
	}
	f, err := os.CreateTemp("", "rnd")
	if err != nil {
		panic(err)
	}
	p, err := os.CreateTemp("", "ptr")
	if err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		panic(err)
	}
	d, err := os.MkdirTemp("", "rmf")
	if err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		_ = p.Close()
		_ = os.Remove(p.Name())
		panic(err)
	}
	clean := func() {
		_ = p.Close()
		_ = f.Close()
		_ = os.Remove(p.Name())
		_ = os.Remove(f.Name())
		_ = os.RemoveAll(d)
		if err != nil {
			panic(err)
		}
	}
	b := bufio.NewWriter(f)
	o := sha256.New()
	m := io.MultiWriter(o, b)
	for i := int64(0); i < size; i += 8 {
		if err = binary.Write(m, binary.LittleEndian, rand.Int64()); err != nil {
			clean()
		}
	}
	if err = b.Flush(); err != nil {
		clean()
	}
	ptr := lfs.NewPointer(hex.EncodeToString(o.Sum(nil)), size, nil)
	if _, err = ptr.Encode(p); err != nil {
		clean()
	}
	s, err := p.Stat()
	if err != nil {
		clean()
	}
	stat, ok := s.Sys().(*syscall.Stat_t)
	if !ok {
		clean()
	}
	pl := &plock{lk: make(map[string]*lock)}
	file := NewRemoteFile(ptr, pl, &fetcher{f: f}, d, stat.Ino, int(p.Fd()))
	return file, clean
}

const testsize = 5 * pagesize

func random(b []byte) {
	if len(b) != 32 {
		panic("len(b) != 32")
	}
	binary.LittleEndian.PutUint64(b[0:8], rand.Uint64())
	binary.LittleEndian.PutUint64(b[8:16], rand.Uint64())
	binary.LittleEndian.PutUint64(b[16:24], rand.Uint64())
	binary.LittleEndian.PutUint64(b[24:32], rand.Uint64())
}

func pipe(f *RemoteFile, w io.Writer, buf, beg, end int64) (err error) {
	b := make([]byte, buf)
	for i := beg; i < end; i += int64(len(b)) {
		if i+int64(len(b)) > end {
			b = b[:end-i]
		}
		_, err = f.Read(context.Background(), b, i)
		if !errors.Is(err, syscall.Errno(0)) {
			return err
		}
		if _, err = w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func TestRemoteFile_Read(t *testing.T) {
	f, cancel := createRandomRemoteFile(testsize)
	defer cancel()

	o := sha256.New()
	if err := pipe(f, o, 1009, 0, f.ptr.Size); err != nil { // intentional prime
		t.Fatalf("Read error: %v", err)
	}
	if hex.EncodeToString(o.Sum(nil)) != f.ptr.Oid {
		t.Fatalf("oid mismatch")
	}
}

func TestRemoteFile_Write_Full(t *testing.T) {
	f, cancel := createRandomRemoteFile(testsize)
	defer cancel()

	o1 := sha256.New()
	o2 := sha256.New()
	b := make([]byte, 32)
	s := 31 // intentional prime
	for i := 0; i < testsize; i += s {
		if i+s > testsize {
			s = testsize - i
		}
		random(b)
		n, err := f.Write(context.Background(), b[:s], int64(i))
		if !errors.Is(err, syscall.Errno(0)) || n != uint32(s) {
			t.Fatalf("Write error: %d %v", n, err)
		}
		_, _ = o1.Write(b[:s])
	}
	if err := pipe(f, o2, 1009, 0, f.ptr.Size); err != nil { // intentional prime
		t.Fatalf("Read error: %v", err)
	}
	if !bytes.Equal(o1.Sum(nil), o2.Sum(nil)) {
		t.Fatalf("oid mismatch")
	}
}

func TestRemoteFile_Write_Middle(t *testing.T) {
	f, cancel := createRandomRemoteFile(testsize)
	defer cancel()

	o1 := sha256.New()
	o2 := sha256.New()
	b := make([]byte, 32)
	s := 31 // intentional prime
	if err := pipe(f, o1, 1009, 0, testsize/5); err != nil {
		t.Fatalf("Read error: %v", err)
	}
	i := testsize / 5
	for ; i < (testsize*3)/5; i += s {
		random(b)
		n, err := f.Write(context.Background(), b[:s], int64(i))
		if !errors.Is(err, syscall.Errno(0)) || n != uint32(s) {
			t.Fatalf("Write error: %d %v", n, err)
		}
		_, _ = o1.Write(b[:s])
	}
	if err := pipe(f, o1, 1009, int64(i), testsize); err != nil {
		t.Fatalf("Read error: %v", err)
	}
	if err := pipe(f, o2, 1009, 0, f.ptr.Size); err != nil { // intentional prime
		t.Fatalf("Read error: %v", err)
	}
	if !bytes.Equal(o1.Sum(nil), o2.Sum(nil)) {
		t.Fatalf("oid mismatch")
	}
}

func TestRemoteFile_Write_Append(t *testing.T) {
	f, cancel := createRandomRemoteFile(testsize)
	defer cancel()

	o1 := sha256.New()
	o2 := sha256.New()
	b := make([]byte, 32)
	s := 31 // intentional prime
	if err := pipe(f, o1, 1009, 0, testsize-100); err != nil {
		t.Fatalf("Read error: %v", err)
	}
	i := testsize - 100
	for ; i < testsize+pagesize*3/2; i += s {
		random(b)
		n, err := f.Write(context.Background(), b[:s], int64(i))
		if !errors.Is(err, syscall.Errno(0)) || n != uint32(s) {
			t.Fatalf("Write error: %d %v", n, err)
		}
		_, _ = o1.Write(b[:s])
	}
	if f.ptr.Size != int64(i) {
		t.Fatalf("Size mismatch")
	}
	if err := pipe(f, o2, 1009, 0, f.ptr.Size); err != nil { // intentional prime
		t.Fatalf("Read error: %v", err)
	}
	if !bytes.Equal(o1.Sum(nil), o2.Sum(nil)) {
		t.Fatalf("oid mismatch")
	}
}

func TestRemoteFile_Truncate(t *testing.T) {
	f, cancel := createRandomRemoteFile(testsize)
	defer cancel()

	// backup first 100 bytes
	b := make([]byte, 100)
	if _, err := f.Read(context.Background(), b, int64(0)); !errors.Is(err, syscall.Errno(0)) {
		t.Fatalf("Read error: %v", err)
	}
	// populate pages
	if err := pipe(f, io.Discard, 1009, 0, f.ptr.Size); err != nil {
		t.Fatalf("Read error: %v", err)
	}
	attr := &fuse.AttrOut{}
	if err := f.Getattr(context.Background(), attr); !errors.Is(err, syscall.Errno(0)) {
		t.Fatalf("Getattr error: %v", err)
	}
	if attr.Attr.Size != uint64(f.ptr.Size) {
		t.Fatalf("attr mismatch")
	}
	attrIn := &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			Size:      attr.Size,
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
	// truncate to 100
	attrIn.Size = 100
	if err := f.Setattr(context.Background(), attrIn, attr); !errors.Is(err, syscall.Errno(0)) {
		t.Fatalf("Setattr error: %v", err)
	}
	if err := f.Getattr(context.Background(), attr); !errors.Is(err, syscall.Errno(0)) {
		t.Fatalf("Getattr error: %v", err)
	}
	if f.ptr.Size != 100 {
		t.Fatalf("Size mismatch")
	}
	// enlarge
	attrIn.Size = testsize + 100
	if err := f.Setattr(context.Background(), attrIn, attr); !errors.Is(err, syscall.Errno(0)) {
		t.Fatalf("Setattr error: %v", err)
	}
	if err := f.Getattr(context.Background(), attr); !errors.Is(err, syscall.Errno(0)) {
		t.Fatalf("Getattr error: %v", err)
	}
	if f.ptr.Size != testsize+100 {
		t.Fatalf("Size mismatch")
	}

	o1 := sha256.New()
	o2 := sha256.New()

	o1.Write(b)
	o1.Write(make([]byte, testsize))

	if err := pipe(f, o2, 1009, 0, f.ptr.Size); err != nil { // intentional prime
		t.Fatalf("Read error: %v", err)
	}

	if !bytes.Equal(o1.Sum(nil), o2.Sum(nil)) {
		t.Fatalf("oid mismatch")
	}
}

func TestRemoteFile_TruncateClose(t *testing.T) {
	f, cancel := createRandomRemoteFile(testsize)
	defer cancel()

	attr := &fuse.AttrOut{}
	if err := f.Getattr(context.Background(), attr); !errors.Is(err, syscall.Errno(0)) {
		t.Fatalf("Getattr error: %v", err)
	}
	attrIn := &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			Size:      attr.Size,
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
	// truncate to 100
	attrIn.Size = 100
	if err := f.Setattr(context.Background(), attrIn, attr); !errors.Is(err, syscall.Errno(0)) {
		t.Fatalf("Setattr error: %v", err)
	}

	bs, _ := os.ReadFile(filepath.Join(f.pr, "tc"))
	tc, err := strconv.ParseInt(string(bs), 10, 64)
	if err != nil || tc != 100 {
		t.Fatalf("incorrect tc on disk: %v %v", tc, err)
	}
}

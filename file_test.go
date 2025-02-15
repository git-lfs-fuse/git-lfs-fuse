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
	"syscall"
	"testing"

	"github.com/git-lfs/git-lfs/v3/lfs"
)

type fetcher struct {
	f *os.File
}

func (f *fetcher) Fetch(ctx context.Context, w io.Writer, ptr *lfs.Pointer, off, end int64) error {
	if _, err := f.f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	_, err := io.CopyN(w, f.f, end-off)
	return err
}

func CreateRandomRemoteFile(size int64) (*RemoteFile, func()) {
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
	return NewRemoteFile(ptr, &fetcher{f: f}, d, int(p.Fd())), clean
}

const testsize = 5 * pagesize

func PipeAll(f *RemoteFile, w io.Writer, buf int) (err error) {
	b := make([]byte, buf)
	for i := int64(0); i < f.ptr.Size; i += int64(len(b)) {
		if i+int64(len(b)) > f.ptr.Size {
			b = b[:f.ptr.Size-i]
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
	f, cancel := CreateRandomRemoteFile(testsize)
	defer cancel()

	o := sha256.New()
	if err := PipeAll(f, o, 1009); err != nil { // intentional prime
		t.Fatalf("Read error: %v", err)
	}
	if hex.EncodeToString(o.Sum(nil)) != f.ptr.Oid {
		t.Fatalf("oid mismatch")
	}
}

func TestRemoteFile_Write_Full(t *testing.T) {
	f, cancel := CreateRandomRemoteFile(testsize)
	defer cancel()

	o1 := sha256.New()
	o2 := sha256.New()
	b := make([]byte, 32)
	s := 31 // intentional prime
	for i := 0; i < testsize; i += s {
		if i+s > testsize {
			s = testsize - i
		}
		binary.LittleEndian.PutUint64(b[0:8], rand.Uint64())
		binary.LittleEndian.PutUint64(b[8:16], rand.Uint64())
		binary.LittleEndian.PutUint64(b[16:24], rand.Uint64())
		binary.LittleEndian.PutUint64(b[24:32], rand.Uint64())
		n, err := f.Write(context.Background(), b[:s], int64(i))
		if !errors.Is(err, syscall.Errno(0)) || n != uint32(s) {
			t.Fatalf("Write error: %d %v", n, err)
		}
		_, _ = o1.Write(b[:s])
	}
	if err := PipeAll(f, o2, 1009); err != nil { // intentional prime
		t.Fatalf("Read error: %v", err)
	}
	if !bytes.Equal(o1.Sum(nil), o2.Sum(nil)) {
		t.Fatalf("oid mismatch")
	}
}

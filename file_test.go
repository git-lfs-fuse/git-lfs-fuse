package gitlfsfuse

import (
	"bufio"
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

const testsize = 4 * 1024 * 1024

func TestRemoteFile_Read(t *testing.T) {
	f, cancel := CreateRandomRemoteFile(testsize)
	defer cancel()

	o := sha256.New()
	b := make([]byte, 1009) // intentional prime
	for i := 0; i < testsize; i += len(b) {
		if i+len(b) > testsize {
			b = b[:testsize-i]
		}
		_, err := f.Read(context.Background(), b, int64(i))
		if !errors.Is(err, syscall.Errno(0)) {
			t.Fatalf("Read error: %v", err)
		}
		_, _ = o.Write(b)
	}
	if hex.EncodeToString(o.Sum(nil)) != f.ptr.Oid {
		t.Fatalf("oid mismatch")
	}
}

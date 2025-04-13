package gitlfsfuse

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	lts "github.com/git-lfs-fuse/lfs-test-server"
	"github.com/git-lfs/git-lfs/v3/config"
	"github.com/git-lfs/git-lfs/v3/lfs"
)

type repository struct {
	ln   net.Listener
	dir  string
	repo string
}

func (r *repository) Close() {
	if r.ln != nil {
		_ = r.ln.Close()
	}
	if r.dir != "" {
		_ = os.RemoveAll(r.dir)
	}
	if r.repo != "" {
		_ = os.RemoveAll(filepath.Dir(r.repo))
	}
}

func run(dir, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	bs, err := cmd.CombinedOutput()
	return string(bs), err
}

func startLFS(dir string) (net.Listener, error) {
	tl, err := lts.NewTrackingListener("tcp://:0")
	if err != nil {
		return nil, err
	}

	_, port, _ := net.SplitHostPort(tl.Addr().String())
	cfg := &lts.Configuration{
		AdminUser: "testuser",
		AdminPass: "testpass",
		ExtOrigin: "http://testuser:testpass@localhost:" + port,
	}

	metaStore, err := lts.NewMetaStore(cfg, filepath.Join(dir, "lfs.db"))
	if err != nil {
		return nil, err
	}

	contentStore, err := lts.NewContentStore(filepath.Join(dir, "content.db"))
	if err != nil {
		return nil, err
	}

	app := lts.NewApp(cfg, contentStore, metaStore)
	go func() {
		_ = app.Serve(tl)
	}()
	return tl, nil
}

func prepareRepo() (r *repository, err error) {
	r = &repository{}
	defer func() {
		if r != nil && err != nil {
			r.Close()
			r = nil
		}
	}()

	var tmp string
	tmp, err = os.MkdirTemp("", "glf-repo")
	if err != nil {
		return
	}
	defer os.RemoveAll(tmp)

	var f *os.File
	f, err = os.Create(filepath.Join(tmp, "emptylarge.bin"))
	if err != nil {
		return
	}
	if err = f.Truncate(pagesize * 5); err != nil {
		_ = f.Close()
		return
	}
	_ = f.Close()

	f, err = os.Create(filepath.Join(tmp, "normal.txt"))
	if err != nil {
		return
	}
	if err = f.Truncate(1024); err != nil {
		_ = f.Close()
		return
	}
	_ = f.Close()

	r.dir, err = os.MkdirTemp("", "glf-svc")
	if err != nil {
		return
	}
	r.ln, err = startLFS(r.dir)
	if err != nil {
		return
	}

	f, err = os.Create(filepath.Join(tmp, ".lfsconfig"))
	if err != nil {
		return
	}
	_, port, _ := net.SplitHostPort(r.ln.Addr().String())
	_, err = f.WriteString("[lfs]\n")
	_, err = f.WriteString("  url = \"http://testuser:testpass@localhost:" + port + "/\"\n")
	if err != nil {
		_ = f.Close()
		return
	}
	_ = f.Close()

	if _, err = run(tmp, "git", "init", "--initial-branch=main"); err != nil {
		return
	}
	cfg := config.NewIn(tmp, "")
	lfo := lfs.FilterOptions{
		GitConfig: cfg.GitConfig(),
		Force:     true,
		Local:     true,
	}
	if err = lfo.Install(); err != nil {
		return
	}
	if _, err = run(tmp, "git", "lfs", "track", "*.bin"); err != nil {
		return
	}
	if _, err = run(tmp, "git", "add", "-A"); err != nil {
		return
	}
	if _, err = run(tmp, "git", "config", "user.email", "testuser@example.com"); err != nil {
		return
	}
	if _, err = run(tmp, "git", "config", "user.name", "testuser"); err != nil {
		return
	}
	if _, err = run(tmp, "git", "commit", "-m", "msg"); err != nil {
		return
	}

	remote, err := os.MkdirTemp("", "glf-remote")
	if err != nil {
		_ = os.RemoveAll(remote)
		return
	}
	r.repo = filepath.Join(remote, "repo.git")
	if _, err = run(tmp, "git", "init", "--bare", r.repo, "--initial-branch=main"); err != nil {
		return
	}
	if _, err = run(tmp, "git", "remote", "add", "origin", r.repo); err != nil {
		return
	}
	if _, err = run(tmp, "git", "push", "-u", "origin", "main"); err != nil {
		return
	}

	// Prepare a new branch
	if _, err = run(tmp, "git", "checkout", "-b", "branch2"); err != nil {
		return
	}
	// Modify the files
	f, err = os.Create(filepath.Join(tmp, "emptylarge.bin"))
	if err != nil {
		return
	}
	if err = f.Truncate(pagesize); err != nil { // change the file size
		_ = f.Close()
		return
	}
	_ = f.Close()
	f, err = os.Create(filepath.Join(tmp, "emptylarge2.bin"))
	if err != nil {
		return
	}
	if err = f.Truncate(pagesize * 5); err != nil { // change the file size
		_ = f.Close()
		return
	}
	_ = f.Close()

	f, err = os.Create(filepath.Join(tmp, "normal.txt"))
	if err != nil {
		return
	}
	if err = f.Truncate(pagesize); err != nil { // change the file size
		_ = f.Close()
		return
	}
	_ = f.Close()
	f, err = os.Create(filepath.Join(tmp, "normal2.txt"))
	if err != nil {
		return
	}
	if err = f.Truncate(1024); err != nil { // change the file size
		_ = f.Close()
		return
	}
	_ = f.Close()

	if _, err = run(tmp, "git", "add", "-A"); err != nil {
		return
	}
	if _, err = run(tmp, "git", "commit", "-m", "msg"); err != nil {
		return
	}
	if _, err = run(tmp, "git", "push", "-u", "origin", "branch2"); err != nil {
		return
	}

	return r, nil
}

func cloneMount(t *testing.T) (hid, repo string, cancel func()) {
	var r *repository
	var mnt string
	var svc *Server
	var err error
	cancel = func() {
		if svc != nil {
			svc.Close()
			svc = nil
		}
		if mnt != "" {
			_ = os.RemoveAll(mnt)
			mnt = ""
		}
		if r != nil {
			r.Close()
			r = nil
		}
	}
	defer func() {
		if err != nil {
			cancel()
		}
	}()
	r, err = prepareRepo()
	if err != nil {
		t.Fatal(err)
	}
	mnt, err = os.MkdirTemp("", "glf-mnt")
	if err != nil {
		t.Fatal(err)
	}
	hid, repo, svc, err = CloneMount(r.repo, filepath.Join(mnt, "repo"), false, nil)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func verifyLocalFile(t *testing.T, hid, mnt, name string) os.FileInfo {
	t.Helper()

	fi1, err := os.Stat(filepath.Join(hid, name))
	if err != nil {
		t.Fatal(err)
	}
	fi2, err := os.Stat(filepath.Join(mnt, name))
	if err != nil {
		t.Fatal(err)
	}
	if fi1.Name() != fi2.Name() {
		t.Fatalf("file name doesn't match: %v != %v", fi1.Name(), fi2.Name())
	}
	if fi1.Mode() != fi2.Mode() {
		t.Fatalf("file mode doesn't match: %v != %v", fi1.Mode(), fi2.Mode())
	}
	if fi1.IsDir() != fi2.IsDir() {
		t.Fatalf("file isdir doesn't match: %v != %v", fi1.IsDir(), fi2.IsDir())
	}
	if fi1.ModTime() != fi2.ModTime() {
		t.Fatalf("file modtime doesn't match: %v != %v", fi1.ModTime(), fi2.ModTime())
	}
	if fi1.Size() != fi2.Size() {
		t.Fatalf("file size doesn't match: %v != %v", fi1.Size(), fi2.Size())
	}
	o1 := sha256.New()
	o2 := sha256.New()
	bytes1, err := os.ReadFile(filepath.Join(hid, name))
	if err != nil {
		t.Fatalf("ReadFile error: %v", err)
	}
	bytes2, err := os.ReadFile(filepath.Join(mnt, name))
	if err != nil {
		t.Fatalf("ReadFile error: %v", err)
	}
	o1.Write(bytes1)
	o2.Write(bytes2)
	if sum1, sum2 := hex.EncodeToString(o1.Sum(nil)), hex.EncodeToString(o2.Sum(nil)); sum1 != sum2 {
		t.Fatalf("file sum doesn't match: %v != %v", sum1, sum2)
	}

	return fi2
}

func verifyRemoteFile(t *testing.T, hid, mnt, name string) os.FileInfo {
	t.Helper()

	fi1, err := os.Stat(filepath.Join(hid, name))
	if err != nil {
		t.Fatal(err)
	}
	fi2, err := os.Stat(filepath.Join(mnt, name))
	if err != nil {
		t.Fatal(err)
	}
	if fi1.Name() != fi2.Name() {
		t.Fatalf("file name doesn't match: %v != %v", fi1.Name(), fi2.Name())
	}
	if fi1.Mode() != fi2.Mode() {
		t.Fatalf("file mode doesn't match: %v != %v", fi1.Mode(), fi2.Mode())
	}
	if fi1.IsDir() != fi2.IsDir() {
		t.Fatalf("file isdir doesn't match: %v != %v", fi1.IsDir(), fi2.IsDir())
	}
	if fi1.ModTime() != fi2.ModTime() {
		t.Fatalf("file modtime doesn't match: %v != %v", fi1.ModTime(), fi2.ModTime())
	}
	ptr, err := lfs.DecodePointerFromFile(filepath.Join(hid, name))
	if err != nil {
		t.Fatal(err)
	}
	if fi2.Size() != ptr.Size {
		t.Fatalf("file size doesn't match: %v != %v", fi2.Size(), ptr.Size)
	}
	o := sha256.New()
	bytes, err := os.ReadFile(filepath.Join(mnt, name))
	if err != nil {
		t.Fatalf("ReadFile error: %v", err)
	}
	o.Write(bytes)
	if sum := hex.EncodeToString(o.Sum(nil)); ptr.Oid != sum {
		t.Fatalf("oid doesn't match: %v != %v", ptr.Oid, sum)
	}

	return fi2
}

func TestMount(t *testing.T) {
	hid, mnt, cancel := cloneMount(t)
	defer cancel()

	_ = verifyLocalFile(t, hid, mnt, "normal.txt")
	_ = verifyRemoteFile(t, hid, mnt, "emptylarge.bin")
}

func TestMountCheckout(t *testing.T) {
	hid, mnt, cancel := cloneMount(t)
	defer cancel()

	ni1 := verifyLocalFile(t, hid, mnt, "normal.txt")
	if ni1.Size() != 1024 {
		t.Fatalf("file size doesn't match: %v != %v", ni1.Size(), 1024)
	}

	fi1 := verifyRemoteFile(t, hid, mnt, "emptylarge.bin")
	if fi1.Size() != pagesize*5 {
		t.Fatalf("file size doesn't match: %v != %v", fi1.Size(), pagesize*5)
	}

	if _, err := run(mnt, "git", "checkout", "-f", "branch2"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(mnt, "git", "pull"); err != nil {
		t.Fatal(err)
	}

	ni2 := verifyLocalFile(t, hid, mnt, "normal.txt")
	if ni2.Size() != pagesize {
		t.Fatalf("file size doesn't match: %v != %v", ni1.Size(), pagesize)
	}

	fi2 := verifyRemoteFile(t, hid, mnt, "emptylarge.bin")
	if fi2.Size() != pagesize {
		t.Fatalf("file size doesn't match: %v != %v", fi2.Size(), pagesize)
	}

	_ = verifyLocalFile(t, hid, mnt, "normal2.txt")
	_ = verifyRemoteFile(t, hid, mnt, "emptylarge2.bin")
}

// 4. As a user, I can write local files in the mounted local repository correctly.
// 6. As a user, I can commit modified local files using the Git command correctly.
func TestLocalFileWrite(t *testing.T) {
	hid, mnt, cancel := cloneMount(t)
	defer cancel()

	newContent := []byte("new local content")
	filePath := filepath.Join(mnt, "normal.txt")
	if err := os.WriteFile(filePath, newContent, 0644); err != nil {
		t.Fatalf("write local file error: %v", err)
	}

	mntContent, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read file from mnt: %v", err)
	}
	if string(mntContent) != string(newContent) {
		t.Fatalf("mnt file content mismatch: got %q, want %q", mntContent, newContent)
	}

	if _, err := run(mnt, "git", "add", "normal.txt"); err != nil {
		t.Fatalf("git add error: %v", err)
	}
	if _, err := run(mnt, "git", "commit", "-m", "Update normal.txt with new content"); err != nil {
		t.Fatalf("git commit error: %v", err)
	}
	_ = verifyLocalFile(t, hid, mnt, "normal.txt")

	if _, err = run(mnt, "git", "push", "-u", "origin", "main"); err != nil {
		t.Fatalf("git push error: %v", err)
	}
}

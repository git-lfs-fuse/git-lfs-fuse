package gitlfsfuse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	lts "github.com/git-lfs-fuse/lfs-test-server"
	"github.com/git-lfs/git-lfs/v3/config"
	"github.com/git-lfs/git-lfs/v3/lfs"
	"github.com/gorilla/mux"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type repository struct {
	ln   net.Listener
	app  *lts.App
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

func startLFS(dir string) (net.Listener, *lts.App, error) {
	tl, err := lts.NewTrackingListener("tcp://:0")
	if err != nil {
		return nil, nil, err
	}

	_, port, _ := net.SplitHostPort(tl.Addr().String())
	cfg := &lts.Configuration{
		AdminUser: "testuser",
		AdminPass: "testpass",
		ExtOrigin: "http://testuser:testpass@localhost:" + port,
	}

	metaStore, err := lts.NewMetaStore(cfg, filepath.Join(dir, "lfs.db"))
	if err != nil {
		return nil, nil, err
	}

	contentStore, err := lts.NewContentStore(filepath.Join(dir, "content.db"))
	if err != nil {
		return nil, nil, err
	}

	app := lts.NewApp(cfg, contentStore, metaStore)
	go func() {
		_ = app.Serve(tl)
	}()
	return tl, app, nil
}

func prepareRepo() (r *repository, err error) {
	logRun := func(dir string, cmd string, args ...string) error {
		log.Printf("Running: %s %s", cmd, strings.Join(args, " "))
		out, err := run(dir, cmd, args...)
		if err != nil {
			log.Printf("FAILED: %v\nOutput: %s", err, out)
		}
		return err
	}

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
	r.ln, r.app, err = startLFS(r.dir)
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

	if err = logRun(tmp, "git", "init", "--initial-branch=main"); err != nil {
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

	if err = logRun(tmp, "git", "lfs", "track", "*.bin"); err != nil {
		return
	}
	if err = logRun(tmp, "git", "add", "-A"); err != nil {
		return
	}
	if err = logRun(tmp, "git", "config", "user.email", "testuser@example.com"); err != nil {
		return
	}
	if err = logRun(tmp, "git", "config", "user.name", "testuser"); err != nil {
		return
	}
	if err = logRun(tmp, "git", "commit", "-m", "msg"); err != nil {
		return
	}

	remote, err := os.MkdirTemp("", "glf-remote")
	if err != nil {
		_ = os.RemoveAll(remote)
		return
	}
	r.repo = filepath.Join(remote, "repo.git")
	if err = logRun(tmp, "git", "init", "--bare", r.repo, "--initial-branch=main"); err != nil {
		return
	}
	if err = logRun(tmp, "git", "remote", "add", "origin", r.repo); err != nil {
		return
	}
	if err = logRun(tmp, "git", "push", "-u", "origin", "main"); err != nil {
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
	return cloneMountWithCacheSize(t, 5120, nil)
}

func cloneMountWithCacheSize(t *testing.T, maxPage int64, middleware mux.MiddlewareFunc) (hid, repo string, cancel func()) {
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
	r.app.Middleware = middleware

	mnt, err = os.MkdirTemp("", "glf-mnt")
	if err != nil {
		t.Fatal(err)
	}
	hid, repo, svc, err = CloneMount(r.repo, filepath.Join(mnt, "repo"), false, nil, maxPage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = run(hid, "git", "config", "user.email", "testuser@example.com"); err != nil {
		t.Fatal(err)
	}
	if _, err = run(hid, "git", "config", "user.name", "testuser"); err != nil {
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
	RecordNodeOperations = true
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

func TestLocalFileWrite(t *testing.T) {
	RecordNodeOperations = true
	hid, mnt, cancel := cloneMount(t)
	defer cancel()

	// modify normal.txt
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

	// create a new local file
	newFilePath := filepath.Join(mnt, "normal3.txt")
	f, err := os.Create(newFilePath)
	if err != nil {
		t.Fatalf("create new file error: %v", err)
	}
	defer f.Close()

	if _, err := run(mnt, "git", "add", "normal.txt"); err != nil {
		t.Fatalf("git add error: %v", err)
	}
	if out, err := run(mnt, "git", "commit", "-m", "Modify normal.txt"); err != nil {
		t.Fatalf("git commit error: %s\n%v", out, err)
	}
	_ = verifyLocalFile(t, hid, mnt, "normal.txt")

	if _, err := run(mnt, "git", "add", "normal3.txt"); err != nil {
		t.Fatalf("git add error: %v", err)
	}
	if out, err := run(mnt, "git", "commit", "-m", "Add normal3.txt"); err != nil {
		t.Fatalf("git commit error: %s\n%v", out, err)
	}
	_ = verifyLocalFile(t, hid, mnt, "normal3.txt")

	if _, err := run(mnt, "git", "push", "-u", "origin", "main"); err != nil {
		t.Fatalf("git push error: %v", err)
	}

	// Clone the remote repository to verify the pushed content.
	cloneDir, err := os.MkdirTemp("", "remote-check")
	if err != nil {
		t.Fatalf("MkdirTemp error: %v", err)
	}
	defer os.RemoveAll(cloneDir)

	remoteURL, err := run(mnt, "git", "remote", "get-url", "origin")
	if err != nil {
		t.Fatalf("failed to get remote URL: %v", err)
	}
	remoteURL = strings.TrimSpace(remoteURL)

	out, err := run(mnt, "git", "clone", remoteURL, cloneDir)
	if err != nil {
		t.Fatalf("git clone error: %v\nOutput: %s", err, out)
	}

	remoteNormal, err := os.ReadFile(filepath.Join(cloneDir, "normal.txt"))
	if err != nil {
		t.Fatalf("failed to read normal.txt from remote clone: %v", err)
	}
	if string(remoteNormal) != string(newContent) {
		t.Fatalf("remote normal.txt content mismatch: got %q, want %q", remoteNormal, newContent)
	}

	remoteNormal3, err := os.ReadFile(filepath.Join(cloneDir, "normal3.txt"))
	if err != nil {
		t.Fatalf("failed to read normal3.txt from remote clone: %v", err)
	}
	if len(remoteNormal3) != 0 {
		t.Fatalf("remote normal3.txt content mismatch: expected empty file, got %q", remoteNormal3)
	}
}

func TestRemoteFileWrite(t *testing.T) {
	RecordNodeOperations = true
	hid, mnt, cancel := cloneMount(t)
	defer cancel()

	// modify emptylarge.bin
	newContent := []byte("new remote content")
	filePath := filepath.Join(mnt, "emptylarge.bin")
	if err := os.WriteFile(filePath, newContent, 0644); err != nil {
		t.Fatalf("failed to write remote file: %v", err)
	}

	mntContent, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read file from mnt: %v", err)
	}
	if string(mntContent) != string(newContent) {
		t.Fatalf("mnt file content mismatch: got %q, want %q", mntContent, newContent)
	}

	// create a new remote file
	newFilePath := filepath.Join(mnt, "emptylarge3.bin")
	f, err := os.Create(newFilePath)
	if err != nil {
		t.Fatalf("create new file error: %v", err)
	}
	f.Close()

	if _, err := run(mnt, "git", "add", "emptylarge.bin"); err != nil {
		t.Fatalf("git add error: %v", err)
	}
	if _, err := run(mnt, "git", "commit", "-m", "Modify emptylarge.bin"); err != nil {
		t.Fatalf("git commit error: %v", err)
	}
	if _, err := run(mnt, "git", "add", "emptylarge3.bin"); err != nil {
		t.Fatalf("git add error: %v", err)
	}
	if _, err := run(mnt, "git", "commit", "-m", "Add emptylarge3.bin"); err != nil {
		t.Fatalf("git commit error: %v", err)
	}

	// checkout back to the original branch (main) to refresh the new pointer files
	if _, err := run(mnt, "git", "checkout", "-f", "branch2"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(mnt, "git", "checkout", "-f", "main"); err != nil {
		t.Fatal(err)
	}

	_ = verifyRemoteFile(t, hid, mnt, "emptylarge.bin")
	_ = verifyRemoteFile(t, hid, mnt, "emptylarge3.bin")

	if _, err := run(mnt, "git", "push", "-u", "origin", "main"); err != nil {
		t.Fatalf("git push error: %v", err)
	}

	// Clone the remote repository to verify the pushed content.
	cloneDir, err := os.MkdirTemp("", "remote-check")
	if err != nil {
		t.Fatalf("MkdirTemp error: %v", err)
	}
	defer os.RemoveAll(cloneDir)

	remoteURL, err := run(mnt, "git", "remote", "get-url", "origin")
	if err != nil {
		t.Fatalf("failed to get remote URL: %v", err)
	}
	remoteURL = strings.TrimSpace(remoteURL)

	out, err := run(mnt, "git", "clone", remoteURL, cloneDir)
	if err != nil {
		t.Fatalf("git clone error: %v\nOutput: %s", err, out)
	}

	remoteLarge, err := os.ReadFile(filepath.Join(cloneDir, "emptylarge.bin"))
	if err != nil {
		t.Fatalf("failed to read emptylarge.bin from remote clone: %v", err)
	}
	if string(remoteLarge) != string(newContent) {
		t.Fatalf("remote emptylarge.bin content mismatch: got %q, want %q", remoteLarge, newContent)
	}

	remoteLarge3, err := os.ReadFile(filepath.Join(cloneDir, "emptylarge3.bin"))
	if err != nil {
		t.Fatalf("failed to read emptylarge3.bin from remote clone: %v", err)
	}
	if len(remoteLarge3) != 0 {
		t.Fatalf("remote emptylarge3.bin content mismatch: expected empty file, got %q", remoteLarge3)
	}
}

// 10. As a user, I can access remote Git-LFS tracked files without storing them entirely locally by specifying the cache size.
func TestLimitedCacheSize(t *testing.T) {
	RecordNodeOperations = true
	// Set a very small cache size (2 pages) to force eviction
	smallCacheSize := int64(2)

	// Clone and mount with limited cache size
	hid, mnt, cancel := cloneMountWithCacheSize(t, smallCacheSize, nil)
	defer cancel()

	// First, check out branch2 to access both test files
	if _, err := run(mnt, "git", "checkout", "-f", "branch2"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(mnt, "git", "pull"); err != nil {
		t.Fatal(err)
	}

	// Count pages in the shared directory
	countSharedPages := func() (int64, error) {
		fuseDir := filepath.Join(hid, ".git", "fuse")
		count := int64(0)

		entries, err := os.ReadDir(fuseDir)
		if err != nil {
			return 0, err
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}

			oid := entry.Name()
			sharedDir := filepath.Join(fuseDir, oid, "shared")
			sharedEntries, err := os.ReadDir(sharedDir)
			if err != nil {
				continue
			}

			for _, sharedEntry := range sharedEntries {
				if !sharedEntry.IsDir() && sharedEntry.Name() != "tc" {
					count++
				}
			}
		}
		return count, nil
	}

	// Get the initial page count before accessing any files
	if count, err := countSharedPages(); err != nil || count != 0 {
		t.Error("failed to count shared pages", count, err)
	}

	_ = verifyRemoteFile(t, hid, mnt, "emptylarge.bin")
	_ = verifyRemoteFile(t, hid, mnt, "emptylarge2.bin")
	_ = verifyRemoteFile(t, hid, mnt, "emptylarge2.bin")
	_ = verifyRemoteFile(t, hid, mnt, "emptylarge.bin")

	if count, err := countSharedPages(); err != nil || count > defaultpreload {
		t.Error("failed to count shared pages", count, err)
	}
}

func TestFsNodeOperations(t *testing.T) {
	RecordNodeOperations = true
	root, err := os.MkdirTemp("", "fsnode_ops")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	if out, err := exec.Command("git", "init", "--initial-branch=main", root).CombinedOutput(); err != nil {
		t.Fatalf("git init failed: %s\n%v", out, err)
	}
	cfg := config.NewIn(root, "")
	node, cancel, err := NewGitLFSFuseRoot(root, cfg, 5120)
	if err != nil {
		t.Fatalf("NewGitLFSFuseRoot error: %v", err)
	}
	defer cancel()
	fsnode := node.(*FSNode)

	mnt, err := os.MkdirTemp("", "mnt")
	if err != nil {
		t.Fatalf("MkdirTemp error: %v", err)
	}
	srv, err := fs.Mount(mnt, fsnode, &fs.Options{NullPermissions: true})
	defer srv.Unmount()

	ctx := context.Background()
	t.Run("Statfs", func(t *testing.T) {
		var out fuse.StatfsOut
		errno := fsnode.Statfs(ctx, &out)
		if errno != 0 {
			t.Fatalf("FSNode.Statfs returned error: %v", errno)
		}
	})

	t.Run("Mknod", func(t *testing.T) {
		testFile := "test_mknod.txt"
		var out fuse.EntryOut
		inode, errno := fsnode.Mknod(ctx, testFile, fuse.S_IFREG|0644, 0, &out)
		if errno != 0 {
			t.Fatalf("FSNode.Mknod returned error: %v", errno)
		}
		if inode == nil {
			t.Fatalf("FSNode.Mknod returned nil inode")
		}
	})

	t.Run("Rmdir_Readdir", func(t *testing.T) {
		subdir := "testdir"
		subdirPath := filepath.Join(root, subdir)
		if err := os.Mkdir(subdirPath, 0755); err != nil {
			t.Fatalf("Failed to create subdirectory: %v", err)
		}

		_, errno := fsnode.Readdir(ctx)
		if errno != 0 {
			t.Fatalf("FSNode.Readdir returned error: %v", errno)
		}

		errno = fsnode.Rmdir(ctx, subdir)
		if errno != 0 {
			t.Fatalf("FSNode.Rmdir returned error: %v", errno)
		}

		_, err = os.Stat(subdirPath)
		if err == nil || !os.IsNotExist(err) {
			t.Fatalf("Subdirectory %q still exists after Rmdir", subdirPath)
		}
	})

	t.Run("Symlink_Readlink", func(t *testing.T) {
		symName := "test_symlink"
		linkPath := filepath.Join(mnt, symName)
		target := "target.txt"
		targetPath := filepath.Join(mnt, target)
		if err := os.WriteFile(targetPath, []byte("target content"), 0644); err != nil {
			t.Fatalf("Failed to create target file: %v", err)
		}

		if err := os.Symlink(targetPath, linkPath); err != nil {
			t.Fatalf("Failed to create symlink: %v", err)
		}

		_, err := os.Readlink(linkPath)
		if err != nil {
			t.Fatalf("Failed to read symlink: %v", err)
		}
	})

	t.Run("Setxattr_Removexattr_Listxattr", func(t *testing.T) {
		attrName := "user.testattr"
		attrValue := []byte("testvalue")

		errno := fsnode.Setxattr(ctx, attrName, attrValue, 0)
		if errno != 0 {
			t.Fatalf("FSNode.Setxattr returned error: %v", errno)
		}

		_, errno = fsnode.Listxattr(ctx, nil)
		if errno != 0 {
			t.Fatalf("FSNode.Listxattr returned error: %v", errno)
		}

		errno = fsnode.Removexattr(ctx, attrName)
		if errno != 0 {
			t.Fatalf("FSNode.Removexattr returned error: %v", errno)
		}
	})

	t.Run("Fsync", func(t *testing.T) {
		fh, _, errno := fsnode.Open(ctx, uint32(os.O_RDONLY))
		if errno != 0 {
			t.Fatalf("FSNode.Open returned error: %v", errno)
		}

		errno = fsnode.Fsync(ctx, fh, uint32(os.O_RDWR))
		if errno != 0 {
			t.Fatalf("FSNode.Fsync returned error: %v", errno)
		}

		errno = fsnode.Release(ctx, fh)
		if errno != 0 {
			t.Fatalf("FSNode.Release returned error: %v", errno)
		}
	})

	t.Run("Setattr", func(t *testing.T) {
		ptrFile := "test_ptr"
		mntPath := filepath.Join(mnt, ptrFile)
		ptrContent :=
			"version https://git-lfs.github.com/spec/v1\n" +
				"oid sha256:3b2e9e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f\n" +
				"size 256\n"
		if err := os.WriteFile(mntPath, []byte(ptrContent), 0644); err != nil {
			log.Fatalf("Failed to write pointer file: %v", err)
		}

		if err := os.Truncate(mntPath, 128); err != nil {
			t.Fatalf("truncate failed: %v", err)
		}
	})

	t.Run("ConcurrentOpen", func(t *testing.T) {
		ptrFile := "test_ptr_concurrent"
		mntPath := filepath.Join(mnt, ptrFile)
		ptrContent :=
			"version https://git-lfs.github.com/spec/v1\n" +
				"oid sha256:3b2e9e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f1e8f\n" +
				"size 256\n"
		if err := os.WriteFile(mntPath, []byte(ptrContent), 0644); err != nil {
			log.Fatalf("Failed to write pointer file: %v", err)
		}

		var wg sync.WaitGroup
		files := make([]*os.File, 100)
		wg.Add(100)
		for i := 0; i < 100; i++ {
			go func(i int) {
				defer wg.Done()
				files[i], err = os.OpenFile(mntPath, os.O_RDONLY, 0644)
				if err != nil {
					t.Errorf("failed to open file: %v", err)
				}
				fi, err := files[i].Stat()
				if err != nil {
					t.Errorf("failed to open file: %v", err)
				}
				if fi.Size() != 256 {
					t.Errorf("file size mismatch: got %d, want %d", fi.Size(), 256)
				}
			}(i)
		}
		wg.Wait()
		wg.Add(100)
		for i := 0; i < 100; i++ {
			go func(i int) {
				defer wg.Done()
				_ = files[i].Close()
			}(i)
		}
		wg.Wait()
	})
}

func TestE2EFileOperation(t *testing.T) {
	RecordNodeOperations = true
	_, mnt, cancel := cloneMount(t)
	defer cancel()

	t.Run("Fsync", func(t *testing.T) {
		p := filepath.Join(mnt, "emptylarge.bin")
		f, err := os.OpenFile(p, os.O_RDWR, 0644)
		if err != nil {
			t.Fatalf("failed to open file: %v", err)
		}
		if err := f.Truncate(0); err != nil {
			t.Fatalf("failed to truncate file: %v", err)
		}
		if _, err := f.WriteString("new content"); err != nil {
			t.Fatalf("failed to write file: %v", err)
		}
		if err := f.Sync(); err != nil {
			t.Fatalf("failed to sync file: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("failed to close file: %v", err)
		}
		out, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("failed to read file: %v", err)
		}
		if string(out) != "new content" {
			t.Fatalf("file content mismatch: got %q, want %q", out, "new content")
		}
	})
}

func TestBlockSize(t *testing.T) {
	RecordNodeOperations = true
	_, mnt, cancel := cloneMount(t)
	defer cancel()

	target := filepath.Join(mnt, "emptylarge.bin")

	fi, err := os.Stat(target)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	size := fi.Size()
	wantBlockSize := int64(size+511) / 512
	sys := fi.Sys().(*syscall.Stat_t)
	gotBlocks := int64(sys.Blocks)
	if wantBlockSize != gotBlocks {
		t.Fatalf("block size mismatch: got %d, want %d. size = %d", gotBlocks, wantBlockSize, size)
	}

	_, _ = os.ReadFile(target)
	fi2, err := os.Stat(target)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	sys2 := fi2.Sys().(*syscall.Stat_t)
	if wantBlockSize != int64(sys2.Blocks) {
		t.Fatalf("block size mismatch after read: got %d, want %d. size = %d", sys2.Blocks, wantBlockSize, size)
	}
}

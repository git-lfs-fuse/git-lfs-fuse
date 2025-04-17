package gitlfsfuse

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
	return cloneMountWithCacheSize(t, 5120)
}

func cloneMountWithCacheSize(t *testing.T, maxPage int64) (hid, repo string, cancel func()) {
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
	hid, repo, svc, err = CloneMount(r.repo, filepath.Join(mnt, "repo"), false, nil, maxPage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = run(repo, "git", "config", "user.email", "testuser@example.com"); err != nil {
		t.Fatal(err)
	}
	if _, err = run(repo, "git", "config", "user.name", "testuser"); err != nil {
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

	// create new local file
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
}

// 7. As a user, I can write remote files in the mounted local repository correctly.
// 8. As a user, I can commit modified remote files using the Git command correctly.
func TestRemoteFileWrite(t *testing.T) {
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

	// create new remote file
	newFilePath := filepath.Join(mnt, "emptylarge3.bin")
	f, err := os.Create(newFilePath)
	if err != nil {
		return
	}
	defer f.Close()
	if err := os.WriteFile(newFilePath, newContent, 0644); err != nil {
		t.Fatalf("failed to write remote file: %v", err)
	}

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

	if _, err := run(mnt, "git", "push", "-u", "origin", "main"); err != nil {
		t.Fatalf("git push error: %v", err)
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
}

// 10. As a user, I can access remote Git-LFS tracked files without storing them entirely locally by specifying the cache size.
func TestLimitedCacheSize(t *testing.T) {
	// Set a very small cache size (2 pages) to force eviction
	smallCacheSize := int64(2)
	
	// Clone and mount with limited cache size
	hid, mnt, cancel := cloneMountWithCacheSize(t, smallCacheSize)
	defer cancel()

	// First, checkout branch2 to access both test files
	if _, err := run(mnt, "git", "checkout", "-f", "branch2"); err != nil {
		t.Fatal(err)
	}

	// Verify we can access both remote files
	_ = verifyRemoteFile(t, hid, mnt, "emptylarge.bin")
	_ = verifyRemoteFile(t, hid, mnt, "emptylarge2.bin")

	// Count pages in the shared directory
	countSharedPages := func() map[string]int {
		fuseDir := filepath.Join(hid, ".git", "fuse")
		pagesByOid := make(map[string]int)
		
		entries, err := os.ReadDir(fuseDir)
		if err != nil {
			t.Logf("Error reading fuse dir: %v", err)
			return pagesByOid
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
			
			pageCount := 0
			for _, sharedEntry := range sharedEntries {
				if !sharedEntry.IsDir() && sharedEntry.Name() != "tc" {
					pageCount++
				}
			}
			
			if pageCount > 0 {
				pagesByOid[oid] = pageCount
			}
		}
		return pagesByOid
	}
	
	// Get initial page count
	initialPages := countSharedPages()
	totalInitialPages := 0
	for _, count := range initialPages {
		totalInitialPages += count
	}
	t.Logf("Initial pages by OID: %v (total: %d)", initialPages, totalInitialPages)

	// Now force page usage by reading both files multiple times
	// This ensures we exceed the cache limit and trigger eviction
	for i := range 10 {
		// Read from emptylarge.bin (1 page file)
		file1, err := os.Open(filepath.Join(mnt, "emptylarge.bin"))
		if err != nil {
			t.Fatalf("Failed to open emptylarge.bin: %v", err)
		}
		buffer := make([]byte, 8192) // Small buffer to force multiple reads
		for {
			_, err := file1.Read(buffer)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Error reading emptylarge.bin: %v", err)
			}
		}
		file1.Close()
		
		// Read from emptylarge2.bin (5 page file)
		file2, err := os.Open(filepath.Join(mnt, "emptylarge2.bin"))
		if err != nil {
			t.Fatalf("Failed to open emptylarge2.bin: %v", err)
		}
		for {
			_, err := file2.Read(buffer)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Error reading emptylarge2.bin: %v", err)
			}
		}
		file2.Close()
		
		// Check current page count
		currentPages := countSharedPages()
		totalCurrentPages := 0
		for _, count := range currentPages {
			totalCurrentPages += count
		}
		t.Logf("After iteration %d, pages by OID: %v (total: %d)", i+1, currentPages, totalCurrentPages)
		
		// Verify the cache size is being respected
		// We allow some flexibility here since there might be in-flight operations
		if totalCurrentPages > int(smallCacheSize)*2 {
			t.Errorf("Cache size exceeds the expected limit: got %d pages, expected no more than %d",
				totalCurrentPages, smallCacheSize*2)
		}
	}

	// Final verification - ensure both files can still be accessed
	// even though the cache size was limited
	emptylargePath := filepath.Join(mnt, "emptylarge.bin")
	emptylarge2Path := filepath.Join(mnt, "emptylarge2.bin")
	
	// Read part of emptylarge.bin
	file1, err := os.Open(emptylargePath)
	if err != nil {
		t.Fatalf("Failed to access emptylarge.bin after multiple reads: %v", err)
	}
	buf1 := make([]byte, 1024)
	n1, err := file1.Read(buf1)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read from emptylarge.bin: %v", err)
	}
	file1.Close()
	t.Logf("Successfully read %d bytes from emptylarge.bin after multiple reads", n1)
	
	// Read part of emptylarge2.bin
	file2, err := os.Open(emptylarge2Path)
	if err != nil {
		t.Fatalf("Failed to access emptylarge2.bin after multiple reads: %v", err)
	}
	buf2 := make([]byte, 1024)
	n2, err := file2.Read(buf2)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read from emptylarge2.bin: %v", err)
	}
	file2.Close()
	t.Logf("Successfully read %d bytes from emptylarge2.bin after multiple reads", n2)
	
	// Final page count
	finalPages := countSharedPages()
	totalFinalPages := 0
	for _, count := range finalPages {
		totalFinalPages += count
	}
	t.Logf("Final pages by OID: %v (total: %d)", finalPages, totalFinalPages)
	
	// Verify that despite extensive reading, the cache remained limited
	if totalFinalPages > int(smallCacheSize)*2 {
		t.Errorf("Final cache size exceeds the expected limit: got %d pages, expected no more than %d",
			totalFinalPages, smallCacheSize*2)
	}
	
	t.Logf("Successfully verified limited cache functionality")
}

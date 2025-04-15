package gitlfsfuse

import (
	"crypto/sha256"
	"encoding/hex"
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
	// Logging wrapper for shell commands
	logRun := func(label string, cmd string, args ...string) error {
		log.Printf("Running [%s]: %s %s", label, cmd, strings.Join(args, " "))
		out, err := run("", cmd, args...)
		if err != nil {
			log.Printf("FAILED [%s]: %v\nOutput: %s", label, err, out)
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

	if err = logRun("git init", "git", "init", "--initial-branch=main"); err != nil {
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

	if err = logRun("git lfs track", "git", "lfs", "track", "*.bin"); err != nil {
		return
	}
	if err = logRun("git add", "git", "add", "-A"); err != nil {
		return
	}
	if err = logRun("git config email", "git", "config", "user.email", "testuser@example.com"); err != nil {
		return
	}
	if err = logRun("git config name", "git", "config", "user.name", "testuser"); err != nil {
		return
	}
	if err = logRun("git commit", "git", "commit", "-m", "msg"); err != nil {
		return
	}

	remote, err := os.MkdirTemp("", "glf-remote")
	if err != nil {
		_ = os.RemoveAll(remote)
		return
	}
	r.repo = filepath.Join(remote, "repo.git")

	if err = logRun("git init bare", "git", "init", "--bare", r.repo, "--initial-branch=main"); err != nil {
		return
	}
	if err = logRun("git remote add", "git", "remote", "add", "origin", r.repo); err != nil {
		return
	}
	if err = logRun("git push", "git", "push", "-u", "origin", "main"); err != nil {
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
	hid, repo, svc, err = CloneMount(r.repo, filepath.Join(mnt, "repo"), false, nil, 5120)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func TestMount(t *testing.T) {
	hid, mnt, cancel := cloneMount(t)
	defer cancel()

	fi1, err := os.Stat(filepath.Join(hid, "emptylarge.bin"))
	if err != nil {
		t.Fatal(err)
	}
	fi2, err := os.Stat(filepath.Join(mnt, "emptylarge.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if fi1.Name() != fi2.Name() {
		t.Errorf("fi doesn't match")
	}
	if fi1.Mode() != fi2.Mode() {
		t.Errorf("fi doesn't match")
	}
	if fi1.IsDir() != fi2.IsDir() {
		t.Errorf("fi doesn't match")
	}
	if fi1.ModTime() != fi2.ModTime() {
		t.Errorf("fi doesn't match")
	}
	ptr, err := lfs.DecodePointerFromFile(filepath.Join(hid, "emptylarge.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if fi2.Size() != ptr.Size {
		t.Errorf("fi doesn't match")
	}
	o := sha256.New()
	bytes, err := os.ReadFile(filepath.Join(mnt, "emptylarge.bin"))
	if err != nil {
		t.Errorf("ReadFile error: %v", err)
	}
	o.Write(bytes)
	if ptr.Oid != hex.EncodeToString(o.Sum(nil)) {
		t.Errorf("oid doesn't match")
	}
}

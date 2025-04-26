package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

type repository struct {
	dir  string
	repo string
}

func (r *repository) Close() {
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

	f, err := os.Create(filepath.Join(tmp, "normal.txt"))
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

	if _, err = run(tmp, "git", "init", "--initial-branch=main"); err != nil {
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

	return r, nil
}

func TestMainMount(t *testing.T) {
	repo, err := prepareRepo()
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()

	mnt, err := os.MkdirTemp("", "glf-mnt")
	if err != nil {
		return
	}
	defer os.RemoveAll(mnt)

	done := make(chan struct{})
	go func() {
		os.Args = []string{"main", "mount", repo.repo, mnt, "--origin", "origin", "--branch", "main", "--depth", "1", "--max-pages", "0", "--no-tags"}
		main()
		close(done)
	}()

	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(time.Second)
			entries, err := os.ReadDir(mnt)
			if err != nil {
				t.Log(err)
			}
			for _, e := range entries {
				if e.Name() == "normal.txt" {
					goto stop
				}
			}
		}
		t.Error("timed out")
	stop:
		p, err := os.FindProcess(os.Getpid())
		if err != nil {
			t.Error(err)
		}
		if err := p.Signal(os.Interrupt); err != nil {
			t.Error(err)
		}
	}()

	<-done
}

func TestNoMount(t *testing.T) {
	repo, err := prepareRepo()
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()

	mnt, err := os.MkdirTemp("", "glf-mnt")
	if err != nil {
		return
	}
	defer os.RemoveAll(mnt)

	os.Args = []string{"main", "mount", repo.repo, mnt, "--origin", "origin", "--branch", "main", "--depth", "1", "--max-pages", "0", "--no-tags", "--no-mount", "--debug"}
	main()
}

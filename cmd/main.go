package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"

	gitlfsfuse "github.com/git-lfs-fuse/git-lfs-fuse"
	"github.com/git-lfs/git-lfs/v3/config"
	"github.com/git-lfs/git-lfs/v3/lfs"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func main() {
	log.SetFlags(0)
	var entryCmd = &cobra.Command{
		Use: "git-lfs-fuse",
	}
	var mountCmd = &cobra.Command{
		Run:   mountRun,
		Use:   "mount [<options>] [--] <repo> [<dir>]",
		Short: "Mount the provided repository to the local directory",
	}
	mountCmd.Flags().Bool("progress", false, "force progress reporting")
	mountCmd.Flags().BoolP("no-checkout", "n", false, "don't clone shallow repository")
	mountCmd.Flags().Bool("bare", false, "create a bare repository")
	mountCmd.Flags().Bool("mirror", false, "create a mirror repository (implies bare)")
	mountCmd.Flags().BoolP("shared", "s", false, "setup as shared repository")
	mountCmd.Flags().Bool("recurse-submodules", false, "initialize submodules in the clone")
	mountCmd.Flags().Bool("recurse", false, "alias of --recurse-submodules")
	mountCmd.Flags().StringP("origin", "o", "", "use <name> instead of 'origin' to track upstream")
	mountCmd.Flags().StringP("branch", "b", "", "checkout <branch> instead of the remote's HEAD")
	mountCmd.Flags().Int("depth", 0, "create a shallow clone of that depth")
	mountCmd.Flags().Bool("single-branch", false, "clone only one branch, HEAD or --branch")
	mountCmd.Flags().Bool("no-tags", false, "don't clone any tags, and make later fetches not to follow them")
	mountCmd.Flags().Bool("shallow-submodules", false, "any cloned submodules will be shallow")
	entryCmd.AddCommand(mountCmd)
	if err := entryCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func mountRun(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		log.Fatal("You must specify a repository to clone")
	}

	dst := strings.TrimSuffix(filepath.Base(args[0]), ".git")
	dir, err := filepath.Abs(".")
	if len(args) >= 2 {
		dst = filepath.Base(args[1])
		dir, err = filepath.Abs(filepath.Dir(args[1]))
	}
	if err != nil {
		log.Fatal(err)
	}
	hid := filepath.Join(dir, "."+dst)
	mnt := filepath.Join(dir, dst)

	info, err := os.Stat(hid)
	if os.IsNotExist(err) {
		err = nil
	}
	if err != nil {
		log.Fatal(err)
	}
	if info == nil {
		git := exec.Command("git", "clone")
		cmd.Flags().Visit(func(flg *pflag.Flag) {
			switch flg.Name {
			case "origin", "branch", "depth":
				git.Args = append(git.Args, fmt.Sprintf("--%s=%s", flg.Name, flg.Value.String()))
			default:
				git.Args = append(git.Args, fmt.Sprintf("--%s", flg.Name))
			}
		})
		git.Args = append(git.Args, "--", args[0], hid)
		git.Stdout = os.Stdout
		git.Stderr = os.Stderr
		git.Env = os.Environ()
		git.Env = append(git.Env, "GIT_LFS_SKIP_SMUDGE=1")
		if err := git.Run(); err != nil {
			log.Fatal(err)
		}
		lfo := lfs.FilterOptions{
			GitConfig:  config.NewIn(hid, "").GitConfig(),
			Force:      true,
			Local:      true,
			SkipSmudge: true,
		}
		if err := lfo.Install(); err != nil {
			log.Fatal(err)
		}
	} else if !info.IsDir() {
		log.Fatalf("%s is not a directory", hid)
	}

	pxy, err := gitlfsfuse.NewGitLFSFuseRoot(hid)
	if err != nil {
		log.Fatal(err)
	}
	svc, err := fs.Mount(mnt, pxy, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: dst,
			Name:   dst,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s is mounted at %s.\nPlease keep this process running.", args[0], mnt)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		for err := os.ErrInvalid; err != nil; err = svc.Unmount() {
			<-sig
		}
	}()
	svc.Wait()
}

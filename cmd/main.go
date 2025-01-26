package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func main() {
	log.SetFlags(0)

	var rootCmd = &cobra.Command{
		Use: "git-lfs-fuse",
	}

	var mountCmd = &cobra.Command{
		Use:   "mount [<options>] [--] <repo> [<dir>]",
		Short: "Mount the provided repository to the local directory",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("you must specify a repository to clone")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			dest := strings.TrimSuffix(filepath.Base(args[0]), ".git")
			base, err := filepath.Abs(".")
			if len(args) >= 2 {
				dest = filepath.Base(args[1])
				base, err = filepath.Abs(filepath.Dir(args[1]))
			}
			if err != nil {
				log.Fatal(err)
			}

			root := filepath.Join(base, "."+dest)

			command := exec.Command("git", "clone")
			cmd.Flags().Visit(func(flag *pflag.Flag) {
				switch flag.Name {
				case "origin", "branch":
					command.Args = append(command.Args, fmt.Sprintf("--%s=%s", flag.Name, flag.Value))
				default:
					command.Args = append(command.Args, fmt.Sprintf("--%s", flag.Name))
				}
			})
			command.Args = append(command.Args, "--", args[0], root)
			command.Stdout = os.Stdout
			command.Stderr = os.Stderr
			command.Env = os.Environ()
			command.Env = append(command.Env, "GIT_LFS_SKIP_SMUDGE=1")
			if err := command.Run(); err != nil {
				log.Fatal(err)
			}

			// TODO disable git lfs checkout hook

			loopbackRoot, err := fs.NewLoopbackRoot(root)
			if err != nil {
				log.Fatal(err)
			}

			opts := &fs.Options{
				MountOptions: fuse.MountOptions{
					FsName: dest,
					Name:   dest,
				},
			}

			mnt := filepath.Join(base, dest)

			server, err := fs.Mount(mnt, loopbackRoot, opts)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("%s is mounted at %s", args[0], mnt)

			c := make(chan os.Signal)
			signal.Notify(c, os.Interrupt, syscall.SIGTERM)
			go func() {
				for {
					<-c
					if err := server.Unmount(); err != nil {
						log.Println(err)
						continue
					}
					return
				}
			}()

			server.Wait()
		},
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

	rootCmd.AddCommand(mountCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

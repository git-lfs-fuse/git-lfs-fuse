package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"

	"github.com/common-nighthawk/go-figure"
	gitlfsfuse "github.com/git-lfs-fuse/git-lfs-fuse"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var directMount bool

var entryCmd = &cobra.Command{
	Use: "git-lfs-fuse",
}

var mountCmd = &cobra.Command{
	RunE:  mountRun,
	Use:   "mount <repo> [<dir>]",
	Short: "Mount the provided remote repository locally",
}

func init() {
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
	mountCmd.Flags().Bool("no-mount", false, "")
	mountCmd.Flags().Bool("debug", false, "")
	mountCmd.Flags().BoolVar(&directMount, "direct-mount", false, "try to call the mount syscall instead of executing fusermount")
	mountCmd.Flags().Int("max-pages", 5120, "maximum cached pages. 2MiB per page.")
	entryCmd.AddCommand(mountCmd)
}

func mountRun(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		return errors.New("you must specify a repository to clone")
	}
	var mountPoint string
	if len(args) >= 2 {
		mountPoint = args[1]
	}
	var gitOptions []string
	var maxPages int64
	var exit bool
	cmd.Flags().Visit(func(flg *pflag.Flag) {
		switch flg.Name {
		case "origin", "branch", "depth":
			gitOptions = append(gitOptions, fmt.Sprintf("--%s=%s", flg.Name, flg.Value.String()))
		case "max-pages": // this is already guaranteed to be no error.
			maxPages, _ = strconv.ParseInt(flg.Value.String(), 10, 64)
		case "no-mount":
			exit = true
		case "debug":
			gitlfsfuse.RecordNodeOperations = true
		default:
			gitOptions = append(gitOptions, fmt.Sprintf("--%s", flg.Name))
		}
	})
	if maxPages <= 0 {
		maxPages = 5120
	}
	_, mnt, svc, err := gitlfsfuse.CloneMount(args[0], mountPoint, directMount, gitOptions, maxPages)
	if err == nil {
		myFigure := figure.NewFigure("Git LFS Fuse", "", true)
		myFigure.Print()
		log.Printf("Your repository is ready and mounted at %s\nPlease keep this process running.", mnt)

		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		if exit {
			go func() { sig <- os.Interrupt }()
		}
		<-sig
		svc.Close()
	}
	return err
}

func main() {
	log.SetFlags(0)
	if err := entryCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

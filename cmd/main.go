package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"

	gitlfsfuse "github.com/git-lfs-fuse/git-lfs-fuse"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var directMount bool

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
	mountCmd.Flags().BoolVar(&directMount, "direct-mount", false, "try to call the mount syscall instead of executing fusermount")
	mountCmd.Flags().Int("max-pages", 5120, "maximum cached pages")
	entryCmd.AddCommand(mountCmd)
	if err := entryCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func mountRun(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		log.Fatal("You must specify a repository to clone")
	}
	var mountPoint string
	if len(args) >= 2 {
		mountPoint = args[1]
	}
	var gitOptions []string
	var maxPages int64
	var err error
	cmd.Flags().Visit(func(flg *pflag.Flag) {
		switch flg.Name {
		case "origin", "branch", "depth":
			gitOptions = append(gitOptions, fmt.Sprintf("--%s=%s", flg.Name, flg.Value.String()))
		case "max-pages":
			maxPages, err = strconv.ParseInt(flg.Value.String(), 10, 64)
		default:
			gitOptions = append(gitOptions, fmt.Sprintf("--%s", flg.Name))
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	if maxPages <= 0 {
		maxPages = 5120
	}
	_, mnt, svc, err := gitlfsfuse.CloneMount(args[0], mountPoint, directMount, gitOptions, maxPages)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s is mounted at %s\nPlease keep this process running.", args[0], mnt)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	svc.Close()
}

# git-lfs-fuse

Mount remote repositories and datasets managed by Git LFS locally.

## Features

- **Faster Clone and Checkout**: Git LFS tracked files are downloaded by pages (2MiB per page) on demand.
- **Work with Limited Storage**: Pages are cached locally and limited by the `max-pages` config (default: 5120).

## Getting Started

### Installation

Download prebuilt binaries from https://github.com/git-lfs-fuse/git-lfs-fuse/releases.

### Mount your repository or dataset

```bash
# For example, to mount a huggingface dataset:
git-lfs-fuse mount https://huggingface.co/datasets/nvidia/OpenCodeReasoning
```

### Clean up and unmount

If a user-space program crashes during a FUSE operation, or if `git-lfs-fuse` encounters an error, the FUSE module may get stuck in kernel space, preventing clean shutdown.
In such cases, you may need to manually unmount the FUSE mount point:
```sh
# Linux.
sudo fusermount3 -u <mount-dir> 
```

## Requirements

- FUSE support on your operating system (Linux, Windows WSL 2, macOS with [macFUSE](https://macfuse.github.io/) installed).
- Git LFS installed and configured.

## Roadmap

* NFS v3 or v4.
* Git submodule.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.


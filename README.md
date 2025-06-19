# git-lfs-fuse

[![codecov](https://codecov.io/gh/git-lfs-fuse/git-lfs-fuse/graph/badge.svg?token=9EXOUDNEFC)](https://codecov.io/gh/git-lfs-fuse/git-lfs-fuse)


[繁體中文](README.zh-TW.md) | [簡體中文](README.zh-CN.md)

Mount remote repositories, models and datasets managed by Git LFS locally.

## Features

- **Faster Clone and Checkout**: Git LFS tracked files are downloaded by pages (2MiB per page) on demand.
- **Work with Limited Storage**: Pages are cached locally and limited by the `max-pages` config (default: 5120).

## Getting Started

### Requirements

- FUSE support on your operating system (Linux, Windows WSL 2, macOS with [macFUSE](https://macfuse.github.io/) installed).
- Git LFS installed and configured.

### Installation

Download prebuilt binaries from the [release page](https://github.com/git-lfs-fuse/git-lfs-fuse/releases).

### Mount your repository, model, or dataset

```bash
# For example, to mount a huggingface dataset:
git-lfs-fuse mount https://huggingface.co/datasets/nvidia/OpenCodeReasoning --max-pages 5120
```

### Clean up and unmount

FUSE module may get stuck in kernel space if `git-lfs-fuse` doesn't exit gracefully. In such cases, you may need to manually unmount the FUSE mount point:

```sh
# Linux.
sudo fusermount3 -u <mount-dir>
# macOS.
sudo diskutil unmount <mount-dir>
```

## Roadmap

- NFS v3 or v4.
- Git submodule.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

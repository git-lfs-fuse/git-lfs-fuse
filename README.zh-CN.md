# git-lfs-fuse

[![codecov](https://codecov.io/gh/git-lfs-fuse/git-lfs-fuse/graph/badge.svg?token=9EXOUDNEFC)](https://codecov.io/gh/git-lfs-fuse/git-lfs-fuse)

[繁體中文](README.zh-TW.md) | [English](README.md)

将由 Git LFS 管理的远程仓库、模型和数据集挂载到本地。

## 功能特性

- **更快的克隆与签出**：Git LFS 跟踪的文件会以分页（每页 2MiB）按需下载。
- **适用于有限存储空间**：分页会缓存在本地，并受 `max-pages` 配置（默认：5120）限制。

## 快速开始

### 系统要求

- 操作系统需支持 FUSE（Linux、Windows WSL 2、安装了 [macFUSE](https://macfuse.github.io/) 的 macOS）。
- 已安装并配置 Git LFS。

### 安装

请从 [release page](https://github.com/git-lfs-fuse/git-lfs-fuse/releases) 下载预编译二进制文件。

### 挂载您的仓库、模型或数据集

```bash
# 例如，挂载 huggingface 数据集：
git-lfs-fuse mount https://huggingface.co/datasets/nvidia/OpenCodeReasoning --max-pages 5120
```

### 清理与卸载

如果 `git-lfs-fuse` 未正常退出，FUSE 模块可能会卡在内核空间。此时，您可能需要手动卸载 FUSE 挂载点：

```sh
# Linux.
sudo fusermount3 -u <mount-dir>
# macOS.
sudo diskutil unmount <mount-dir>
```

## 路线图

- NFS v3 或 v4。
- Git 子模块。

## 贡献

欢迎贡献！请随时提交 Pull Request。

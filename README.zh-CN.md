# git-lfs-fuse

[English](README.md) | [繁體中文](README.zh-TW.md)

将由 Git LFS 管理的远程仓库与数据集挂载到本地。

## 功能特性

- **更快的克隆与签出**：Git LFS 跟踪的文件会以分页（每页 2MiB）按需下载。
- **适用于有限存储空间**：分页会缓存在本地，并受 `max-pages` 配置（默认：5120）限制。

## 快速开始

### 安装

请从 [release page](https://github.com/git-lfs-fuse/git-lfs-fuse/releases) 下载预编译二进制文件。

### 挂载您的仓库或数据集

```bash
# 例如，挂载 huggingface 数据集：
git-lfs-fuse mount https://huggingface.co/datasets/nvidia/OpenCodeReasoning
```

### 清理与卸载

如果用户空间程序在 FUSE 操作期间崩溃，或 `git-lfs-fuse` 发生错误，FUSE 模块可能会卡在内核空间，导致无法正常关闭。
此时，您可能需要手动卸载 FUSE 挂载点：

```sh
# Linux.
sudo fusermount3 -u <mount-dir>
```

## 系统要求

- 操作系统需支持 FUSE（Linux、Windows WSL 2、安装了 [macFUSE](https://macfuse.github.io/) 的 macOS）。
- 已安装并配置 Git LFS。

## 路线图

- NFS v3 或 v4。
- Git 子模块。

## 贡献

欢迎贡献！请随时提交 Pull Request。

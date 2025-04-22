# git-lfs-fuse

[English](README.md) | [簡體中文](README.zh-CN.md)

將由 Git LFS 管理的遠端儲存庫與資料集掛載於本地端。

## 功能特色

- **更快的克隆與簽出**：Git LFS 追蹤的檔案會以分頁（每頁 2MiB）按需下載。
- **適用於有限儲存空間**：分頁會快取於本地端，並受 `max-pages` 設定（預設：5120）限制。

## 快速開始

### 安裝

請從 [release page](https://github.com/git-lfs-fuse/git-lfs-fuse/releases) 下載預建二進位檔。

### 掛載您的儲存庫或資料集

```bash
# 例如，掛載 huggingface 資料集：
git-lfs-fuse mount https://huggingface.co/datasets/nvidia/OpenCodeReasoning
```

### 清理與卸載

如果使用者空間程式在 FUSE 操作期間崩潰，或 `git-lfs-fuse` 發生錯誤，FUSE 模組可能會卡在核心空間，導致無法正常關閉。
此時，您可能需要手動卸載 FUSE 掛載點：

```sh
# Linux.
sudo fusermount3 -u <mount-dir>
```

## 系統需求

- 作業系統需支援 FUSE（Linux、Windows WSL 2、安裝 [macFUSE](https://macfuse.github.io/) 的 macOS）。
- 已安裝並設定 Git LFS。

## 發展藍圖

- NFS v3 或 v4。
- Git 子模組。

## 貢獻

歡迎貢獻！請隨時提交 Pull Request。

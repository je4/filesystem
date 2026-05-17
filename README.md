> \[!NOTE]
> ****filesystem has been moved to [https://github.com/ocfl-archive/filesystem](https://github.com/ocfl-archive/filesystem).****

# Filesystem Library

This Go library provides advanced filesystem features not available in the standard library's `io/fs`. It introduces write support via `writefs` and a configurable virtual filesystem layer via `vfsrw`.

## Core Features

- **Write Support (`writefs`)**: Extends `io/fs` with standard interfaces for write operations like `Create`, `Append`, `MkDir`, `Rename`, `Remove`, and `WriteFile`.
- **Virtual File System (`vfsrw`)**: A unified layer that maps multiple backend filesystems into a single hierarchy using a `vfs:/<backend>/<path>` scheme.

## Available Filesystems

This library includes various storage backends and wrappers:

- **Local Storage**: [osfsrw](./pkg/osfsrw/README.md)
- **Object Storage**: [s3fsrw](./pkg/s3fsrw/README.md) (Amazon S3, MinIO)
- **Remote Protocols**: [sftpfsrw](./pkg/sftpfsrw/README.md), [remotefs](./pkg/remotefs/README.md), [webFS](./pkg/webFS/README.md)
- **Archives**: [zipfsrw](./pkg/zipfsrw/README.md), [zipasfolder](./pkg/zipasfolder/README.md)
- **Specialized**: [miniKVStoreFSRW](./pkg/miniKVStoreFSRW/README.md), [mountFS](./pkg/mountFS/README.md)

## Getting Started

### Installation

```bash
go get github.com/je4/filesystem/v3
```

### Basic Usage with `vfsrw`

```go
import (
	"github.com/je4/filesystem/v3/pkg/vfsrw"
	"github.com/je4/utils/v2/pkg/zLogger"
)

// Define backend configuration
cfg := vfsrw.Config{
	"data": &vfsrw.VFS{
		Type: "os",
		OS: &vfsrw.OS{BaseDir: "/var/lib/mydata"},
	},
}

// Initialize VFS
vfs, _ := vfsrw.NewFS(cfg, logger)

// Open a file with the vfs:/ prefix
f, _ := vfs.Open("vfs:/data/logs/app.log")
```

For more details on each package, see the [Packages Overview](./pkg/README.md).

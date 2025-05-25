# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Building
- `make` - Build all commands in the cmd/ directory
- `make <command>` - Build a specific command (e.g., `make dirfs`, `make ccat`)
- `make clean` - Remove all built binaries from bin/

### Testing
- `make test` - Run all tests with `go test ./...`

### Dependencies
- `make deps` - Install additional dependencies (currently b2client)

### Cross-compilation
- `make rpi-einkfs` - Cross-compile einkfs for Raspberry Pi (ARM6)

## Architecture Overview

CFS (Cloud File System) is a 9P protocol-based file system abstraction that provides a unified interface for operating on files in the cloud. The project implements the Plan 9 file system protocol (9P2000) to expose various backends as file servers.

### Core Components

**ninep package** (`ninep/`) - Core 9P protocol implementation
- `server.go` - 9P server implementation with connection handling, session management, and message dispatching
- `fs.go` - FileSystem interface and file handle abstractions
- `client.go` - 9P client implementation for connecting to servers
- `messages.go` - 9P protocol message parsing and serialization

**File System Servers** (`cmd/` and `fs/`) - Various backends exposed as file servers
- **dirfs** - Serves a local directory via 9P
- **memfs** - In-memory file system
- **s3fs** - Amazon S3 backend
- **dockerfs** - Docker daemon interface
- **sftpfs** - SFTP connection backend
- **procfs** - Local process information
- **envfs** - Environment variables as files
- **rssfs** - RSS feeds as file system

**Client Tools** (`cmd/`) - Command-line clients for interacting with 9P servers
- **ccat**, **ccp**, **cls**, **crm**, etc. - Standard Unix-like operations over 9P

**CLI Framework** (`cli/`) - Common server setup and configuration
- `server_main.go` - Standardized server startup with logging, TLS, profiling

### Key Architectural Patterns

1. **9P Protocol Abstraction** - All file systems implement the `ninep.FileSystem` interface, allowing any backend to be exposed via 9P
2. **Session Management** - Server tracks client sessions, file handles (fids), and qids for stateful 9P operations
3. **Modular Backends** - Each file system type is a separate package with its own implementation
4. **Client/Server Architecture** - Clear separation between 9P servers and client tools

### File System Interface

The core `ninep.FileSystem` interface requires:
- `MakeDir`, `CreateFile`, `OpenFile` - File operations
- `ListDir`, `Stat`, `WriteStat` - Metadata operations  
- `Delete` - Removal operations

File handles implement `ninep.FileHandle` with `ReadAt`, `WriteAt`, `Sync`, `Close` methods.

### Development Notes

- Go 1.23+ required
- Uses structured logging via `slog`
- Built-in support for TLS and profiling
- Follows Plan 9 conventions for file permissions and user/group handling
- Each server binary accepts standard flags: `-addr`, `-srv-log`, `-help`, etc.
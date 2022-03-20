# CFS

Cloud File System.

A cloud-abstraction for a file system. Provides a unified way of operating on
files in the cloud.

All programs talk to each over using the 9p protocol. The goal is the provide
cloud infrastructure in a form that just looks like a collection of file
servers.

# Note to the User

This project is an exploration of plan9 / 9p protocol. There are probably lots
of things to fix.

While having a file system as the underlying protocol can allow ultimate
flexibility, it introduces distributed system problems as part of its interface.
In short, a file system is sharing global mutable state across multiple clients.

9P doesn't have a wholelistic solution for these problems, and which surfaces as
unclear implementation details for each file server:

 - What does it mean if multiple clients attempt to modify the same file?
 - What if the underlying store doesn't support locks?
 - How does one support locks with flaky clients?
 - How do you support features if another client access the underlying store
   that the file server is utilizing?

A better approach would probably be to simplify the protocol to an object store
where:

 - Writers are all-or-nothing and atomic.
 - Last writer wins semantics
 - Locking has expiry and prevents other writers from accessing an object
 - (Can) separate the notion of file hierarchy from a key-value store
 - Moves away from disk-specific details (eg - block sizes, etc.)
 - Requires less state on protocol (which allows better scalability)

Directory management can either be built on-top of a key-value / object store
and can scale separately.

# Building

Just run `make`. Each binary has its own help. See `dirfs` or `memfs` for the
simplest file server. Look at the list of clients below for interaction with a
file server.

## Servers

 - **b2fs** is a file server for Backblaze's b2 service
 - **dirfs** is a file server that serves a local file directory
 - **tmpfs** is a file server that serves a temporary directory
 - **s3fs** is a file server for Amazon's S3 service (or any compatible variant)
 - **dockerfs** is a file server that exposes the docker daemon (aka - docker cli)
 - **memfs** is a file server that serves from memory
 - **procfs** is a file server that serves local process information
 - **sftpfs** is a file server that serves a sftp connection's set of files

## Clients

 - **ccat** a client that reads from a file (akin to *cat*)
 - **ccp** a client that copies a file (akin to *cp*)
 - **cexe** a client that executes remote commands via ssh (akin to *cpu*)
 - **cfind** a client that searches for files in a file server (akin to *find*)
 - **cls** a client that lists files and directories a given location (akin to *ls*)
 - **cmkdir** a client that creates to a directory (akin to *mkdir*)
 - **cmv** a client that moves a file or directory (akin to *mv*)
 - **cpipe** a client that writes to a file from contents of stdin (similiar to bash's '>' operator)
 - **crm** a client that removes a file or directory (akin to *rm*)
 - **ctee** a client that writes to a file (akin to *tee*)
 - **ctouch** a client that creates to a file (akin to *touch*)

## Proxies

 - **proxyfs** is a file server that proxies to another 9p server
 - **unionfs** is a file server that provides a "unioned" file system based on
   a collection of 9p file servers.
 - **exportfs** is a client that hosts a FUSE file system that proxies to a 9p server
     - Basically, allow a unix-based OS to directly manipulate a 9p file server
 - **encryptfs** is a file proxy that encrypts files to the underlying file system.

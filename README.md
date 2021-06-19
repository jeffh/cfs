# CFS

Cloud File System.

A cloud-abstraction for a file system. Provides a unified way of operating on
files in the cloud.

All programs talk to each over using the 9p protocol. The goal is the provide
cloud infrastructure in a form that just looks like a collection of file
servers.

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
 - **cls** a client that lists files and directories a given location (akin to *ls*)
 - **cmkdir** a client that creates to a directory (akin to *mkdir*)
 - **cmv** a client that moves a file or directory (akin to *mv*)
 - **crm** a client that removes a file or directory (akin to *rm*)
 - **ctee** a client that writes to a file (akin to *tee*)
 - **ctouch** a client that creates to a file (akin to *touch*)
 - **cexe** a client that executes remote commands via ssh (akin to *cpu*)
 - **cpipe** a client that writes to a file from contents of stdin (similiar to bash's '>' operator)

## Proxies

 - **proxyfs** is a file server that proxies to another 9p server
 - **unionfs** is a file server that provides a "unioned" file system based on
   a collection of 9p file servers.
 - **exportfs** is a client that hosts a FUSE file system that proxies to a 9p server
     - Basically, allow a unix-based OS to directly manipulate a 9p file server
 - **encryptfs** is a file proxy that encrypts files to the underlying file system.

# CFS

Cloud File System.

A cloud-abstraction for a file system. Provides a unified way of operating on
files in the cloud.

All programs talk to each over using the [9p protocol][9p2000]. The goal is the provide
cloud infrastructure in a form that just looks like a collection of file
servers.

[9p2000]: https://man.cat-v.org/plan_9/5/

# Note to the User

This project is an exploration of plan9 / 9p protocol. There are probably lots
of things to fix. API stability is not guaranteed until the proper abstractions
are made.

While having a file system as the underlying protocol can allow ultimate
flexibility, it introduces distributed system problems as part of its interface.
In short, a file system is sharing global mutable state across multiple clients
(readers and writers).

9P doesn't have a wholelistic solution for these problems, and which surfaces as
unclear implementation details for each file server:

- What does it mean if multiple clients attempt to modify the same file?
- What if the underlying store doesn't support locks?
- How does one support locks with flaky clients?
- How do you support features if another client access the underlying store that
  the file server is utilizing?

Plan 9 addressed these issues by having the Kernel proxy requests to servers.
This allowed a centralized place to manage concurrent access and retries - akin to
Plan 9 Port's 9pserve.

A better approach would probably be to simplify the protocol to an object store
where:

- Writers are all-or-nothing and atomic.
- Last writer wins semantics
- Locking has expiry and prevents other writers from accessing an object
- (Can) separate the notion of file hierarchy from a key-value store
- Moves away from disk-specific details (eg - block sizes, etc.)
- Requires less state on protocol (which allows better scalability)

The original object store (AWS S3), did make the cardinal sin of making keys look
directory-like but not actual be directories.

Directory management can either be built on-top of a key-value / object store
and can scale separately. But that is out of the scope of this project. This is
partly why object stores are so popular - it is a simplification that tries to
remove as much file/block based complexity.

As probably many people have learned, 9p is not equivalent to linux file system
behaviors, so bridging between them is not a one-to-one mapping.

[upspin]: https://upspin.io/

# Building

Just run `make`. Each binary has its own help. See `dirfs` or `memfs` for the
simplest file server. Look at the list of clients below for interaction with a
file server.

## Servers

- **dirfs** is a file server that serves a local file directory
- **envfs** is a file server that serves the process' environment variables
- **tmpfs** is a file server that serves a temporary directory
- **s3fs** is a file server for Amazon's S3 service (or any compatible variant)
- **dockerfs** is a file server that exposes the docker daemon (aka - docker
  cli)
- **memfs** is a file server that serves from memory
- **procfs** is a file server that serves local process information
- **sftpfs** is a file server that serves a sftp connection's set of files
- **rssfs** is a file server taht serves contents of an rss feed

## Clients

- **ccat** a client that reads from a file (akin to _cat_)
- **ccp** a client that copies a file (akin to _cp_)
- **cexe** a client that executes remote commands via ssh (akin to _cpu_)
- **cfind** a client that searches for files in a file server (akin to _find_)
- **cls** a client that lists files and directories a given location (akin to
  _ls_)
- **cmkdir** a client that creates to a directory (akin to _mkdir_)
- **cmv** a client that moves a file or directory (akin to _mv_)
- **cpipe** a client that writes to a file from contents of stdin (similiar to
  bash's '>' operator)
- **crm** a client that removes a file or directory (akin to _rm_)
- **ctee** a client that writes to a file (akin to _tee_)
- **ctouch** a client that creates to a file (akin to _touch_)

## Proxies

- **proxyfs** is a file server that proxies to another 9p server
- **unionfs** is a file server that provides a "unioned" file system based on a
  collection of 9p file servers.
- **exportfs** is a client that hosts a FUSE file system that proxies to a 9p
  server
  - Basically, allow a unix-based OS to directly manipulate a 9p file server
- **encryptfs** is a file proxy that encrypts files to the underlying file
  system.

# Protocol Limitations

 - Notification of file changes. Polling is the only way. (Jokingly,) you could
   provide this as a file!
 - Errors are just text strings with no structure. This makes it harder for
   programs to know how to handle special errors. In *cfs*' case, errors are
   mapped back to `fs.Err*` or `os.Err*` errors when possible.
 - Unix modes feel like weird specifics to file systems as part of the
   protocol. Permissions are bespoke to the file server.

## Musing on Improvements

Perhaps a better approach would be:

Client <-> Multiplexer <-> Object Store

The Multiplexer would be responsible for managing locks, retries, and other
coordination mechanisms. This is similar to [upspin][upspin]'s approach, a
spiritual successor to 9p in some ways. Upspin basically has:

`Client` -> `Upspin Server` -> `Object Store`

Here's the protocol for the server in upspin:

```go
// The "Upspin Server" interface
type StoreServer interface {
    Dial(Config, Endpoint) (Service, error)
    // Endpoint returns the network endpoint of the server.
    Endpoint() Endpoint
    // Close closes the connection to the service and releases all resources used.
    // A Service may not be re-used after close.
    Close()


    // Get attempts to retrieve the data identified by the reference.
    // Three things might happen:
    // 1. The data is in this StoreServer. It is returned. The Location slice
    // and error are nil. Refdata contains information about the data.
    // 2. The data is not in this StoreServer, but may be in one or more
    // other locations known to the store. The slice of Locations
    // is returned. The data, Refdata, Locations, and error are nil.
    // 3. An error occurs. The data, Locations and Refdata are nil
    // and the error describes the problem.
    Get(ref Reference) ([]byte, *Refdata, []Location, error)

    // Put puts the data into the store and returns the reference
    // to be used to retrieve it.
    Put(data []byte) (*Refdata, error)

    // Delete permanently removes all storage space associated
    // with the reference. After a successful Delete, calls to Get with the
    // same reference will fail. If the reference is not found, an error is
    // returned. Implementations may disable this method except for
    // privileged users.
    Delete(ref Reference) error
}
```

```go
// The "Object Store" interfaces
type Storage interface {
    // LinkBase returns the base URL from which any ref may be downloaded.
    // If the backend does not offer direct links it returns
    // upspin.ErrNotSupported.
    LinkBase() (base string, err error)

    // Download retrieves the bytes associated with a ref.
    Download(ref string) ([]byte, error)

    // Put stores the contents given as ref on the storage backend.
    Put(ref string, contents []byte) error

    // Delete permanently removes all storage space associated
    // with a ref.
    Delete(ref string) error

    // (optional)
    // List returns a list of references contained by the storage backend.
    // The token argument is for pagination: it specifies a starting point
    // for the list. To obtain a complete list of references, pass an empty
    // string for the first call, and the last nextToken value for for each
    // subsequent call. The pagination tokens are opaque values particular
    // to the storage implementation.
    List(token string) (refs []upspin.ListRefsItem, nextToken string, err error)
}
```

This interface does have a limitation of large files having to fit in memory
unless some block-based structure is done. Using a block-structure (which
upspin does do as I understand), would restrict what storage systems can do.

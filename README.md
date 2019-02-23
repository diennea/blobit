# Blobit

Blobit is a ditributed binary large objects (BLOBs) storage built upon Apache BookKeeper

# Overview

Blobit stores binary objects in buckets, a *bucket* is like a namespace, and this is
fundamental to the design of the whole system.
Blobit has been designed with multitenancy in mind and it is expected that each
tenant uses its own bucket.

Data is stored on a [Apache BookKeeper](https://bookkeeper.apache.org) cluster, 
and this automagically enables BlobIt to scale horizontally, the more *Bookies* you have
the more amount of data you will be able to store.

BlobIt needs a *metadata service* in order to store refecences to the data, it ships
by default with [HerdDB](https://herddb.org), which is also built upon BookKeeper.

# Architectural overview

BlobIt is designed for performance and expecially low latency in this scenario:
one client stores a blob and other one immediately reads such blob (from another machine).
This is the most common path in [EmailSuccess](https://emailsuccess.com), as BlobIt
is the core data store for it.
Blobs are supposed to be retained for a couple of weeks.

Blobit Clients talk directly to Bookies both for reads and for writes, this way
we are exploiting directly all of the BookKeeper optimiziations in the write and read path.
This architecture is totally decentralized, there is no real BlobIt server.

You can see BlobIt as an extension to BookKeeper, with a metadata layer which makes it simple to:
- reference Data using a user-supplied name (in form of bucketId/name)
- organize efficently data in BookKeeper, an allow deletion of blobs.


## Writes

Batches of Blobs are stored in BookKeeper ledgers (using the WriteHandleAdv API),
one ledger will contain more then
one blob, and BlobIt will collect unused ledger and delete them.

When a writer stores a blob it receives immedialy an unique id of the blob,
this is is unique in the whole cluster, and it is not a key insidee the bucket.
Such id is a "smart id" and it contains all of the information needed to retrieve
data without using the metadata service.
Such id contains information like:
- the id of the ledger
- fist entry id
- number of entries
- size of the entry

With such information it is possible to read the whole blob or even only parts.
An object is immutable and it cannot be modified.

While writing the client can assign a custom name, unique inside the context of the bucket,
in order to be able to retrieve the object using an application supplied key.
Using custom ids needs a lookup on the metadata service, and this lookup needs an additionl RPC.

# Reads

Blobit client reads data directly from Bookies. Because the objectId
contains all of the information to access the data.
In case of lookup by custom id a lookup into the metadata service is needed.

The read can read the full Blob of parts of it. BlobIT supports a Streaming API
for reads, suitable for very large objects: as soon as data comes from BookKeeper
it is streamed to the application (Think about a Servlet which retrives an object
and serves it directly to the client).

# Buckets and data locality


## Metadata service

Metadata

## License

Blobit is under [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).

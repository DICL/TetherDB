## TetherDB

### About MANIFEST
TetherDB manages the MANIFEST and LSM metadata in exactly the same way as RocksDB.
For simplicity, we refer to both as "MANIFEST" throughout the paper.
The main thread is responsible for reading the MANIFEST (or LSM metadata), selecting victim SSTables, and appending metadata for newly generated SSTables—just like in RocksDB. 

Please refer to the `DBImpl::BackgroundCompaction` function in the `db/db_impl/db_impl_compaction_flush.cc` file of the **Initiator code** (lines 3151–3210).
The compaction victim is selected while holding the global mutex, which is then released. After the compaction is completed, the mutex is reacquired before committing the result.

### About Filesystem Metadata
As described in the third paragraph of Section 4, the NDP Engine does not directly access the metadata; it only performs compaction tasks based on file extent mapping information of victim SSTables and output SSTables provided by the Task Delegator.
Synchronization of the metadata is outside the scope of the storage node’s responsibilities. 

Please refer to `env/io_spdk.h` in the **Initiator code** and `env/spdk_env.h` in the **Target code**.
The Initiator manages file metadata by calling `WriteMeta` whenever there is a metadata change.
In the Target, the `WriteMeta` call is disabled.
Metadata of new files created by the Target is ultimately reflected via `ioptions->spdk_fs->AddInMeta()` at line 457 of `grpc/grpc_lemma.cc` in the **Initiator code**.

### About NDP Cache
The NDP Engine keeps the extent mappings of the victim and new SSTable files upon completing compaction.
Using those file metadata, it can delete and insert cache entries using block addresses without scanning the entire cache.

Please refer to the `SPDKFileMeta` struct in `env/spdk_env.h` of the **Target code** (line 423).
The NDP Engine uses this structure to manage file extent mappings received from the Task Delegator.
An instance of `SPDKFileMeta` is created upon receiving a compaction request and remains valid until the compaction is completed, enabling file access and cache management.

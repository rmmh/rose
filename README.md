== Rose ==

Rose, the Reduced Object Storage Engine, is a simple personal-scale distributed object store appropriate for storing terabytes to petabytes of data. It supports strong durability guarantees, snapshots, compression, deduplication, snapshots, arbitrary pools of drives, multiple protection schemes, availability despite drive ~~or node~~ loss, and fast scrubbing, rebalancing, and reprotecting.

It supports access through FUSE, its own API and tooling, or ~~an S3 compatible API~~. As an object store, file modifications are directory movements are slower than a traditional filesystem, but they are supported.

Metadata is stored in a ~~replicated~~ key-value database (currently SQLite), and ideally should be located on an SSD with power loss protection, like the Intel DC series.

The data for each top-level directory or **bucket** is stored in a series of append-only files called **logs** a few GB each, to greatly simplify maintaining durability and consistency across drives. Log files include hashes of every block they contain for bitrot protection, and the metadata database stores the roots of hash trees for each log.

Each bucket has configurable protection schemes, including null, replication, and erasure coding with N data and K parity shards that can withstand K losses. Protection schemes are implemented _on top_ of log files, so even the null protection scheme provides bitrot protection. Protection schemes provide a **virtual log** abstraction on top of the **physical log** files that are actually stored.

Files are split into variable-length chunks around a megabyte in size for deduplication across a bucket and, optionally, compression.

Snapshots are unlimited and read-only, with ~~configurable automatic snapshotting periods~~.

Drives can be added or removed from the system at any time, and triggers automatic data rebalancing when a new drive is added and reprotection when a drive is removed. Blocks in a log can be validated in order, so scrubbing, rebalancing, and reprotecting are all bulk sequential read/write operations to maximize IO throughput.


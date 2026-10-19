# Parquet2

Parquet2 is a rust library to interact with the
[parquet format](https://en.wikipedia.org/wiki/Apache_Parquet), welcome to its guide!

This guide describes on how to efficiently and safely read and write
to and from parquet.
Before starting, there are two concepts to introduce in the context of this guide:

* IO-bound operations: perform either disk reads or network calls (e.g. s3)
* CPU-bound operations: perform compute

In this guide, "read", "write" and "seek" correspond to
IO-bound operations, "decompress", "compress", "deserialize", etc. are CPU-bound.

Generally, IO-bound operations are parallelizable with green threads, while CPU-bound
operations are not. 

## Metadata

The starting point of reading a parquet file is reading its
metadata (at the end of the file).
To do so, we offer two functions for `sync` and `async`:

#### Sync

`parquet2::read::read_metadata` for `sync` reads:

```rust,no_run,noplayground
{{#include ../../examples/read_metadata.rs:metadata}}
```

#### Async

and `parquet2::read::read_metadata_async`, for async reads
(using `tokio::fs` as example):

```rust
{{#include ../../examples/read_metadata_async/src/main.rs}}
```

In both cases, `metadata: FileMetaData` is the file's metadata.

## Columns, Row Groups, Columns chunks and Pages

At this point, it is important to give a small introduction to the format itself.
The metadata does not contain any data. Instead, the metadata contains
the necessary information to read, decompress, decode and deserialize data. Generally:

* a file has a schema with columns and data
* data in the file is divided in _row groups_
* each _row group_ contains _column chunks_
* each _column chunk_ contains _pages_
* each _page_ contains multiple values

each of the entities above has associated metadata. Except for the pages,
all this metadata is already available in the `FileMetaData`.
Here we will focus on a single column to show how we can read it.

We access the metadata of a column chunk via

```rust,no_run,noplayground
{{#include ../../examples/read_metadata.rs:column_metadata}}
```

From this, we can produce an iterator of compressed pages (sync), 
`parquet2::read::get_page_iterator` or a stream (async) of compressed
pages, `parquet2::read::get_page_stream`:

```rust,no_run,noplayground
{{#include ../../examples/read_metadata.rs:pages}}
```

in both cases, they yield individual `CompressedDataPage`s. Note that these
pages do hold values and own potentially large chunks of (compressed) memory.

At this point, we are missing 3 steps: decompress, decode and deserialize.
Decompression is done via `decompress`:

```rust,no_run,noplayground
{{#include ../../examples/read_metadata.rs:decompress}}
```

Decoding and deserialization is usually done in the same step, as follows:

```rust,no_run,noplayground
{{#include ../../examples/read_metadata.rs:deserialize}}
```

the details of the `todo!` are highly specific to the target in-memory format to use.
Thus, here we only describe how to decompose the page buffer in its individual
components and what you need to worry about.
For example, reading to Apache Arrow often does not require decoding the
definition levels, as they have the same representation as in Arrow, but do require
deserialization of some values, as e.g. arrow supports unsigned integers while
parquet only accepts (potentially encoded) `i32` and `i64` integers.
Refer to the integration tests's implementation for deserialization to a 
simple in-memory format, and [arrow2](https://github.com/jorgecarleitao/arrow2)
for an implementation to the Apache Arrow format.

## Row group statistics

The metadata of row groups can contain row group statistics that
can be used to pushdown filter operations.

The statistics are encoded based on the physical type of the column and
are represented via trait objects of the trait `Statistics`,
which can be downcasted via its `Statistics::physical_type()`:

```rust,no_run,noplayground
{{#include ../../examples/read_metadata.rs:statistics}}
```

## Bloom filters

The column metadata may contain bloom filter bitsets that can be used to pushdown
filter operations to row groups.

This crate offers the necessary functionality to check whether an item is not in a column chunk:

```rust,no_run,noplayground
{{#include ../../examples/read_metadata.rs:bloom_filter}}
```

## Column and page indexes

The column metadata may contain column and page indexes that can be used to push down filters
when reading (IO) pages.

This crate offers the necessary functionality to check whether an item is not in a column chunk:

```rust,no_run,noplayground
{{#include ../../examples/read_metadata.rs:column_index}}
```

## Sidecar

When writing multiple parquet files, it is common to have a "sidecar" metadata file containing
the combined metadata of all files, including statistics.

This crate supports this use-case, as shown in the example below:

```rust
{{#include ../../examples/write_sidecar.rs}}
```

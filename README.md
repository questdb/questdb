[![Build Status](https://secure.travis-ci.org/NFSdb/nfsdb.png?branch=master)](http://travis-ci.org/NFSdb/nfsdb)

NFSdb
======

NFSdb is a low latency, high throughput time-series database written entirely in Java. We use memory-mapped files to bring you fully persisted database with performance characteristics of in-memory cache.
The goal of this project is to make the fastest and most developer friendly database in the world.


###Maven

NFSdb requires minimum of Java 7 and stable release is available from maven Central:

```xml
        <dependency>
            <groupId>com.nfsdb</groupId>
            <artifactId>nfsdb-core</artifactId>
            <version>1.0.2</version>
        </dependency>

```

### Getting started

Check out our [Quick-Start Guide] (http://www.nfsdb.org/quick-start/)

We also have a growing collection of examples:
 
[nfsdb-samples] (https://github.com/NFSdb/nfsdb/tree/master/nfsdb-samples) repository.

### Performance

On test rig (Intel i7-920 @ 4Ghz) NFSdb shows average read latency of 20-30ns and write latency of 60ns per column of data. Read and write do not have any GC overhead.

### License

NFSdb is available under [Apache 2.0 License] (http://www.apache.org/licenses/LICENSE-2.0.txt)

### Support

NFSdb project is being actively developed and supported. You can raise and [Issue] (https://github.com/bluestreak01/nfsdb/issues) on github or join our [google group] (https://groups.google.com/forum/#!forum/nfsdb)

Please visit our official web site [www.nfsdb.org] (http://nfsdb.org).

### Upcoming release Change Log

- thrift support is optional. NFSdb works well with POJOs.
- durable transaction support (JournalWriter.commitDurable())
- Journal.select() to chose active columns. This saves a lot of unnecessary disk reading.
- Queries for invalid symbols return empty result set instead of throwing exceptions
- Maven: can assemble .zip file from nfsdb-core.
- 5% performance improvement

NFSdb
======

NFSdb is a low latency, high throughput time-series database written entirely in Java. The goal of the project is a study of what's possible to do in java with large datasets. And we would like to share benefits of this study with everybody.


###Maven

NFSdb requires minimum of Java 7 and stable release is available from maven Central:

```xml
        <dependency>
            <groupId>com.nfsdb</groupId>
            <artifactId>nfsdb-core</artifactId>
            <version>1.0.1</version>
        </dependency>

```

### Getting started

To help you getting started we have created code examples in [nfsdb-samples] (https://github.com/bluestreak01/nfsdb/tree/master/nfsdb-samples) repository.

Alternatively there is quick start guide on github [wiki] (https://github.com/bluestreak01/nfsdb/wiki)

### Performance

On test rig (Intel i7-920 @ 4Ghz) NFSdb shows average read latency of 20-30ns and write latency of 60ns per column of data. Read and write do not have any GC overhead.

### License

NFSdb is available under [Apache 2.0 License] (http://www.apache.org/licenses/LICENSE-2.0.txt)

### Support

NFSdb project is being actively developed and supported. You can raise and [Issue] (https://github.com/bluestreak01/nfsdb/issues) on github or join our [google group] (https://groups.google.com/forum/#!forum/nfsdb)

10 May 2014.

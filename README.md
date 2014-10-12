## Overview

* [What is NFSdb?](#what-is-nfsdb)
* [Why?](#why)
* [How?](#how)
* [Examples](#more-examples)
* [Maven](#maven)
* [Performance](#performance)
* [License](#license)
* [Support](#support) 



[![Build Status](https://secure.travis-ci.org/NFSdb/nfsdb.png?branch=master)](http://travis-ci.org/NFSdb/nfsdb)
[![Dependency Status](https://www.versioneye.com/user/projects/5439d968b2a9c527fe00027c/badge.svg?style=flat)](https://www.versioneye.com/user/projects/5439d968b2a9c527fe00027c)

##What is NFSdb?

NFSdb is a java library that lets you easily persist huge volumes of POJOs on disk with almost zero GC overhead and minimal latency (millions of writes in a second). With NFSdb you can also query these objects and replicate them over the network. Fast. Very, very fast.

---

##Why?

Storing and querying data for Java developer is always pain in the neck. JDBC requires ORM tools, which is always a maintenance nightmare and performance hog. NoSQL databases are better but come with tricky installation and integration procedures and are not maintenance free either. We wanted to create a library that would help us to:

- linear scalability in response to data volume increase.
- throw away boilerplate persistence layer.
- have clean, minimalistic API.
- throw away caching because our database would be fast enough!
- have minimal heap footprint.
- leverage all of the available memory without using it for heap.
- handle time series queries efficiently.
- provide out of box support for temporal data.
- scale processing out to multiple servers

[to top](#overview)

---

##How?

NFSdb provides automatic serialization for primitive types of POJOs to Memory Mapped Files. Files organised on disk in directories per class and files per attributes, providing column-based data store. String values can be indexed for fast searches and if your data has timestamp - it can be partitioned by DAY, MONTH or YEAR. Memory Mapped Files are managed by Operating System, which provides resilience in case of JVM crash and also a way of inter-process communication as data written by one process is immediately available to all other processes.

NFSdb also provides easy to setup data replication over TCP/IP with automatic service discovery via multicast.

This is an example of fully functional data _publisher/server_:

```java
public class SimpleReplicationServerMain {

    private final String location;

    public SimpleReplicationServerMain(String location) {
        this.location = location;
    }

    public static void main(String[] args) throws Exception {
        new SimpleReplicationServerMain(args[0]).start();
    }

    public void start() throws Exception {
        JournalFactory factory = new JournalFactory(location);
        JournalServer server = new JournalServer(factory);

        JournalWriter<Price> writer = factory.writer(Price.class);
        server.publish(writer);

        server.start();

        System.out.print("Publishing: ");
        for (int i = 0; i < 10; i++) {
            publishPrice(writer, 1000000);
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            System.out.print(".");
        }
        System.out.println(" [Done]");
    }

    private void publishPrice(JournalWriter<Price> writer, int count) 
            throws JournalException {
        long tZero = System.currentTimeMillis();
        Price p = new Price();
        for (int i = 0; i < count; i++) {
            p.setTimestamp(tZero + i);
            p.setSym(String.valueOf(i % 20));
            p.setPrice(i * 1.04598 + i);
            writer.append(p);
        }
        // commit triggers network publishing
        writer.commit();
    }
}
```

And this is fully functional _client_ measuring transaction latency:

```java
public class SimpleReplicationClientMain {
    public static void main(String[] args) throws Exception {
        JournalFactory factory = new JournalFactory(args[0]);
        final JournalClient client = new JournalClient(factory);

        final Journal<Price> reader = factory.bulkReader(Price.class, "price-copy");

        client.subscribe(Price.class, null, "price-copy", new TxListener() {
            @Override
            public void onCommit() {
                int count = 0;
                long t = 0;
                for (Price p : reader.incrementBuffered()) {
                    if (count == 0) {
                        t = p.getTimestamp();
                    }
                    count++;
                }
                System.out.println("took: "
                                + (System.currentTimeMillis() - t) 
                                + ", count=" + count);
            }
        });
        client.start();
        System.out.println("Client started");
    }
}
```
[to top](#overview)

---

### More examples?

We have growing collection of examples in our [git repository] (https://github.com/NFSdb/nfsdb/tree/master/nfsdb-examples/src/main/java/org/nfsdb/examples).

[to top](#overview)

---

### Performance

On test rig (Intel i7-920 @ 4Ghz) NFSdb shows average read latency of 20-30ns and write latency of 60ns per column of data. Read and write do not have any GC overhead (excluding reading strings - they are immutable).

Above example takes ~500ms to produce and fully consume 1 million objects on localhost.

[to top](#overview)

---

##Maven

NFSdb requires minimum of Java 7 and stable release is available from maven Central

```xml
<dependency>
    <groupId>com.nfsdb</groupId>
    <artifactId>nfsdb-core</artifactId>
    <version>2.0.1</version>
</dependency>
```
Check out [release notes](https://github.com/NFSdb/nfsdb/releases/tag/2.0.1) for details of the release.

Snapshot releases are also available from Maven central. To get hold of those add these lines to pom.xml:

```xml
<repositories>
    <repository>
        <id>sonatype-snapshots</id>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.nfsdb</groupId>
    <artifactId>nfsdb-core</artifactId>
    <version>2.0.2-SNAPSHOT</version>
</dependency>
```

[to top](#overview)

---

## License

NFSdb is available under [Apache 2.0 License] (http://www.apache.org/licenses/LICENSE-2.0.txt)

[to top](#overview)

---

## Support

We actively respond to all [issues](https://github.com/NFSdb/nfsdb/issues) raised via GitHub. Please do not hesitate to ask questions or request features.

[to top](#overview)

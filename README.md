
[![Codacy Badge](https://api.codacy.com/project/badge/grade/83c6250bd9fc45a98c12c191af710754)](https://www.codacy.com/app/bluestreak/nfsdb)
[![CircleCI](https://img.shields.io/circleci/build/github/questdb/questdb/master?token=c019f9fac8d84c0fa4896447d6073504a830e099)](https://circleci.com/gh/questdb/questdb)
[![Slack Status](https://serieux-saucisson-79115.herokuapp.com/badge.svg)](https://serieux-saucisson-79115.herokuapp.com/)
[![Downloads](https://img.shields.io/github/downloads/questdb/questdb/total)](https://github.com/questdb/questdb/releases/download/4.0.0/questdb-4.0.0-bin.tar.gz)

## About QuestDB

QuestDB is a relational database built to provide ultimate performance for time-series data.

Our technology is born from low-latency trading and built around fully zero-GC java and off-heap data structures.
We provide the highest performance, use SQL and offer one binary, which is portable across architectures.

We don't use third-party libraries, and our design may seem unorthodox. This is because we challenge the status-quo across the full stack.
Our approach is a different take on high performance databases and was born from low latency trading principles. We elevate database performance to new heights. 

All code is licensed under the Apache 2.0 Open Source license.

## Building from source

### Pre-requitites:

- Java 8 64-bit. We recommend Oracle Java 8, but OpenJDK8 will also work (although a little slower).
- Maven 3
- Compatible 64-bit Operating System: Windows, Linux, OSX or ARM Linux
- Configure JAVA_HOME environment variable
- Add Maven "bin" directory to PATH environment variable

```
Note: Java versions above 8 are not yet supported. It is possible to build QuestDB with Java 11,
but this requires backward incompatible source code changes.
```

### Building

```
mvn clean package
```


### Running

Main class: `io.questdb.ServerMain`

Program arguments: `-d <home_directory>`

QuestDB will start HTTP server on 0:9000, which you can visit from your browser: http://localhost:9000. HTTP server is constrained by directory specified as program argument (-d). Additionally QuestDB will start PostgreSQL server on 0:8812, default login credentials are admin/quest. Both HTTP and PostresSQL server reference database in `home_directory/db`

## Getting Started

[Install](https://www.questdb.io/docs/install) and [run](https://www.questdb.io/docs/run) QuestDB.
Then, the easiest way to get started is to play with our
web [console](https://www.questdb.io/docs/console). This will allow you to import
and query data using an intuitive interface.

You may also take a look at our [storage model](https://www.questdb.io/docs/storagemodel). In a nutshell,
we are a column-oriented database, which partitions data by time intervals.

You can find more documentation [here](https://www.questdb.io/docs/documentation)

## Support / Contact

- [Slack Channel](https://join.slack.com/t/questdb/shared_invite/enQtNzk4Nzg4Mjc2MTE2LTEzZThjMzliMjUzMTBmYzVjYWNmM2UyNWJmNDdkMDYyZmE0ZDliZTQxN2EzNzk5MDE3Zjc1ZmJiZmFiZTIwMGY)

### Roadmap

We have built the ultimate performance for read and write on a single-thread.
But we still have some work to do.
Elements on our roadmap include:

- Query and aggregates optimisation. Currently, we run aggregates through linear scans.
While our scans are highly efficient, our current implementations of aggregates are somehow naive.
Further optimisation will take performance to new levels.
- Multithreading. Currently, we use one single thread. While this is good for certain use cases
(you can limit QuestDB to one thread and leave resources for other programs), we will provide
the ability to distribute load/query work over several cores to benefit from parallelisation.
- High-availability. Working on providing out-of-the-box high-availability with extreme simplicity.

### Contribution

Feel free to contribute to the project by forking the repository and submitting pull requests.
Please make sure you have read our [contributing guide](https://github.com/questdb/questdb/blob/master/CONTRIBUTING.md).

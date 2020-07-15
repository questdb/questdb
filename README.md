<div align="center">
  <img alt="QuestDB Logo" src="https://raw.githubusercontent.com/questdb/questdb/master/.github/logo-readme.png" width="305px"/>
</div>
<p>&nbsp;</p>

<p align="center">
  <a href="https://github.com/questdb/questdb/blob/master/LICENSE.txt">
    <img src="https://img.shields.io/github/license/questdb/questdb" />
  </a>
  <a href="https://www.codacy.com/app/bluestreak/nfsdb">
    <img src="https://api.codacy.com/project/badge/grade/83c6250bd9fc45a98c12c191af710754" />
  </a>
  <a href="https://circleci.com/gh/questdb/questdb">
    <img src="https://img.shields.io/circleci/build/github/questdb/questdb/master?token=c019f9fac8d84c0fa4896447d6073504a830e099" />
  </a>
  <a href="https://github.com/questdb/questdb/releases/tag/5.0.1">
    <img src="https://img.shields.io/github/downloads/questdb/questdb/total" />
  </a>
  <a href="https://search.maven.org/search?q=g:org.questdb">
    <img src="https://img.shields.io/maven-central/v/org.questdb/core" />
  </a>
  <a href="https://serieux-saucisson-79115.herokuapp.com/">
    <img src="https://serieux-saucisson-79115.herokuapp.com/badge.svg" />
  </a>
</p>

<div align="center">

<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-6-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->
</div>

## What is QuestDB

QuestDB is an open-source database designed to make time-series lightning fast
and easy.

It uses a column-oriented approach, vectorized execution, SIMD instructions, and
a whole array of low-latency techniques. The whole code base is built from
scratch, without dependencies, in the name of performance. We are 100% free from
garbage collection.

QuestDB implements SQL, and augments it for time-series. It exposes a Postgres
Wire protocol, a high-performance HTTP API, and even supports ingestion with
Influx Line Protocol. It supports both relational and time-series joins, which
makes it easy to correlate data over time. Writes are durably committed to disk,
meaning that the data is safe, yet instantly accessible.

## Performance figures

### Raw figures

Number operations per second **per thread**. Writes are durable and written to
disk.

| Operation | 64-bit double  | 32-bit int     |
| --------- | -------------- | -------------- |
| Read      | 120 Million /s | 240 Million /s |
| Write     | 240 Million /s | 480 Million /s |

On a CPU with 6 memory channels, QuestDB can scan through **117GB of data per
second**.

### Queries

Execution time on a c5.metal instance using 16 of the 96 threads available.

| Query                                                     | Runtime    |
| --------------------------------------------------------- | ---------- |
| `SELECT sum(double) FROM 1bn`                             | 0.061 secs |
| `SELECT tag, sum(double) FROM 1bn`                        | 0.179 secs |
| `SELECT tag, sum(double) FROM 1bn WHERE timestamp='2019'` | 0.05 secs  |

## Getting Started

The easiest way to get started is with Docker:

```shell script
docker run -p 9000:9000 -p 8812:8812 questdb/questdb
```

You can more information about Docker usage
[here](https://hub.docker.com/repository/docker/questdb/questdb/).

#### Alternative methods

- [Start with Homebrew](https://questdb.io/docs/guideHomebrew)
- [Start with the binaries](https://questdb.io/docs/guideBinaries)

### Connecting to QuestDB

You can interact with QuestDB using:

- [Web Console](https://questdb.io/docs/usingWebConsole) listening on port
  `9000`: [localhost:9000](http://localhost:9000)
- [Postgres Wire](https://questdb.io/docs/guidePSQL), for example with PSQL
  (alpha) on port `8812`
- [HTTP API](https://questdb.io/docs/guideREST) listening on port `9000`

Both the HTTP and PostgreSQL servers reference the database in
`<root_directory>/db`.

You can connect to the Postgres server as follows. The default password is
`quest`

```shell script
psql -h localhost -p 8812 -U admin -W -d qdb
```

## Building from source

#### (a) Prerequisites

- Java 11 64-bit
- Maven 3
- Node.js 12 / NPM 6

```shell script
java --version
mvn --version
node --version
```

#### (b) Clone the Repository

```shell script
git clone git@github.com:questdb/questdb.git
```

#### (c) Build the Code

```shell script
cd questdb
mvn clean package -DskipTests
```

The build should take around 2 minutes. You can remove `-DskipTests` to run the
3000+ unit tests. The tests take 3-5 minutes to run.

#### (d) Run QuestDB

```shell script
# Create a database root directory and run QuestDB
mkdir <root_directory>
java -p core/target/core-5.0.2-SNAPSHOT.jar -m io.questdb/io.questdb.ServerMain -d <root_directory>
```

## Resources

Complete references are available in the
[Documentation](https://questdb.io/docs/documentationOverview). There are also
several guides to get started.

Quick-start guides:

- [Docker](https://questdb.io/docs/guideDocker)
- [Homebrew](https://questdb.io/docs/guideHomebrew)
- [Using the binaries](https://questdb.io/docs/guideBinaries)

Usage guides:

- [Web Console](https://questdb.io/docs/usingWebConsole)
- [Postgres Wire with PSQL](https://questdb.io/docs/guidePSQL) (alpha)
- [REST API](https://questdb.io/docs/guideREST)
- [CRUD operations](https://questdb.io/docs/crudOperations)

Concepts:

- [SQL extensions](https://questdb.io/docs/sqlExtensions)
- [Storage model](https://questdb.io/docs/storageModel)
- [Partitions](https://questdb.io/docs/partitions)
- [Designated timestamp](https://questdb.io/docs/designatedTimestamp)

## Support / Contact

[Slack Channel](https://join.slack.com/t/questdb/shared_invite/enQtNzk4Nzg4Mjc2MTE2LTEzZThjMzliMjUzMTBmYzVjYWNmM2UyNWJmNDdkMDYyZmE0ZDliZTQxN2EzNzk5MDE3Zjc1ZmJiZmFiZTIwMGY)

## Roadmap

[Our roadmap is here](https://github.com/questdb/questdb/projects/3)

## Contribution

Feel free to contribute to the project by forking the repository and submitting
pull requests. Please make sure you have read our
[contributing guide](https://github.com/questdb/questdb/blob/master/CONTRIBUTING.md).

## Contributors ✨

Thanks to these wonderful people
([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="https://github.com/clickingbuttons"><img src="https://avatars1.githubusercontent.com/u/43246297?v=4" width="100px;" alt=""/><br /><sub><b>clickingbuttons</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=clickingbuttons" title="Code">💻</a> <a href="#ideas-clickingbuttons" title="Ideas, Planning, & Feedback">🤔</a> <a href="#userTesting-clickingbuttons" title="User Testing">📓</a></td>
    <td align="center"><a href="https://github.com/ideoma"><img src="https://avatars0.githubusercontent.com/u/2159629?v=4" width="100px;" alt=""/><br /><sub><b>ideoma</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=ideoma" title="Code">💻</a> <a href="#userTesting-ideoma" title="User Testing">📓</a> <a href="https://github.com/questdb/questdb/commits?author=ideoma" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://github.com/tonytamwk"><img src="https://avatars2.githubusercontent.com/u/20872271?v=4" width="100px;" alt=""/><br /><sub><b>tonytamwk</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=tonytamwk" title="Code">💻</a> <a href="#userTesting-tonytamwk" title="User Testing">📓</a></td>
    <td align="center"><a href="http://sirinath.com/"><img src="https://avatars2.githubusercontent.com/u/637415?v=4" width="100px;" alt=""/><br /><sub><b>sirinath</b></sub></a><br /><a href="#ideas-sirinath" title="Ideas, Planning, & Feedback">🤔</a></td>
    <td align="center"><a href="https://www.linkedin.com/in/suhorukov"><img src="https://avatars1.githubusercontent.com/u/10332206?v=4" width="100px;" alt=""/><br /><sub><b>igor-suhorukov</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=igor-suhorukov" title="Code">💻</a> <a href="#ideas-igor-suhorukov" title="Ideas, Planning, & Feedback">🤔</a></td>
    <td align="center"><a href="https://github.com/mick2004"><img src="https://avatars1.githubusercontent.com/u/2042132?v=4" width="100px;" alt=""/><br /><sub><b>Abhishek Pratap Singh</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=mick2004" title="Code">💻</a> <a href="#platform-mick2004" title="Packaging/porting to new platform">📦</a></td>
  </tr>
</table>

<!-- markdownlint-enable -->
<!-- prettier-ignore-end -->
<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the
[all-contributors](https://github.com/all-contributors/all-contributors)
specification. Contributions of any kind welcome!

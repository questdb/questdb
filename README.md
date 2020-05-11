<h4 align="center">
  <img src="https://raw.githubusercontent.com/questdb/questdb/master/ui/assets/images/logo-readme.jpg"/>
</h4>

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
  <a href="https://github.com/questdb/questdb/releases/download/4.2.1/questdb-4.2.1-bin.tar.gz">
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

[![All Contributors](https://img.shields.io/badge/all_contributors-4-orange.svg?style=flat-square)](#contributors-)

<!-- ALL-CONTRIBUTORS-BADGE:END -->
</div>

## What is QuestDB

QuestDB is an open-source NewSQL relational database designed to process
time-series data, faster. Our approach comes from low-latency trading; QuestDB’s
stack is engineered from scratch, zero-GC Java and dependency-free.

QuestDB ingests data via HTTP, PostgreSQL wire protocol, Influx line protocol or
directly from Java. Reading data is done using SQL via HTTP, PostgreSQL wire
protocol or via Java API. The whole database and console fits in a 3.5Mb
package.

## Project goals

- Treat time-series as first class citizen within a relational database
  framework.

- Minimise hardware resources through software optimisation. Don’t waste CPU
  cycles, memory nor storage.

- Be a reliable and trustworthy store of critical data.

- Low friction operation. Empower developers with SQL. Simplify every database
  interaction.

- Operate efficiently at both extremes: allow users to prioritise performance
  over data loss, and vice versa.

- Be both embedded and standalone.

## Getting Started

See our [My First Database](https://www.questdb.io/docs/myFirstDatabase)
tutorial. The easiest way to get started is to play with our web
[console](https://www.questdb.io/docs/usingWebConsole). This will allow you to
import and query data using an intuitive interface.

You may also take a look at our
[storage model](https://www.questdb.io/docs/storageModel). In short, we are a
column-oriented database, which partitions data by time intervals.

You can find our documentation
[here](https://www.questdb.io/docs/documentationOverview)

## Support / Contact

- [Slack Channel](https://join.slack.com/t/questdb/shared_invite/enQtNzk4Nzg4Mjc2MTE2LTEzZThjMzliMjUzMTBmYzVjYWNmM2UyNWJmNDdkMDYyZmE0ZDliZTQxN2EzNzk5MDE3Zjc1ZmJiZmFiZTIwMGY)

## Roadmap

[Our roadmap is here](https://github.com/questdb/questdb/projects/3)

## Building from source

### Pre-requisites

- Operating system - **x86-64**: Windows, Linux, FreeBSD and OSX / **ARM
  (AArch64/A64)**: Linux
- Java 11 64-bit. We recommend Oracle Java 11, but OpenJDK 11 will also work
  (although a little slower)
- Maven 3 (from your package manager on Linux / OSX
  ([Homebrew](https://github.com/Homebrew/brew)) or
  [from the jar](https://maven.apache.org/install.html) for any OS)
- Node.js 12 / npm 6 (to manage your Node.js versions we recommend
  [nvm](https://github.com/nvm-sh/nvm) for OSX/Linux/windows WSL, and
  [nvm-windows](https://github.com/coreybutler/nvm-windows) for Windows) -
  OPTIONAL

> Java versions above 8 are not yet supported. It is possible to build QuestDB
> with Java 11, but this requires backward incompatible changes. If your java
> version is above 8 you can download & install JDK8 and use the absolute path
> to the java executable instead of "`java`".

### Building & Running

```bash
git clone git@github.com:questdb/questdb.git
cd questdb

# Check the java version, the output should be similar to:
#
# openjdk 11.0.6 2020-01-14
# OpenJDK Runtime Environment (build 11.0.6+10)
# OpenJDK 64-Bit Server VM (build 11.0.6+10, mixed mode)
#
# For OpenJDK, for Oracle JDK you should get:
#
# java version "11.0.6" 2019-01-14 LTS
# Java(TM) SE Runtime Environment 18.9 (build 11.0.6+8-LTS)
# Java HotSpot(TM) 64-Bit Server VM 18.9 (build 11.0.6+8-LTS, mixed mode)
java -version

# Maven flags:
# 1. Remove 'skipTests' if you want to run all tests (3000+ unit tests, 3-5 mins)
# 2. You can add the parameter '-Dprofile.install-local-nodejs' if you do not want
# to rely on a local version of NodeJS, it will be pulled by maven instead
mvn clean package -DskipTests

# replace <root_dir> with the actual directory name, example "out"
mkdir <root_dir>

java -cp core/target/core-4.2.1-SNAPSHOT.jar io.questdb.ServerMain -d <root_dir>
```

QuestDB will start an HTTP server with the web console available at
[localhost:9000](http://localhost:9000). Additionally, a PostgreSQL server will
be started and available on port 8812, the default login credentials are
`admin`/`quest`. Both the HTTP and PostresSQL servers reference the database in
`<root_directory>/db`.

## Contribution

Feel free to contribute to the project by forking the repository and submitting
pull requests. Please make sure you have read our
[contributing guide](https://github.com/questdb/questdb/blob/master/CONTRIBUTING.md).

## Contributors ✨

Thanks goes to these wonderful people
([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="https://github.com/clickingbuttons"><img src="https://avatars1.githubusercontent.com/u/43246297?v=4" width="100px;" alt=""/><br /><sub><b>clickingbuttons</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=clickingbuttons" title="Code">💻</a> <a href="#ideas-clickingbuttons" title="Ideas, Planning, & Feedback">🤔</a> <a href="#userTesting-clickingbuttons" title="User Testing">📓</a></td>
    <td align="center"><a href="http://sirinath.com/"><img src="https://avatars2.githubusercontent.com/u/637415?v=4" width="100px;" alt=""/><br /><sub><b>Suminda Sirinath Salpitikorala Dharmasena</b></sub></a><br /><a href="#ideas-sirinath" title="Ideas, Planning, & Feedback">🤔</a></td>
    <td align="center"><a href="https://github.com/tonytamwk"><img src="https://avatars2.githubusercontent.com/u/20872271?v=4" width="100px;" alt=""/><br /><sub><b>tonytamwk</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=tonytamwk" title="Code">💻</a> <a href="#userTesting-tonytamwk" title="User Testing">📓</a></td>
    <td align="center"><a href="https://github.com/ideoma"><img src="https://avatars0.githubusercontent.com/u/2159629?v=4" width="100px;" alt=""/><br /><sub><b>Alex Pelagenko</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=ideoma" title="Code">💻</a> <a href="#userTesting-ideoma" title="User Testing">📓</a> <a href="https://github.com/questdb/questdb/commits?author=ideoma" title="Tests">⚠️</a></td>
  </tr>
</table>

<!-- markdownlint-enable -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the
[all-contributors](https://github.com/all-contributors/all-contributors)
specification. Contributions of any kind welcome!

<h4 align="center">
  <img src="https://raw.githubusercontent.com/questdb/questdb/master/core/src/main/resources/site/public/images/logo-readme.jpg"/>
</h4>

<p align="center">
  <a href="https://github.com/questdb/questdb/blob/master/LICENSE.txt"><img src="https://img.shields.io/github/license/questdb/questdb"></a>
  <a href="https://www.codacy.com/app/bluestreak/nfsdb"><img src="https://api.codacy.com/project/badge/grade/83c6250bd9fc45a98c12c191af710754"></a>
  <a href="https://circleci.com/gh/questdb/questdb"><img src="https://img.shields.io/circleci/build/github/questdb/questdb/master?token=c019f9fac8d84c0fa4896447d6073504a830e099"></a>
  <a href="https://serieux-saucisson-79115.herokuapp.com/"><img src="https://serieux-saucisson-79115.herokuapp.com/badge.svg"></a>
  <a href="https://github.com/questdb/questdb/releases/download/4.0.1/questdb-4.0.1-bin.tar.gz"><img src="https://img.shields.io/github/downloads/questdb/questdb/total"></a>
  <a href="https://search.maven.org/search?q=g:org.questdb"><img src="https://img.shields.io/maven-central/v/org.questdb/core"></a>
</p
  
<p/>

## What is QuestDB

QuestDB is a fast NewSQL database for Hybrid Transactional, Analytical and Time Series Processing workloads. QuestDB ingests 
data via HTTP, PostgresSQL wire protocol, Influx line protocol or directly from Java. Reading data is done using SQL via HTTP, 
PostgreSQL wire protocol or via Java API. The whole database and console fits in a 3.5Mb package.

## Project goals

- Treat time-series as first class citizen within a relational database framework.

- Minimise hardware resources through software optimisation. Don’t waste CPU cycles, memory nor storage.

- Be a reliable and trustworthy store of critical data.

- Low friction operation. Empower developers with SQL. Simplify every database interaction.

- Operate efficiently at both extremes: allow users to prioritise performance over data loss, and vice versa.

- Be both embedded and standalone.

## Getting Started

[Install](https://www.questdb.io/docs/install) and [run](https://www.questdb.io/docs/run) QuestDB.
Then, the easiest way to get started is to play with our
web [console](https://www.questdb.io/docs/console). This will allow you to import
and query data using an intuitive interface.

You may also take a look at our [storage model](https://www.questdb.io/docs/storagemodel). In short,
we are a column-oriented database, which partitions data by time intervals.

You can find more documentation [here](https://www.questdb.io/docs/documentation)

## Support / Contact

- [Slack Channel](https://join.slack.com/t/questdb/shared_invite/enQtNzk4Nzg4Mjc2MTE2LTEzZThjMzliMjUzMTBmYzVjYWNmM2UyNWJmNDdkMDYyZmE0ZDliZTQxN2EzNzk5MDE3Zjc1ZmJiZmFiZTIwMGY)

## Roadmap

Our roadmap is [here](https://www.questdb.io/docs/roadmap)

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

### Building & Running

```
git clone https://github.com/questdb/questdb.git
cd questdb

# check java version
# output should be similar to:
#
# java version "1.8.0_212"
# Java(TM) SE Runtime Environment (build 1.8.0_212-b10)
# Java HotSpot(TM) 64-Bit Server VM (build 25.212-b10, mixed mode)
#
# if your java version is above 8 you can download & install JDK8 and use absolute
# path to java executable instead of 'java'

java -version

# remove 'skipTests' if you want to run all tests (3000+ unit tests, 3-5 mins)
mvn clean package -DskipTests

# check contents of 'core/target' directory to find QuestDB's current version number

# create QuestDB root directory if one does not exist already
# replace <root_dir> with actual directory name
mkdir <root_dir>

# <version> is QuestDB's current version from pom.xml
# <root_dir> is the root directory created at the previous step
java -cp core/target/core-<version>.jar io.questdb.ServerMain -d <dir>
```

QuestDB will start the HTTP server on 0:9000, which you can visit from your browser: http://localhost:9000. HTTP server is constrained by directory specified as program argument (-d). Additionally QuestDB will start PostgreSQL's server on 0:8812, default login credentials are admin/quest. Both HTTP and PostresSQL server reference database in `<root_directory>/db`


## Contribution

Feel free to contribute to the project by forking the repository and submitting pull requests.
Please make sure you have read our [contributing guide](https://github.com/questdb/questdb/blob/master/CONTRIBUTING.md).

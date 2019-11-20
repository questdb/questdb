## Contributing to QuestDB

### Raise an Issue

Raising **[issues](https://github.com/questdb/questdb/issues)** on GitHub is super welcome. We aim to respond quickly and thoughtfully. This is a good place to start before deep diving into the code base.

### Contribute a PR

#### Requirements

- Java 8. You can use either Oracle JDK 8 or OpenJDK 8. Newer versions of Java are not yet supported.
- Maven 3 (https://maven.apache.org/download.cgi)
- C-compiler, CMake - to contribute to C libraries.
- Java IDE
- 64-bit Operating System (OSX, Windows or Linux) - QuestDB does not work on 32-bit platforms.

### Repository overview

Repository contains three components:
- QuestDB database and libraries source in Java (core/src/main/java)
- QuestDN database tests (core/src/test/java)
- QuestDB libraries in C (core/src/main/c)
- QuestDB Windows service wrapper (win64svc/)
- QuestDB Web Console (ui/)
- Micro benchmarks (benchmarks/)

QuestDB is packaged with maven, which packages everything under (core/src/). For this reason compilation output of
other packages is distributed as follows:

core/src/main/c -> core/src/main/resources/binaries
ui -> core/src/main/resources/site/public
win64svc -> core/src/main/bin

Compiled binaries are committed to git to make builk of development process Java-centric and simplified.

## Compiling Java

Unless your default Java is Java8 you may want to set JAVA_HOME to Java8 directory before running maven:

Linux/OSX
```text
export JAVA_HOME=/path/to/java/
mvn clean package
```

Windows
```text
set JAVA_HOME="c:\path\to\java directory"
mvn clean package
```

## Compiling C-libraries

C-libraries will have to be compiled for each platform separately. The following commands will compile on Linux/OSX:

```text
export JAVA_HOME=/path/to/java
cmake
make
```

on Windows we use Intellij CLion, which can open cmake files.

## Testing

We have a lot of unit tests, most of which are of "integration" type, e.g. test starts a server, interacts with it and asserts the outcome. We expect all contributors to submit PRs with tests. Please reach out to us via slack if you uncertain on how to test or you think existing test is inadequate and should be removed.

## Dependencies

QuestDB does not have dependencies. This may sound unorthodox but in realitiy we try not to reinvent the wheel but rather than using libraries we
implement best algorithms ourselves to ensure perfect fit with existing code. With that in mind we expect contribitions that do not add third-party dependencies.

## Allocations, "new" operator and garbage collection

QuestDB is zero-GC along data pipelines. We expect contributions not to allocate if possible. That said we would like to help you to
contribute zero-GC code, do not hesitate to reach out!

## Committing

We use [Conventional commits](https://www.conventionalcommits.org/en/v1.0.0/) to auto-generate release notes. We require all commit comments to conform. To that end commits have to be granular enough to be successfuly described using this method.

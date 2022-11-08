# Contributing to QuestDB

Hi, glad to know that you're interested in contributing to QuestDB. Here are
some topics that can help you to get started:

- 💡 [Bugs and features](#bugs-and-features)
- 🧭 [Navigation](#navigation)
- 🔧 [Environment setup](#environment-setup)
- ✅ [Before you submit](#before-you-submit)
- 📢 [For maintainers](#for-maintainers)

# Bugs and features

Whether it's a bug report or feature request, you're welcome to raise an
**[issue](https://github.com/questdb/questdb/issues)** using the respective
template.

If you're not sure whether you should raise an issue, you can also join our
community **[Slack channel](https://slack.questdb.io/)** and post your questions
there.

If you find any **security bug**, kindly refer to [SECURITY.md](https://github.com/questdb/questdb/blob/master/SECURITY.md) file for more info.

We aim to respond to your issues and questions soonest. If you wish to receive a
faster response, we recommend you always describe your steps and provide
information about your environment in the bug reports. And if you're proposing a
new feature, it'll help us to evaluate the priority if you explain why you need
it.

# Navigation

## Repository overview

- QuestDB database and libraries source in Java:
  [`core/src/main/java/`](https://github.com/questdb/questdb/tree/master/core/src/main/java)
- QuestDB database tests:
  [`core/src/test/java/`](https://github.com/questdb/questdb/tree/master/core/src/test/java)
- QuestDB libraries in C:
  [`core/src/main/c/`](https://github.com/questdb/questdb/tree/master/core/src/main/c)
- QuestDB Windows service wrapper:
  [`win64svc/`](https://github.com/questdb/questdb/tree/master/win64svc/)
- QuestDB web console (frontend):
  [`github.com/questdb/ui`](https://github.com/questdb/ui/tree/main/packages/web-console)
- Micro benchmarks:
  [`benchmarks/`](https://github.com/questdb/questdb/tree/master/benchmarks/)

Compiled binaries (for C libraries and Windows service wrapper) are committed to
git to make build of development process Java-centric and simplified.

## Finding suitable issues

Our maintainers label issues with relevant categories so you can use that to
search for certain type of issues you'd like to work on. If you don't know where
to start, try search for issues labeled with
[`good first issue`](https://github.com/questdb/questdb/labels/Good%20first%20issue)
or [`help wanted`](https://github.com/questdb/questdb/labels/Help%20wanted). If
you wish to understand how our maintainers work together, you can refer to
[this section](#for-maintainers).

# Environment setup

## Requirements

- Operating system - **x86-64**: Windows, Linux, FreeBSD and OS X
- Java 11 64-bit or later
- Maven 3 (from your package manager on Linux / OSX
  ([Homebrew](https://github.com/Homebrew/brew)) or
  [from the jar](https://maven.apache.org/install.html) for any OS)
- C-compiler, CMake - to contribute to C libraries - _OPTIONAL_

## Local environment

### Setting up Java and `JAVA_HOME`

`JAVA_HOME` is required by Maven. It is possible to have multiple versions of
Java on the same platform. Please set up `JAVA_HOME` to point to Java 11 or
later. Other versions of Java may not work. If you are new to Java please check
that `JAVA_HOME` is pointing to the root of Java directory:
`C:\Users\me\dev\jdk-11.0.8` and **not** `C:\Users\me\dev\jdk-11.0.8\bin\java`.

Linux/OS X

```bash
export JAVA_HOME="/path/to/java/"
```

Windows

```bash
set JAVA_HOME="c:\path\to\java directory"
```

### Compiling Java and frontend code

You can compile the database and build the web console with the following
command:

```bash
mvn clean package -DskipTests -P build-web-console
```

You can then run QuestDB with:

```bash
java -p core/target/questdb-<version>-SNAPSHOT.jar -m io.questdb/io.questdb.ServerMain -d <root_dir>
```

The web console will be available at [localhost:9000](http://localhost:9000).

### Compiling C-libraries

C-libraries will have to be compiled for each platform separately. Cmake will
also need `JAVA_HOME` to be set. The following commands will compile on
Linux/OSX.

```text
cd core
cmake -B build/release -DCMAKE_BUILD_TYPE=Release .
cmake --build build/release --config Release
```

For more details, see [CMake build instructions](core/CMAKE_README.md).

For C/С++ development we use CLion. This IDE "understands" cmake files and makes
compilation easier.

The build will copy artifacts as follows:

```
core/src/main/c -> core/src/main/resources/io/questdb/bin
```

## Local setup for frontend development

The frontend code (i.e. web console) is located in a [separate repository](https://github.com/questdb/ui/tree/main/packages/web-console).
To set it up you should follow instructions provided in that repository.

The development environment for frontend can run on it's own, but will require QuestDB instance running in the background. You can achieve this in multiple ways:

1. Run development version of QuestDB from this repository. Consult
   [environment setup](#environment-setup) section of this document
2. Run published QuestDB version, for example, with docker. More details
   can be found in the [readme of this repository](./README.md)

# Before you submit

## Testing

We have a lot of tests, most of which are of "integration" type, e.g. test
starts a server, interacts with it and asserts the outcome. We expect all
contributors to submit PRs with tests. Please reach out to us via Slack if
you're uncertain on how to test, or you think an existing test can be improved.

## Dependencies

QuestDB does not have Java dependencies. This may sound unorthodox but in
reality we try not to reinvent the wheel but rather than using libraries we
implement algorithms on first principles to ensure perfect fit with existing
code. With that in mind we expect contributions that do not add third-party
dependencies.

## Allocations, "new" operator and garbage collection

QuestDB is zero-GC along data pipelines. We expect contributions not to allocate
if possible. That said we would like to help you to contribute zero-GC code, do
not hesitate to reach out!

## Committing

We use [Conventional commits](https://www.conventionalcommits.org/en/v1.0.0/) to
auto-generate release notes. We require all commit comments to conform. To that
end, commits have to be granular enough to be successfully described using this
method.

## FAQ

### Why does the server work, but the UI returns a `404` on [localhost:9000](http://localhost:9000)?

This means that the web console artifacts are not present in
`core/src/main/resources/io/questdb/site/public.zip`. To fix this, you can
simply run:

```bash
mvn clean package -DskipTests -P build-web-console
```

### Why do some tests fail on Windows?

Some antivirus software may cause tests to fail. Typical indicators that
antivirus is interfering with tests are that tests
[pass on CI](https://github.com/questdb/questdb/blob/master/ci/new-pull-request.yml)
but are failing in the following cases:

- HTTP chunk size assertion (missing initial zeroes in e.g. `IODispatcherTest`)
- Test timeout
- JVM crash

In case of ESET products, the following steps may resolve the issue:

- Disable "application protocol content filtering" explicitly **or**
- Add `127.0.0.1` to "Excluded IP addresses" list in the **advanced settings**
  menu because disabling all top-level mechanisms doesn't turn protocol
  filtering off.

# For maintainers

We have an
[engineering project board](https://github.com/orgs/questdb/projects/2/views/8)
to help us organize pending GitHub issues among engineers across different
timezones. And there are configured views for bug reports and feature requests
respectively.

This is what you can do to organize open issues:

- Add new issues to project board
- Always assign yourself when you pick up an issue
- Label the issues correctly after you assess them
- Close the issue when no further action is required

Stages of our project board:

| Stage            | Description                                                                                   |
| ---------------- | --------------------------------------------------------------------------------------------- |
| New              | When users reported a bug, await for someone to reproduce or follow up                        |
| More info needed | When more communication with OP is required before we can begin triage                        |
| To do            | Once issues are confirmed and engineers can pick up. Order of items should imply the priority |
| In progress      | If an engineer picks up an issue, self-assign and move the issue to `In progress` stage       |
| Done             | Once the linked pull request is merged, the issue is closed and moved to `Done` automatically |

We also use labels to organize GitHub issues to have better overview. The
current labels can be roughly categorized as below:

| Categories | Labels                                                                                     |
| ---------- | ------------------------------------------------------------------------------------------ |
| Type       | `Bug`, `New features`, `Enhancement`, `Test`, `Tidy up`, `Question`, `Performance`, `Java` |
| Component  | `ILP`, `Rest API`, `Postgres wire`, `SQL`, `Core`, `UI`                                    |
| Priority   | `Immediate`, `Minor`, `Won't fix`, `Later`                                                 |
| Difficulty | `Good first issue`                                                                         |

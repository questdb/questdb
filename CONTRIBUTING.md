# Contributing to QuestDB

Hi, glad to know that you're interested in contributing to QuestDB. Here are
some topics that can help you get started:

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

If you find any **security bug**, kindly refer to [SECURITY.md](https://github.com/questdb/questdb/blob/master/SECURITY.md) for more info.

We aim to respond to your issues and questions as soon as possible. To help us respond faster, please always describe your steps and provide
information about your environment in bug reports. If you're proposing a
new feature, it helps us evaluate priority if you explain why you need
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
git to make the development process Java‑centric and simpler.

## Finding suitable issues

Our maintainers label issues with relevant categories so you can use that to
search for certain types of issues you'd like to work on. If you don't know where
to start, try searching for issues labeled with
[`good first issue`](https://github.com/questdb/questdb/labels/Good%20first%20issue)
or [`help wanted`](https://github.com/questdb/questdb/labels/Help%20wanted). If
you wish to understand how our maintainers work together, you can refer to
[this section](#for-maintainers).

# Environment setup

## Requirements

- Operating system - **x86-64**: Windows, Linux, FreeBSD, and macOS
- Java 11 64-bit or later
- Maven 3 (from your package manager on Linux/macOS
  ([Homebrew](https://github.com/Homebrew/brew)) or
  [from the jar](https://maven.apache.org/install.html) for any OS)
- C compiler, CMake — to contribute to C libraries — _OPTIONAL_

## Local environment

### Setting up Java and `JAVA_HOME`

`JAVA_HOME` is required by Maven. It is possible to have multiple versions of
Java on the same platform. Please set up `JAVA_HOME` to point to Java 11 or
later. Other versions of Java may not work. If you are new to Java please check
that `JAVA_HOME` is pointing to the root of the Java directory:
`C:\Users\me\dev\jdk-11.0.8` and **not** `C:\Users\me\dev\jdk-11.0.8\bin\java`.

Linux/macOS

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

### Code formatting

Code is formatted using configuration files in the `.idea` directory.
To minimize conflicts when merging and problems in CI, all contributed code should be formatted
before submitting a PR.

In IntelliJ IDEA you can:
- Automate formatting (preferable):
  - Open File | Settings
  - Choose Tools | Actions on Save
  - Select Reformat and Rearrange Code
  - Click Apply
- Or format files manually by selecting them and choosing Code | Reformat File.

### Compiling C libraries

C libraries will have to be compiled for each platform separately. CMake will
also need `JAVA_HOME` to be set. The following commands will compile on
Linux/macOS.

```text
cd core
cmake -B build/release -DCMAKE_BUILD_TYPE=Release .
cmake --build build/release --config Release
```

For more details, see [CMake build instructions](core/CMAKE_README.md).

For C/C++ development we use CLion. This IDE understands CMake files and makes
compilation easier.

The build will copy artifacts as follows:

```
core/src/main/c -> core/src/main/resources/io/questdb/bin
```

## Local setup for frontend development

The frontend code (i.e. web console) is located in a [separate repository](https://github.com/questdb/ui/tree/main/packages/web-console).
To set it up you should follow the instructions provided in that repository.

The development environment for the frontend can run on its own, but it requires a QuestDB instance running in the background. You can achieve this in multiple ways:

1. Run a development version of QuestDB from this repository. Consult the
   [environment setup](#environment-setup) section of this document.
2. Run a published QuestDB version, for example with Docker. More details
   can be found in the [README of this repository](./README.md).

# Before you submit

## Testing

We have a lot of tests, most of which are of the "integration" type (e.g., a test
starts a server, interacts with it, and asserts the outcome). We expect all
contributors to submit PRs with tests. Please reach out to us via Slack if
you're uncertain how to test, or you think an existing test can be improved.

Typical Maven commands:
- Run full test suite:

```bash
mvn test
```

- Run a specific test class:

```bash
mvn -Dtest=ClassNameTest test
```

- Run a specific test method:

```bash
mvn -Dtest=ClassNameTest#methodName test
```

## Dependencies

QuestDB does not have third-party Java dependencies. This may sound unorthodox; in
reality we try not to reinvent the wheel, but rather than using libraries we
implement algorithms on first principles to ensure a perfect fit with existing
code. With that in mind, we expect contributions not to add third-party
dependencies.

## Allocations, "new" operator and garbage collection

QuestDB is zero-GC along data pipelines. We expect contributions not to allocate
if possible. That said, we would like to help you contribute zero-GC code—do
not hesitate to reach out!

## Committing

We use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) to
auto-generate release notes. We require all commit messages to conform. To that
end, commits have to be granular enough to be successfully described using this
method.

Example:

```
feat(sql): add support for X in Y
fix(core): avoid NPE when Z
test(ilp): add regression test for issue #1234
```

## Pull request checklist

Before opening a PR, please ensure:
- Code is formatted as per the repo’s settings (see Code formatting).
- Tests pass locally; add/adjust tests for your changes.
- No new third-party dependencies are introduced.
- Commit messages follow Conventional Commits.
- The PR description links the relevant issue and explains the change and rationale.

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
but fail in the following cases:

- HTTP chunk size assertion (missing initial zeroes in e.g. `IODispatcherTest`)
- Test timeout
- JVM crash

In case of ESET products, the following steps may resolve the issue:

- Disable "application protocol content filtering" explicitly, or
- Add `127.0.0.1` to the "Excluded IP addresses" list in the advanced settings
  menu because disabling all top-level mechanisms doesn't turn protocol
  filtering off.

# For maintainers

We have an
[engineering project board](https://github.com/orgs/questdb/projects/2/views/8)
to help us organize pending GitHub issues among engineers across different
timezones. There are configured views for bug reports and feature requests
respectively.

This is what you can do to organize open issues:

- Add new issues to the project board
- Always assign yourself when you pick up an issue
- Label the issues correctly after you assess them
- Close the issue when no further action is required

Stages of our project board:

| Stage            | Description                                                                                   |
| ---------------- | --------------------------------------------------------------------------------------------- |
| New              | When users report a bug; wait for someone to reproduce or follow up                           |
| More info needed | When more communication with OP is required before we can begin triage                        |
| To do            | Once issues are confirmed and engineers can pick them up. Order of items should imply priority |
| In progress      | If an engineer picks up an issue, self-assign and move the issue to `In progress`             |
| Done             | Once the linked pull request is merged, the issue is closed and moved to `Done` automatically |

We also use labels to organize GitHub issues to have better overview. The
current labels can be roughly categorized as below:

| Categories | Labels                                                                                     |
| ---------- | ------------------------------------------------------------------------------------------ |
| Type       | `Bug`, `New features`, `Enhancement`, `Test`, `Tidy up`, `Question`, `Performance`, `Java` |
| Component  | `ILP`, `Rest API`, `Postgres wire`, `SQL`, `Core`, `UI`                                    |
| Priority   | `Immediate`, `Minor`, `Won't fix`, `Later`                                                 |
| Difficulty | `Good first issue`                                                                         |

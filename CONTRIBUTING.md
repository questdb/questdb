# Contributing to QuestDB

Hi, glad to know that you're interested in contributing to QuestDB. Here are
some topics that can help you get started:

- 📜 [License and conduct](#license-and-conduct)
- 🎯 [Contribution expectations](#contribution-expectations)
- 🤝 [Contribution process](#contribution-process)
- 💡 [Bugs and features](#bugs-and-features)
- 🧭 [Navigation](#navigation)
- 🔧 [Environment setup](#environment-setup)
- ✅ [Before you submit](#before-you-submit)
- 📢 [For maintainers](#for-maintainers)

# License and conduct

By contributing to QuestDB, you agree that your contributions will be licensed
under the [Apache License 2.0](LICENSE.txt). No CLA or DCO signature is required.

Please review our [Code of Conduct](CODE_OF_CONDUCT.md) before participating.
We are committed to providing a welcoming and inclusive environment for everyone.

# Contribution expectations

QuestDB is a production-grade system serving critical use cases. All contributions
must meet high standards for quality and reliability. Please read this section
carefully before starting any work.

## Quality standards

- **Exhaustive testing required** — every contribution must include comprehensive tests
- **Problem-focused changes only** — contributions must solve a real problem; superficial
  changes (spelling fixes in comments, whitespace adjustments, stylistic tweaks) are not accepted
- **Minimize change surface area** — keep changes focused and minimal to reduce risk
- **Additive and non-breaking** — make every effort to avoid breaking existing behavior;
  if a breaking change is truly unavoidable, join [Slack](https://slack.questdb.io/) to
  discuss it before starting implementation
- **No architectural changes without discussion** — large-scale or structural changes
  must be discussed in an issue before implementation begins

## High-risk contributions

Changes that affect core data paths, storage formats, or wire protocols are considered
high-risk. These require prior discussion and approval from maintainers before any code
is written.

## Timeline and prioritization

QuestDB Inc maintains QuestDB Open Source. We use GitHub issues internally for planning.
We reserve the right to prioritize any issue according to our roadmap, even if a
contribution is in progress but proceeding slower than required.

# Contribution process

Only QuestDB maintainers and invited collaborators can open pull requests in
this repository. This keeps our review time for changes we have agreed to take.
We still welcome code from everyone: every change starts as an issue, and once
a maintainer agrees to it, they open the pull request from your fork.

1. **Propose the change.** Read the
   [contribution expectations](#contribution-expectations), then open an
   [issue](https://github.com/questdb/questdb/issues) or comment on an existing
   one. Describe the problem and how you plan to solve it. Do not start coding
   until you get the go-ahead.
2. **Get the go-ahead.** A maintainer reviews your proposal. If they agree with
   it, they assign the issue to you. If the issue is already assigned to
   someone else, please pick another one.
3. **Implement the change in your fork.** Work on a branch of your fork of this
   repository and follow [Before you submit](#before-you-submit).
4. **Ask for a pull request.** When your branch is ready for review, comment on
   the issue with a link to the branch and the pull request description that
   the [pull request checklist](#pull-request-checklist) asks for.
5. **A maintainer opens the pull request** from your branch. To address review
   comments, push to the same branch: the pull request picks up your new
   commits. You can reply to reviews on the pull request itself.
6. **A maintainer squash-merges the pull request** and adds a `Co-authored-by:`
   line with your name and email to the commit message, so GitHub lists you as
   a co-author of the commit.

Contributors with several merged changes may be invited as collaborators, which
lets them open pull requests directly.

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

## Claiming an issue

To work on an issue, follow the [contribution process](#contribution-process):
propose your approach in a comment, and start coding once a maintainer assigns
the issue to you.

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

- Operating system - **x86-64 or ARM64**: Windows, Linux, FreeBSD, and macOS.
  macOS is **ARM64 only**: QuestDB neither builds, tests nor ships native
  libraries for x86-64 (Intel) macOS, and the jar contains no `darwin-x86-64`
  binary. On an Intel Mac you have to build both native libraries from source
  yourself — see [Compiling the native libraries](#compiling-the-native-libraries).
- Java 25 64-bit (strict requirement — no earlier, no later)
- Maven 3 (latest version recommended; from your package manager on Linux/macOS
  ([Homebrew](https://github.com/Homebrew/brew)) or
  [from the jar](https://maven.apache.org/install.html) for any OS)
- Rust nightly and Cargo (the pinned toolchain is core/rust/qdbr/rust-toolchain.toml)
- A host linker/C toolchain: GCC or Clang on Linux, macOS, and FreeBSD, or MSVC on Windows
- CMake is optional; it is needed only to rebuild the committed C/C++ native libraries from source

**Note for Apple Silicon (ARM64) users:** Tests run normally, JIT tests included —
QuestDB compiles filters with the JIT on ARM64 as well as on x86-64, so nothing is
skipped on this architecture. Avoid reaching for an x86-64 JDK under Rosetta: the jar
bundles no `darwin-x86-64` native libraries, so that route additionally requires
building both native libraries from source in the same x86-64 environment — see
[Compiling the native libraries](#compiling-the-native-libraries).

## Local environment

### Setting up Java and `JAVA_HOME`

`JAVA_HOME` is required by Maven. It is possible to have multiple versions of
Java on the same platform. Please set up `JAVA_HOME` to point to Java 25.
Other versions of Java will not work. If you are new to Java please check
that `JAVA_HOME` is pointing to the root of the Java directory:
`C:\Users\me\dev\jdk-25` and **not** `C:\Users\me\dev\jdk-25\bin\java`.

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
java --add-exports=java.base/jdk.internal.vm=io.questdb \
  -p core/target/questdb-<version>-SNAPSHOT.jar \
  -m io.questdb/io.questdb.ServerMain -d <root_dir>
```

The `--add-exports=java.base/jdk.internal.vm=io.questdb` flag is required when
launching the module jar with `-m`. QuestDB's worker threads use the
JDK-internal `jdk.internal.vm.Continuation` API, and without the export the JVM
fails at startup with an `IllegalAccessError` on
`jdk.internal.vm.ContinuationScope` (the workers die and the server shuts down
right after the "listening on ..." lines). Pass the flag once; a second
`--add-exports` for the same package keeps only one target module.

The web console will be available at [localhost:9000](http://localhost:9000).

### Code formatting

Code is formatted using configuration files in the `.idea` directory.
To minimize conflicts when merging and problems in CI, all contributed code should be formatted
before it goes into a pull request.

In IntelliJ IDEA you can:
- Automate formatting (preferable):
  - Open File | Settings
  - Choose Tools | Actions on Save
  - Select Reformat and Rearrange Code
  - Click Apply
- Or format files manually by selecting them and choosing Code | Reformat File.

### Debugging

We recommend using IntelliJ IDEA for development and debugging. The repository
includes run configurations in the `.idea` directory that you can use to start
QuestDB with a debugger attached.

### Compiling the native libraries

QuestDB loads two native libraries: `libquestdb` (C/C++) and `libquestdbr`
(Rust). The repository commits the prebuilt C/C++ library for every supported
platform, while Maven builds the Rust library from source. You need the C/C++
steps below only when you change native code or run on a platform the repository
ships no C/C++ binary for, such as x86-64 (Intel) macOS.

Compile the C/C++ library with CMake, which also needs `JAVA_HOME`. These
commands work on Linux/macOS:

```text
cd core
cmake -B build/release -DCMAKE_BUILD_TYPE=Release .
cmake --build build/release --config Release
```

CMake writes `libquestdb` to `core/target/classes/io/questdb/bin-local/`.

Maven builds `libquestdbr` from source by default and copies it to
`core/target/classes/io/questdb/bin/<platform>/`; add the `qdbr-debug` profile
for a debug build. A bare `cargo build` in `core/rust/qdbr` leaves the library
in `core/rust/qdbr/target/`, which is on no classpath.

`io.questdb.std.Os` reads the locally built C++ library from `bin-local/` and the
Rust library from the platform directory. `mvn clean` deletes both outputs.

An IDE-only build (IntelliJ "Rebuild Project" without delegating to Maven)
copies resources but never runs the Rust build, so every test then fails in
`Os.<clinit>` with `Internal error: cannot find
/io/questdb/bin/<platform>/libquestdbr.<ext>, broken package?`. Run
`mvn -pl core compile`, or wire the `qdbr-build` target from
`core/rust/intellij_triggers.xml` as a before-compile Ant trigger, and rebuild.
A locally built jar carries the Rust library only for the platform it was
built on; official release jars bundle it for every supported platform.

For more details, see [CMake build instructions](core/CMAKE_README.md).

For C/C++ development we use CLion. This IDE understands CMake files and makes
compilation easier.

## Developing with the Java ILP client

The QuestDB server tests use the [Java ILP client](https://github.com/questdb/java-questdb-client)
for integration testing. `core/pom.xml` names the client version. When that version is a
released one, Maven resolves it from Maven Central; between releases it is a `-SNAPSHOT`
that no configured repository serves, so you have to build with `-P local-client` (see
below). CI detects the `-SNAPSHOT` and adds the profile itself. Use the same profile when
you need to modify both the client and the server simultaneously.

### Setup

The Java client is available as a git submodule in `java-questdb-client/`. Initialize it with:

```bash
git submodule update --init java-questdb-client
```

### Building with local client changes

Use the `local-client` profile to build the client from the submodule and use it in tests:

```bash
mvn test -P local-client
```

This will:
1. Build the client from `java-questdb-client/` first
2. Use the locally built client (the `-SNAPSHOT` version declared by the
   submodule) for server tests

### IntelliJ IDEA setup

To work on both the client and server in IntelliJ IDEA:

1. Initialize the submodule (see Setup above)
2. Open IntelliJ IDEA and go to **File > Project Structure > Modules**
3. Click **+** (Add) > **Import Module**
4. Select `java-questdb-client/core/pom.xml` and click **OK**
5. IntelliJ will import the client module alongside the server modules
6. In the **Project** view, you can now navigate and edit both codebases

To ensure IntelliJ uses the local client instead of the Maven Central version:

1. Open the **Maven** tool window (usually on the right side, or via **View > Tool Windows > Maven**)
2. Expand **Profiles** and check `local-client`
3. Click the **Reload All Maven Projects** button (circular arrows icon)

Now when you run tests from IntelliJ, they will use your local client changes.

### Workflow tips

- Make changes to both `java-questdb-client/` and `core/` as needed
- Run tests with `-P local-client` to verify everything works together
- When done, commit changes to each repository separately
- The client submodule changes should be pushed to the
  [java-questdb-client](https://github.com/questdb/java-questdb-client) repository

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
starts a server, interacts with it, and asserts the outcome). We expect every
contribution to include tests. Please reach out to us via Slack if
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

## Performance testing

For changes that may impact performance, you must benchmark before and after your
changes. Use the [row generator functions](https://questdb.com/docs/query/functions/row-generator/)
to create large datasets for meaningful measurements. Include data volume and
performance figures (before/after) in your PR description.

## Dependencies

QuestDB does not have third-party Java dependencies. This may sound unorthodox; in
reality we try not to reinvent the wheel, but rather than using libraries we
implement algorithms on first principles to ensure a perfect fit with existing
code. With that in mind, we expect contributions not to add third-party
dependencies.

## Allocations, "new" operator and garbage collection

QuestDB is zero-GC along critical paths. We expect contributions not to allocate
if possible. Here are the key principles:

- **No allocations on critical paths** — query execution and data ingestion must
  not trigger garbage collection
- **No string concatenation** — use the builder pattern (e.g., `StringSink`) instead
  of `+` or `StringBuilder`
- **No string creation** — work with `CharSequence`, `Utf8Sequence`, or direct
  memory where possible
- **Safe object reuse and pooling** — reuse objects rather than creating new ones;
  use existing pool implementations
- **No throwing `new` exceptions** — use pre-allocated exception instances or
  error codes on hot paths
- **Use QuestDB collections and primitives** — avoid JDK collections (`ArrayList`,
  `HashMap`, etc.); use QuestDB's internal implementations (`ObjList`, `CharSequenceIntHashMap`, etc.)
- **Use QuestDB utilities** — we have built-in tools for comparison, number parsing,
  and UTF-8 handling; use these rather than JDK equivalents
- **Watch out for lambdas** — lambdas can allocate; cache them if you must use them

If you're unsure how to achieve zero-GC for your contribution, reach out via Slack.

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

Before you ask for a pull request, or open one as a collaborator, make sure that:
- Code is formatted as per the repo's settings (see Code formatting).
- Tests pass locally; add/adjust tests for your changes.
- No new third-party dependencies are introduced.
- Commit messages follow Conventional Commits.
- The PR description must include the following (if a maintainer opens the PR
  for you, post the description on the issue):
  - Link to the relevant issue
  - Clear explanation of **why** the change is needed
  - Technical rationale and approach taken
  - For performance-related changes: before/after benchmarks with data volumes

  Reviewers should not have to reverse-engineer algorithms from code to understand
  what the PR does.

## Secret scanning

The `Gitleaks` check scans your branch's own commits for credentials. If it
fails on a real secret, rotate it before doing anything else.

For a false positive, the failing job's summary carries ready-to-paste
`.gitleaksignore` lines under "What to do with these findings", in the
commit-independent three-part `<file>:<rule-id>:<start-line>` form. Do not paste
the four-part `Fingerprint:` line that gitleaks prints in the job log: PRs here
are squash-merged, so the commit it names never reaches `master`, and the entry
silences your PR and then fails the push scan of `master`.

An ignore entry still pins a path and a line number, so anything that moves the
finding before the squash lands — a later commit, an unrelated pull request
shifting that line on `master`, a rename — stops it matching, leaving the PR
green and `master` red with nothing warning you. It is also blind to the secret
itself, silencing whatever that rule reports at that file and line from then on,
a real credential later written there included. Use one only for a settled file
and a value that will not change; `.gitleaksignore` has the full detail.

For a false positive that recurs, or one in a file anyone is still changing, add
an allowlist to `.gitleaks.toml` instead. It matches content rather than
position, so none of those cases can shift the finding out from under it. Match
the placeholder value itself, not the line it sits on: the default target is the
secret gitleaks extracted, so a substituted value stops matching and the finding
comes back. Wrap the value in `^\Q...\E$`, as below: `regexes` holds a Go RE2
regexp matched as an unanchored substring, so `\Q...\E` takes the placeholder
literally (an unescaped `.` would otherwise match any character) and `^...$`
keeps it from suppressing a longer, real secret that merely contains it.

```toml
[[rules]]
id = "generic-api-key"
  [[rules.allowlists]]
  description = "why this is not a secret"
  regexes = ['''^\QdGhlIHNhbXBsZSBub25jZQ==\E$''']
```

Reach for `regexTarget = "line"` only where there is no stable value to match —
the rule fires on a different string each time, or the placeholder itself keeps
changing. It trades a line number for a piece of content, but keeps the blind
spot above, silencing whatever that rule reports on any matching line.

```toml
[[rules]]
id = "generic-api-key"
  [[rules.allowlists]]
  description = "why this is not a secret"
  regexTarget = "line"
  regexes = ['''Sec-WebSocket-Key''']
```

Two ways a hand-written allowlist fails quietly. The `id` must name an existing
default rule exactly — `[extend]` disables rule validation, so a misspelled id
is accepted in silence, allowlists nothing, and leaves the job red on the
byte-identical finding. And add the `[[rules.allowlists]]` to the `[[rules]]`
block already present for that id, as there usually is one, because
`generic-api-key` raises nearly every finding here: a second block with the same
id replaces the first, discarding its allowlist with no warning.

The check reads each commit separately rather than the squashed diff, so a
secret-shaped line added in one commit and removed in a later one still fails.

Three limits are worth knowing, because in all three the push scan of `master`
is the first thing that reads the code:

- Fork pull requests do not receive the license secret, so the job skips the
  scan and reports success without reading anything.
- The action reads only a pull request's first 30 commits.
- The scan walks only your branch's first-parent line and skips merge commits,
  so nothing reachable only through a merge's second parent is read — neither a
  conflict resolution written into a merge commit nor a side branch you merged
  in.

## Branching

External contributors work on a branch of their own fork; a maintainer opens the
pull request from that branch (see [Contribution process](#contribution-process)).
Internal contributors (QuestDB team members) must prefix branch names with their
initials (e.g., `jd/fix-parser-bug`).

## Pull request review process

- Reviews begin once the PR is marked ready and CI passes
- We will not always remind contributors to check CI status; it is your responsibility
  to monitor your PR
- PRs that remain open for more than 2 weeks with failing CI will be closed
- Massive PRs from first-time contributors will be rejected — start with smaller,
  focused contributions to establish trust

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

## Handling external contributions

External contributors cannot open pull requests in this repository, so
maintainers open them on their behalf (see
[Contribution process](#contribution-process)):

1. To accept a proposal, assign the issue to the contributor. GitHub lets you
   assign anyone who has commented on the issue.
2. When the contributor asks for a pull request, check that the branch
   implements the agreed approach and meets the
   [contribution expectations](#contribution-expectations). If it does not,
   reply on the issue instead of opening a pull request.
3. Open the pull request from the contributor's branch, with a Conventional
   Commits title and the description the contributor posted. End the
   description with `Contributed by @<user>. Fixes #<issue>.`, so GitHub
   subscribes the contributor to the pull request and links the issue,
   followed by the contributor's `Co-authored-by:` line, which the first
   command below prints:

   ```bash
   gh api repos/questdb/questdb/compare/master...<user>:<branch> \
     --jq '.commits[].commit.author | "Co-authored-by: \(.name) <\(.email)>"' | sort -u
   gh pr create --repo questdb/questdb --base master --head <user>:<branch> \
     --title "<type>(<scope>): <description>" --body-file description.md
   ```

4. When you squash-merge the pull request, copy the `Co-authored-by:` line from
   the description into the commit message, which is empty by default. The
   squash commit names only the person who opened the pull request as its
   author, so without this line the contributor gets no credit. Pull requests
   from forks never get the required `PR title` check, so only a maintainer
   who can bypass the `master` rules can merge one (`--admin` in the CLI):

   ```bash
   gh pr merge <number> --squash --admin --body "Co-authored-by: Jane Doe <jane@example.com>"
   ```

## Inviting collaborators

A contributor with several merged changes can be invited to the repository with
the write role, which lets them open pull requests themselves. The write role
also lets them push branches to this repository, and workflows on those
branches run with the repository's Actions secrets, so invite only contributors
you trust with that access.

## Organizing issues

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

# Release steps

This is a short guide to outline steps involved making QuestDB release.

## Edit release notes

First step is to create a draft release with the intended release version number and release notes. Git tag should not
be created but the correct tag (the intended release number) should be chosen in the draft with the option
to create the tag on release publish. When crafting new release note, please take previous release notes as style
guidelines. Releases should not look too dissimilar.

## Create a new branch from `master`

```bash
git fetch
git checkout master
git pull
git checkout -b v_7_1_1
```

## Clear previous release "memory"

```bash
mvn release:clean
```

## Perform release

This step will do the following:
- create tag in `git` repo
- roll the versions on your branch from one snapshot to the next, e.g. from 7.1.1-SNAPSHOT to 7.1.2-SNAPSHOT
- publish `jar` to maven central

Note: you will need write access to git repo and maven central.

```bash
mvn -B release:prepare release:perform
```

Note that `-B` flag will make assumptions about release version. Use it if your release is routine. When releasing
a special version, for example `8.0` you will want to remove `-B` and answer questions about versions in interactive
mode that follows.

## Release Docker image

Azure [Build Docker Image](https://dev.azure.com/questdb/questdb/_build?definitionId=22) pipeline will automatically
build and release the docker image to Docker Hub once the tag is pushed to the repo.

### Manual way of building the docker

In case of [Build Docker Image](https://dev.azure.com/questdb/questdb/_build?definitionId=22) job fails, here are the
steps to build the images.

Prune docker images to ensure clean build

```
docker system prune -a
```

Build for multiple platforms at once and release version tag, `7.1.1` in this
case. This will take some time. Please note that tag is used twice in the command line:

```bash
cd core
docker buildx build --push --platform linux/arm64,linux/amd64 --tag questdb/questdb:7.1.1 --build-arg tag_name=7.1.1 .
```

Then build `latest`. This should be instant. Note tag name on the end of the command line.

```
docker buildx build --push --platform linux/arm64,linux/amd64 --tag questdb/questdb:latest --build-arg tag_name=7.1.1 .
```

### Build QuestDB binaries

[Github Release - Binaries](https://github.com/questdb/questdb/actions/workflows/github-binaries-release.yml) GitHub Actions workflow will
automatically run on release tag. This workflow will upload the binaries to the existing Github release draft.
It will also mark the release draft as non-draft and latest.

Check the workflow run status and when completed go to the latest release
in https://github.com/questdb/questdb/releases to check that everything is in order.

In case of workflow failure, prefer re-running the failed jobs in the
[Github Release - Binaries](https://github.com/questdb/questdb/actions/workflows/github-binaries-release.yml)
workflow. If you must build and upload the binaries by hand, mind that the Rust native library
(`libquestdbr.so` / `libquestdbr.dylib` / `questdbr.dll`) is no longer committed to the repository:
each `mvn package` builds it only for the host platform.

The platform-specific `rt-<platform>` archives are therefore correct when built on their own platform.
Build the Linux and Windows archives on Linux and Windows respectively, then upload the resulting
`core/target/questdb-*-rt-*.gz` files to the GH release page:

```bash
git fetch --tags
git checkout tags/7.1.1
mvn clean package -DskipTests -P build-web-console,build-binaries
```

The cross-platform `no-jre` JAR, however, must bundle the Rust library for **all** platforms. A
single-host `mvn clean package` produces a `no-jre` JAR missing the other platforms' Rust libraries,
and QuestDB will fail to start on them. To assemble it by hand, download the five `build-rust-*`
artifacts from the workflow run (those jobs usually succeed even when packaging fails), drop each into
the matching `core/target/classes/io/questdb/bin/<platform>/` directory, then package with `-DskipNative`
and no `clean` (which would delete the libraries you just placed):

```bash
git fetch --tags
git checkout tags/7.1.1

# Download the per-platform Rust libraries from the workflow run into:
#   core/target/classes/io/questdb/bin/linux-x86-64/libquestdbr.so      (artifact rust-linux-x64)
#   core/target/classes/io/questdb/bin/linux-aarch64/libquestdbr.so     (artifact rust-linux-arm64)
#   core/target/classes/io/questdb/bin/darwin-aarch64/libquestdbr.dylib (artifact rust-macos-arm64)
#   core/target/classes/io/questdb/bin/darwin-x86-64/libquestdbr.dylib  (artifact rust-macos-x64)
#   core/target/classes/io/questdb/bin/windows-x86-64/questdbr.dll      (artifact rust-windows)

mvn package -DskipTests -DskipNative -P build-web-console,build-binaries
```

## Release Java Library

Logging into https://oss.sonatype.org/ to release the library

## Release AMI

Our current AMI version has to be bumped with every release we do. This might change in the future, but for now
please follow these interactive steps to bump AMI:

https://questdb.slab.com/posts/how-to-release-a-new-aws-ami-w7rkjimy

## Update demo.questdb.io

You need to SSH to the demo box. Either from your desktop, or AWS console via web browser:

https://questdb.slab.com/posts/update-demo-airbus-and-telemetry-box-kyyl1mnw

## Release helm chart

This can be done from a unit env, Linux or OSX:

https://questdb.slab.com/posts/publish-helm-charts-zq0s8kj7

## Update pom.xml to snapshot

Similar to initial release update, update same pom.xml files to next version's
snapshot

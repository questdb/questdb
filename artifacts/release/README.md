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

[Github Binaries Release](https://dev.azure.com/questdb/questdb/_build?definitionId=33) Azure pipeline will
automatically run on release tag. This pipeline will upload the binaries to the existing Github release draft.
It will also mark the release draft as non-draft and latest.

Check the job run status and when completed go to the latest release
in https://github.com/questdb/questdb/releases to check that everything is in order.

In case of [Github Binaries Release](https://dev.azure.com/questdb/questdb/_build?definitionId=33) job failure, the
binaries can be compiled using maven on Windows and Linux
and uploaded to GH release page

```bash
git fetch --tags
git checkout tags/7.1.1
mvn clean package -DskipTests -P build-web-console,build-binaries
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

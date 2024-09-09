# Release steps

This is a short guide to outline steps involved making QuestDB release.

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
See previous versions of this readme to find out how to manually build and push the image.

## Edit release notes

[Github Binaries Release](https://dev.azure.com/questdb/questdb/_build?definitionId=33) Azure pipeline will
automatically run on release tag. This pipeline will create a draft Github release with the binaries attached.
Check the job run status and when completed go to the latest draft release
in https://github.com/questdb/questdb/releases
and edit the release notes. When crafting new release note, please take previous release notes as style
guidelines. Releases should not look too dissimilar.

In case of errors in Azure pipeline, see previous versions of this readme to find out how to manually build
the release binaries to attach to Github release.

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

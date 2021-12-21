# Release steps

This is a short guide to outline steps involved making QuestDB release.

## Compile release notes

Collect commit messages from git log and compile new `draft` release notes.
Please make sure not to publish it before `git` repo is ready.

When crafting new release note, please take previous release notes as style
guidelines. Releases should not look too dissimilar.

## Update pom.xml to release

Update pom.xml to remove -SNAPSHOT part. There are 3 files to update. Commit
this change to master.

## Publish release on GH

Publishing release will create a git tag on the latest master.

## Compile binaries on multiple platforms

Compile using maven on Windows, Linux and FreeBSD and upload to GH release

```bash
mvn clean package --batch-mode --quiet -DskipTests -P build-web-console,build-binaries
```

## Release Docker image

Prune docker images to ensure clean build

```
docker system prune -a
```

Build for multiple platforms at once and release version tag, `6.0.2` in this
case. This will take some time.

```
docker buildx build --push --platform linux/arm64,linux/amd64 --tag questdb/questdb:6.0.2 .
```

Then build `latest`. This should be instant.

```
docker buildx build --push --platform linux/arm64,linux/amd64 --tag questdb/questdb:latest .
```

## Release Java Library

Deploy to Maven Central

```
mvn clean deploy -DskipTests -P build-web-console,maven-central-release
```

Then `release` by logging into https://oss.sonatype.org/

## Update pom.xml to snapshot

Similar to initial release update, update same pom.xml files to next version's
snapshot

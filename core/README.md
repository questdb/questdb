# Building QuestDB

As a first step, clone the repository:

```script
git clone git@github.com:questdb/questdb.git
```

**Note:** in this document, the `<software_version>` placeholder may be replaced
by a release version number that follows semver conventions (e.g. `6.0.0`):

```bash
# example usage
docker push questdb/questdb:<software_version>-linux-amd64
# equivalent to
docker push questdb/questdb:6.0.0-linux-amd64
```

## Building from source

### Prerequisites

- Java 11 64-bit
- Maven 3
- Node.js 12 / NPM 6

```script
java --version
mvn --version
node --version
```

### Maven commands

The following commands will create a JAR without assembling executable binaries
nor build the web console:

```bash
cd questdb
mvn clean package -DskipTests
```

To package the web console with the JAR, use the following command:

```bash
mvn clean package -DskipTests -P build-web-console
```

To build executable binaries, use the following command:

```bash
mvn clean package -DskipTests -P build-web-console,build-binaries
```

To run tests, it is not required to have the binaries nor the web console built.
There are over 4000 tests that should complete within 2-6 minutes depending on
your system:

```bash
mvn clean test
```

To release to Maven Central, use the following command, which activates the
deploy profile. Ensure that your `~/.m2/settings.xml` file contains the
appropriate username/password for server `central`, and `gnupg` is on hand to
sign the artefacts.

```bash
mvn -pl !benchmarks clean deploy -DskipTests -P build-web-console,maven-central-release
```

### Run QuestDB

```bash
# Create a database root directory and run QuestDB
mkdir <root_directory>
java -p core/target/questdb-<software_version>.jar -m io.questdb/io.questdb.ServerMain -d <root_directory>
```

## Docker images

To build docker images successfully, please follow these instructions precisely.
There may be a lot of variation on how these images are built. This is what
worked:

- Use Windows OS. Docker Desktop might also work on Mac, but not on Linux.
- Download Docker Desktop _Edge_. At the time of writing (Nov 2019), only the
  Edge version is able to build ARM64 images and help create multi-platform
  manifests.
- Ensure that "experimental" features are enabled.

To verify that your Docker Desktop is good to go, try the following command:

```bash
docker buildx ls
```

You should see an output similar to:

```bash
C:\Users\blues>docker buildx ls
NAME/NODE DRIVER/ENDPOINT STATUS  PLATFORMS
default * docker
  default default         running linux/amd64, linux/arm64, linux/ppc64le, linux/s390x, linux/386, linux/arm/v7, linux/arm/v6
```

If docker complains on `buildx` command - you are either running the _stable_
version or have not enabled experimental features. Look through your Docker
Settings. There should be a JSON config file which you need to edit **and** a
switch in UI you need to toggle.

Assuming all is good, let's start building.

### Login to Docker Hub

The following command will prompt you for everything that is required and will
keep you logged in for some time. You don't have to run it if you know you are
logged in.

```bash
docker login
```

### Switch Docker Desktop to Linux

Right-click on the Docker Desktop tray icon (bottom right) and chose 'switch'
from the pop-up menu.

Create new builder

```bash
docker buildx create --name multiarchbuilder
docker buildx use multiarchbuilder
docker buildx inspect --bootstrap
```

Create AMD64 image. You can test this image locally and push it only when it's
ready.

```bash
docker build -t questdb/questdb:<software_version>-linux-amd64 --file Dockerfile-linux .
```

Push this image eventually:

```bash
docker push questdb/questdb:<software_version>-linux-amd64
```

Create ARM64 image.

```bash
docker buildx build --platform linux/arm64 -t questdb/questdb:<software_version>-linux-arm64 --file Dockerfile-linux-arm64 . --load
```

Push that eventually as well:

```bash
docker push questdb/questdb:<software_version>-linux-arm64
```

### Switch Docker Desktop to Windows

Build Windows image. Notice that this build does not use `buildx`. Also make
sure that tag (:<software_version>-windows) reflects QuestDB version.

```bash
docker build -t questdb/questdb:<software_version>-windows-amd64 --file Dockerfile-windows .
```

Push to Docker Hub:

```bash
docker push questdb/questdb:<software_version>-windows-amd64
```

### Create manifest

The purpose of the manifest is to simplify image usage for end users. Hopefully
when they install `questdb/questdb`, Docker Hub will still figure out the
appropriate image for their target platform.

```bash
docker manifest create questdb/questdb:<software_version> questdb/questdb:<software_version>-linux-arm64 questdb/questdb:<software_version>-linux-amd64 questdb/questdb:<software_version>-windows-amd64
```

Push manifest:

```bash
docker manifest push questdb/questdb:<software_version> --purge
```

The `--purge` option is there to delete local Docker manifests. If you do not do
this and find out that you added the wrong image to the manifest, it would be
impossible to take that image out!

## Running QuestDB via Docker

Pull the latest image

```bash
docker pull questdb/questdb
```

To run QuestDB interactively:

```bash
docker run --rm -it -p 9000:9000 -p 8812:8812 questdb/questdb
```

You can stop this container using `Ctrl+C`. The container and all the data is
removed when stopped. A practical process for running QuestDB is to create a
container with a name:

```bash
docker create --name questdb -p 9000:9000 -p 8812:8812 questdb/questdb
```

The container named `questdb` can be started, stopped and the logs can be viewed
using the following commands:

```bash
docker start questdb
docker stop questdb
docker logs questdb
```

QuestDB supports the following volumes:

`/root/.questdb/db` for Linux containers `c:\questdb\db` for Windows containers

You can mount host directories using `-v` option, e.g.

```bash
-v /local/dir:/root/.questdb/db
```

For more details, see the
[official Docker documentation](https://questdb.io/docs/get-started/docker) for
working with QuestDB.
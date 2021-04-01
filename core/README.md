# Building QuestDB

As a first step, clone the repository:

```script
git clone git@github.com:questdb/questdb.git
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

The commands below will create a JAR without assembling executable binaries or
building web console:

```script
cd questdb
mvn clean package -DskipTests
```

To package the web console with the JAR, use the following command:

```script
mvn clean package -DskipTests -P build-web-console
```

To build executable binaries use the following command:

```script
mvn clean package -DskipTests -P build-web-console,build-binaries
```

To run tests, it is not required to have the binaries or web console built.
There are over 4000 tests that should complete without 2-6 minutes depending on
the system:

```script
mvn clean test
```

To release to Maven Central use the following command that activates deploy
profile. Ensure that your `~/.m2/settings.xml` contains username/password for
server `central` and `gnupg` is on hand to sign the artefacts.

```script
mvn -pl !benchmarks clean deploy -DskipTests -P build-web-console,maven-central-release
```

### Run QuestDB

```script
# Create a database root directory and run QuestDB
mkdir <root_directory>
java -p core/target/questdb-5.0.5-SNAPSHOT.jar -m io.questdb/io.questdb.ServerMain -d <root_directory>
```

## Docker images

To build docker images successfully please follow these instructions precisely.
There could be a lot of variations on how these images can be built. This is
what worked:

- Use Windows OS. Docker Desktop might also work on Mac, but not on Linux.
- Download Docker Desktop _Edge_. At the time of writing (Nov 2019) only Edge
  version is able to build ARM64 images and help create multi-platform manifest.
- Ensure "experimental" features are enabled.

To verify that your Docker Desktop is good to go try the following command:

```
docker buildx ls
```

You should see output similar to:

```
C:\Users\blues>docker buildx ls
NAME/NODE DRIVER/ENDPOINT STATUS  PLATFORMS
default * docker
  default default         running linux/amd64, linux/arm64, linux/ppc64le, linux/s390x, linux/386, linux/arm/v7, linux/arm/v6
```

If docker complains on `buildx` command - you either running _stable_ version or
have not enabled experimental features. Look through Docker Settings. There is
JSON config you need to edit **and** toggle a switch in UI

Assuming all is good, lets start building.

### Login to Docker Hub

The following command will prompt you for everything thats required and will
keep your logged in for some time. You don't have to run it if you know you are
logged in.

```
docker login
```

### Switch Docker Desktop to Linux

To do that right click on Docker Desktop tray icon (bottom right) and chose
switch from pop-up menu.

Create new builder

```
docker buildx create --name multiarchbuilder
docker buildx use multiarchbuilder
docker buildx inspect --bootstrap
```

Create AMD64 image. You can test this image locally and push it only when it's
ready.

```
docker build -t questdb/questdb:4.0.0-linux-amd64 --file Dockerfile-linux .
```

Push this image eventually:

```
docker push questdb/questdb:4.0.0-linux-amd64
```

Create ARM64 image.

```
docker buildx build --platform linux/arm64 -t questdb/questdb:4.0.0-linux-arm64 --file Dockerfile-linux-arm64 . --load
```

Push that eventually as well:

```
docker push questdb/questdb:4.0.0-linux-arm64
```

### Switch Docker Desktop to Windows

Build Windows image. Notice that this build does not use `buildx`. Also make
sure that tag (:4.0.0-windows) reflect QuestDB version.

```
docker build -t questdb/questdb:4.0.0-windows-amd64 --file Dockerfile-windows .
```

Push to Docker Hub:

```
docker push questdb/questdb:4.0.0-windows-amd64
```

### Create manifest

The purpose of the manifest is to simplify image usage by end user. They will
just hopefully install `questdb/questdb` and Docker Hub sill figure out
appropriate image for target platform.

```
docker manifest create questdb/questdb:4.0.0 questdb/questdb:4.0.0-linux-arm64 questdb/questdb:4.0.0-linux-amd64 questdb/questdb:4.0.0-windows-amd64
```

Push manifest:

```
docker manifest push questdb/questdb:4.0.0 --purge
```

The --purge option is there to delete manifest on local Docker. If you do not do
it and find out that you added wrong image to manifest, it would be impossible
to take that image out!

## Running QuestDB via Docker

Pull the latest image

```
docker pull questdb/questdb
```

To run QuestDB interactively:

```
docker run --rm -it -p 9000:9000 -p 8812:8812 questdb/questdb
```

You can stop this container using Ctrl+C. The container and all the data is
removed when stopped. A practical process for running QuestDB is to create a
container with a name:

```
docker create --name questdb -p 9000:9000 -p 8812:8812 questdb/questdb
```

The container named `questdb` can be started, stopped and the logs can be viewed
using the following commands:

```
docker start questdb
docker stop questdb
docker logs questdb
```

QuestDB supports the following volumes:

`/root/.questdb/db` for Linux containers `c:\questdb\db` for Windows containers

You can mount host directories using -v option, e.g.

```
-v /local/dir:/root/.questdb/db
```

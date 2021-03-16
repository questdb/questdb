## Building Docker images

To build docker images successfully please follow these instructions precisely. There could be a lot of varions on how these images can be built. This is what worked:

- Use Windows OS. Docker Desktop might also work on Mac, but not on Linux.
- Download Docker Desktop _Edge_. At the time of writing (Nov 2019) only Edge version is able to build ARM63 images and help create multi-platform manifest.
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

If docker complains on `buildx` command - you either running _stable_ version or have not enabled experimental features. Look through Docker Settings. There is JSON config you need to edit __and__ toggle a switch in UI

Assuming all is good, lets start building.

### Login to Docker Hub

The following command will prompt you for everything thats required and will keep your logged in for some time. You don't have to run it if you know you are logged in.
```
docker login
```

### Switch Docker Desktop to Linux

To do that right click on Docker Desktop tray icon (bottom right) and chose switch from pop-up menu.

Create new builder
```
docker buildx create --name multiarchbuilder
docker buildx use multiarchbuilder
docker buildx inspect --bootstrap
```
Create AMD64 image. You can test this image locally and push it only when its ready.
```
docker build -t questdb/questdb:4.0.0-linux-amd64 --file Dockerfile .
```

Push this image eventually:
```
docker push questdb/questdb:4.0.0-linux-amd64
```

Create ARM64 image.
```
docker buildx build --platform linux/arm64 -t questdb/questdb:4.0.0-linux-arm64 --file Dockerfile-arm64 . --load
```

Push that eventually as well:
```
docker push questdb/questdb:4.0.0-linux-arm64
```

### Create manifest

The purpose of the manifest is to simplify image usage by end user. They will just hopefully install `questdb/questdb` and Docker Hub sill figure out appropriate image for target platform.

```
docker manifest create questdb/questdb:4.0.0 questdb/questdb:4.0.0-linux-arm64 questdb/questdb:4.0.0-linux-amd64 questdb/questdb:4.0.0-windows-amd64
```

Push manifest:
```
docker manifest push questdb/questdb:4.0.0 --purge
```
The --purge option is there to delete manifest on local Docker. If you do not do it and find out that you added wrong image to manifest, it would be impossible to take that image out!

## Running QuestDB in Docker Container

Pull image
```
docker pull questdb/questdb:4.0.0
```

To run QuestDB as interactive sandbox:
```
docker run --rm -it -p 9000:9000 -p 8812:8812 questdb/questdb:4.0.0
```
You can Ctrl+C QuestDB. Container and all the data is removed when container stops.

To run QuestDB sensibly, create container without running it:
```
docker create --name questdb -p 9000:9000 -p 8812:8812 questdb/questdb:4.0.0
```

Start QuestDB
```
docker start questdb
```

Stop QuestDB
```
docker stop questdb
```
Check logs
```
docker logs questdb
```

QuestDB supports the following volumes:

`/root/.questdb/db` for Linux containers
`c:\questdb\db` for Windows containers

You can mount host directories using -v option, e.g.
```
-v /local/dir:/root/.questdb/db
```

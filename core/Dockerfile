FROM debian:bullseye

RUN apt-get update \
  && apt-get install git curl wget gnupg2 ca-certificates lsb-release software-properties-common unzip -y

RUN wget -O- https://apt.corretto.aws/corretto.key | gpg --dearmor | tee /etc/apt/trusted.gpg.d/winehq.gpg >/dev/null && \
    add-apt-repository 'deb https://apt.corretto.aws stable main' && \
    apt-get update && \
    apt-get install -y java-17-amazon-corretto-jdk=1:17.0.3.6-1

ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
WORKDIR /build

RUN curl -L "https://www.mirrorservice.org/sites/ftp.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.zip" --output maven.zip \
  && unzip maven.zip \
  && git clone  https://github.com/questdb/questdb.git \
  && cd questdb \
  && git checkout vi_docker_err

WORKDIR /build/questdb

RUN ../apache-maven-3.6.3/bin/mvn clean package -Djdk.lang.Process.launchMechanism=vfork -DskipTests -P build-web-console,build-binaries

WORKDIR /build/questdb/core/target

RUN tar xvfz questdb-*-rt-*.tar.gz

RUN rm questdb-*-rt-*.tar.gz

ENV GOSU_VERSION 1.14
RUN dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')"; \
    wget -O gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch" && \
    wget -O gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch.asc" && \
    export GNUPGHOME="$(mktemp -d)" && \
    gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 && \
    gpg --batch --verify gosu.asc gosu && \
    gpgconf --kill all && \
    rm -rf "$GNUPGHOME" gosu.asc && \
    chmod +x gosu && \
    ./gosu --version && \
    ./gosu nobody true

FROM debian:bullseye-slim

WORKDIR /app

COPY --from=0 /build/questdb/core/target/questdb-*-rt-* .
COPY --from=0 /build/questdb/core/target/gosu /usr/local/bin/gosu

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Create questdb user and group
RUN groupadd -g 10001 questdb && \
    useradd -u 10001 -g 10001 -d /var/lib/questdb -M -s /sbin/nologin questdb && \
    mkdir -p /var/lib/questdb && \
    chown -R questdb:questdb /var/lib/questdb

# Make working folder the quest db folder
WORKDIR /root/.questdb/db

# Make port 9000 available to the world outside this container
EXPOSE 9000/tcp
EXPOSE 8812/tcp
EXPOSE 9009/tcp

#
# Run QuestDB when the container launches
#
# 'conf/log.conf' is a placeholder, it does not exist out of box
# which make logger use default configuration. However, when user configures
# a volume with something like:
#
# docker run -v "$(pwd):/var/lib/questdb/"  questdb/questdb
#
# then one can create 'log.conf' in the 'conf' dir and override logger fully
#
ENTRYPOINT ["/docker-entrypoint.sh"]

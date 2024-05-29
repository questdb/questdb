FROM debian:bookworm AS builder
ARG tag_name

ENV GOSU_VERSION=1.14
ENV JDK_VERSION=17.0.11.9-1

RUN apt-get update \
  && apt-get install --no-install-recommends -y \
  git=1:2.30.2-1 \
  curl=7.74.0-1.3+deb11u2 \
  wget=1.21-1+deb11u1 \
  gnupg2=2.2.27-2+deb11u2 \
  ca-certificates=20210119 \
  lsb-release=11.1.0 \
  software-properties-common=0.96.20.2-2 \
  unzip=6.0-26+deb11u1 \
  && rm -rf /var/lib/apt/lists/*

RUN wget -O - https://apt.corretto.aws/corretto.key | gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" | tee /etc/apt/sources.list.d/corretto.list && \
    apt-get update && \
    apt-get install --no-install-recommends -y java-17-amazon-corretto-jdk=1:${JDK_VERSION} && \
    apt-get -y install maven=3.6.3-5 && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /build

RUN echo tag_name ${tag_name:-master}

RUN git clone --progress --branch "${tag_name:-master}" --verbose https://github.com/questdb/questdb.git

WORKDIR /build/questdb

RUN mvn clean package -Djdk.lang.Process.launchMechanism=vfork -Dmaven.resolver.transport=wagon -Dmaven.wagon.httpconnectionManager.ttlSeconds=30 -DskipTests -P build-web-console,build-binaries

WORKDIR /build/questdb/core/target

RUN tar xvfz questdb-*-rt-*.tar.gz && \
    rm questdb-*-rt-*.tar.gz

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

FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /build/questdb/core/target/questdb-*-rt-* .
COPY --from=builder /build/questdb/core/target/gosu /usr/local/bin/gosu

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Create questdb user and group
RUN groupadd -g 10001 questdb && \
    useradd -u 10001 -g 10001 -d /var/lib/questdb -M -s /sbin/nologin questdb && \
    mkdir -p /var/lib/questdb && \
    chown -R questdb:questdb /var/lib/questdb

WORKDIR /var/lib/questdb

# Make port 9000 available to the world outside this container
EXPOSE 9000/tcp
EXPOSE 8812/tcp
EXPOSE 9009/tcp

# Run QuestDB when the container launches
ENTRYPOINT ["/docker-entrypoint.sh"]

# Ensure the container runs as the questdb user
USER questdb

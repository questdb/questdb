FROM debian:bookworm AS builder
ARG tag_name
ARG GRAAL_JDK_VERSION=17.0.9

ENV GOSU_VERSION=1.19

# Base tools + maven (no Corretto repo anymore)
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
       git curl wget gnupg2 ca-certificates lsb-release software-properties-common unzip maven \
    && rm -rf /var/lib/apt/lists/*

# Install GraalVM Community JDK 17
RUN curl -L -o /tmp/graalvm-ce.tar.gz \
      "https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-${GRAAL_JDK_VERSION}/graalvm-community-jdk-${GRAAL_JDK_VERSION}_linux-x64_bin.tar.gz" \
    && mkdir -p /usr/lib/jvm/graalvm-community-jdk-${GRAAL_JDK_VERSION} \
    && tar -xzf /tmp/graalvm-ce.tar.gz \
         -C /usr/lib/jvm/graalvm-community-jdk-${GRAAL_JDK_VERSION} \
         --strip-components=1 \
    && rm /tmp/graalvm-ce.tar.gz

ENV JAVA_HOME=/usr/lib/jvm/graalvm-community-jdk-${GRAAL_JDK_VERSION}
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /build

RUN echo tag_name ${tag_name:-master}

RUN git clone --depth=1 --progress --branch "${tag_name:-master}" --verbose https://github.com/questdb/questdb.git

WORKDIR /build/questdb

RUN mvn clean package -Djdk.lang.Process.launchMechanism=vfork -Dmaven.resolver.transport=wagon -Dmaven.wagon.httpconnectionManager.ttlSeconds=30 -DskipTests -P build-web-console,build-binaries

WORKDIR /build/questdb/core/target

RUN tar xvfz questdb-*-rt-*.tar.gz

RUN rm questdb-*-rt-*.tar.gz

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

##############################################################################
######################### Build for RedHat OpenShift #########################
##############################################################################

FROM registry.access.redhat.com/ubi9/ubi AS rhel
ARG tag_name

WORKDIR /app

COPY --from=builder /build/questdb/core/target/questdb-*-rt-* .

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Add license information
RUN mkdir /licenses
COPY LICENSE.txt /licenses

LABEL name=QuestDB \
    vendor=QuestDB \
    summary="Provides the latest release of QuestDB" \
    description="QuestDB is an open source time-series database for fast ingest and SQL queries" \
    version=${tag_name:-master} \
    release=${tag_name:-master} \
    maintainer=QuestDB \
    url="https://github.com/questdb/questdb"

# Create questdb user and group
RUN groupadd -g 10001 questdb && \
    useradd -u 10001 -g 10001 -d /var/lib/questdb -M -s /sbin/nologin questdb && \
    mkdir -p /var/lib/questdb && \
    chown -R questdb:questdb /var/lib/questdb

USER questdb

WORKDIR /var/lib/questdb

# Make port 9000 available to the world outside this container
EXPOSE 9000/tcp
EXPOSE 8812/tcp
EXPOSE 9009/tcp

ENV DO_CHOWN="false"
ENTRYPOINT ["/docker-entrypoint.sh"]

##############################################################################
########################## General-use Docker build ##########################
##############################################################################

FROM debian:bookworm-slim AS questdb
ARG tag_name

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

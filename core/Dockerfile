FROM debian:bookworm AS builder

ARG tag_name
ARG GRAAL_JDK_VERSION=17.0.9
ARG TARGETARCH

ENV GOSU_VERSION=1.19-go1.25.12
ENV DEBIAN_FRONTEND=noninteractive

# 1) Base tools + Maven
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
       git curl wget gnupg2 ca-certificates lsb-release software-properties-common unzip maven \
    && rm -rf /var/lib/apt/lists/*

# 2) Download GraalVM CE archive + SHA256, arch-aware
RUN set -eux; \
    : "${TARGETARCH:=amd64}"; \
    case "$TARGETARCH" in \
      amd64) ARCH_SUFFIX="linux-x64" ;; \
      arm64) ARCH_SUFFIX="linux-aarch64" ;; \
      *) echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    echo "Building for TARGETARCH=${TARGETARCH}, using GraalVM suffix ${ARCH_SUFFIX}"; \
    TAG="jdk-${GRAAL_JDK_VERSION}"; \
    FILENAME="graalvm-community-jdk-${GRAAL_JDK_VERSION}_${ARCH_SUFFIX}_bin.tar.gz"; \
    BASE_URL="https://github.com/graalvm/graalvm-ce-builds/releases/download/${TAG}"; \
    mkdir -p /tmp/graalvm; \
    cd /tmp/graalvm; \
    curl -L -o "${FILENAME}" "${BASE_URL}/${FILENAME}"; \
    curl -L -o "${FILENAME}.sha256" "${BASE_URL}/${FILENAME}.sha256"

# 3) Verify SHA256 and install GraalVM CE
RUN set -eux; \
    : "${TARGETARCH:=amd64}"; \
    case "$TARGETARCH" in \
      amd64) ARCH_SUFFIX="linux-x64" ;; \
      arm64) ARCH_SUFFIX="linux-aarch64" ;; \
      *) echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    cd /tmp/graalvm; \
    FILENAME="graalvm-community-jdk-${GRAAL_JDK_VERSION}_${ARCH_SUFFIX}_bin.tar.gz"; \
    # .sha256 contains only the hash, so wrap it with filename for sha256sum -c
    HASH="$(cut -d ' ' -f1 "${FILENAME}.sha256")"; \
    echo "${HASH}  ${FILENAME}" | sha256sum -c -; \
    INSTALL_DIR="/usr/lib/jvm/graalvm-community-jdk-${GRAAL_JDK_VERSION}"; \
    mkdir -p "${INSTALL_DIR}"; \
    tar -xzf "${FILENAME}" -C "${INSTALL_DIR}" --strip-components=1; \
    rm -rf /tmp/graalvm

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
    wget -O "gosu-$dpkgArch" "https://github.com/questdb/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch" && \
    wget -O SHA256SUMS "https://github.com/questdb/gosu/releases/download/$GOSU_VERSION/SHA256SUMS" && \
    grep "gosu-$dpkgArch\$" SHA256SUMS | sha256sum -c - && \
    rm SHA256SUMS && \
    mv "gosu-$dpkgArch" gosu && \
    chmod +x gosu && \
    ./gosu --version && \
    ./gosu nobody true

##############################################################################
######################### Build for RedHat OpenShift #########################
##############################################################################

FROM registry.access.redhat.com/ubi10/ubi-minimal AS rhel
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
RUN microdnf install -y shadow-utils util-linux grep && \
    groupadd -g 10001 questdb && \
    useradd -u 10001 -g 10001 -d /var/lib/questdb -M -s /sbin/nologin questdb && \
    microdnf remove -y shadow-utils && \
    microdnf clean all && \
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

FROM quay.io/fedora/fedora-minimal:44 AS questdb
ARG tag_name

WORKDIR /app

COPY --from=builder /build/questdb/core/target/questdb-*-rt-* .
COPY --from=builder /build/questdb/core/target/gosu /usr/local/bin/gosu

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Create questdb user and group
RUN microdnf install -y shadow-utils util-linux grep && \
    groupadd -g 10001 questdb && \
    useradd -u 10001 -g 10001 -d /var/lib/questdb -M -s /sbin/nologin questdb && \
    microdnf remove -y shadow-utils && \
    microdnf clean all && \
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

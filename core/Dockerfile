FROM debian:bookworm

# Argument for the tag name, default is 'master'
ARG tag_name

# Environment variables for GOSU and JDK versions
ENV GOSU_VERSION=1.16
ENV JDK_VERSION=17.0.11.9-1

# Update and install necessary packages
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        git \
        curl \
        wget \
        gnupg2 \
        ca-certificates \
        lsb-release \
        software-properties-common \
        unzip

# Add Corretto key and repository, then install JDK and Maven
RUN wget -O - https://apt.corretto.aws/corretto.key | gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" | tee /etc/apt/sources.list.d/corretto.list \
    && apt-get update \
    && apt-get install --no-install-recommends -y java-17-amazon-corretto-jdk=1:${JDK_VERSION} \
    && apt-get install --no-install-recommends -y maven

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto

# Set working directory
WORKDIR /build

# Echo the tag name
RUN echo "tag_name ${tag_name:-master}"

# Clone the QuestDB repository
RUN git clone --depth=1 --branch "${tag_name:-master}" https://github.com/questdb/questdb.git

# Set working directory to QuestDB source
WORKDIR /build/questdb

# Build QuestDB using Maven
RUN mvn clean package \
    -Djdk.lang.Process.launchMechanism=vfork \
    -Dmaven.resolver.transport=wagon \
    -Dmaven.wagon.httpconnectionManager.ttlSeconds=30 \
    -DskipTests \
    -P build-web-console,build-binaries

# Set working directory to target directory
WORKDIR /build/questdb/core/target

# Extract the built QuestDB tarball
RUN tar xvfz questdb-*-rt-*.tar.gz \
    && rm questdb-*-rt-*.tar.gz

# Download and verify gosu
RUN dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')" \
    && wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/${GOSU_VERSION}/gosu-${dpkgArch}" \
    && wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/${GOSU_VERSION}/gosu-${dpkgArch}.asc" \
    && export GNUPGHOME="$(mktemp -d)" \
    && gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 \
    && gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu \
    && rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc \
    && chmod +x /usr/local/bin/gosu \
    && gosu --version \
    && gosu nobody true

# Use a smaller base image for the final image
FROM debian:bookworm-slim

# Set working directory
WORKDIR /app

# Copy built QuestDB and gosu from the builder stage
COPY --from=0 /build/questdb/core/target/questdb-*-rt-* .
COPY --from=0 /usr/local/bin/gosu /usr/local/bin/gosu

# Copy entrypoint script
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Create questdb user and group, and set permissions
RUN groupadd -g 10001 questdb \
    && useradd -u 10001 -g 10001 -d /var/lib/questdb -M -s /usr/sbin/nologin questdb \
    && mkdir -p /var/lib/questdb \
    && chown -R questdb:questdb /var/lib/questdb

# Set working directory for questdb
WORKDIR /var/lib/questdb

# Expose necessary ports
EXPOSE 9000/tcp
EXPOSE 8812/tcp
EXPOSE 9009/tcp

# Set entrypoint for the container
ENTRYPOINT ["/docker-entrypoint.sh"]

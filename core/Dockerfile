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
  && git clone --depth=1 --progress --verbose https://github.com/questdb/questdb.git

WORKDIR /build/questdb

RUN ../apache-maven-3.6.3/bin/mvn clean package -Djdk.lang.Process.launchMechanism=vfork -DskipTests -P build-web-console,build-binaries

WORKDIR /build/questdb/core/target

RUN tar xvfz questdb-*-rt-*.tar.gz

RUN rm questdb-*-rt-*.tar.gz

FROM debian:bullseye-slim

WORKDIR /app

COPY --from=0 /build/questdb/core/target/questdb-*-rt-* .

WORKDIR /app/bin

# QuestDB root directory
RUN mkdir -p /root/.questdb

# Make working folder the quest db folder
WORKDIR /root/.questdb

# bash prompt
RUN echo "PS1='🐳  \[\033[1;36m\]\h \[\033[1;34m\]\W\[\033[0;35m\] \[\033[1;36m\]# \[\033[0m\]'" > /root/.bashrc

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
# docker run -v "$(pwd):/root/.questdb/"  questdb/questdb
#
# then one can create 'log.conf' in the 'conf' dir and override logger fully
#
CMD ["/usr/bin/env", "QDB_PACKAGE=docker", "/app/bin/java", "-Dout=conf/log.conf", "-m", "io.questdb/io.questdb.ServerMain", "-d", "/root/.questdb", "-f"]

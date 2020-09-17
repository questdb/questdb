FROM amazonlinux:2

ENV MVN_VERSION="3.6.3"
ENV MVN="apache-maven-$MVN_VERSION"
ENV JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto.x86_64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PATH="/mvn/$MVN/bin":$PATH

WORKDIR /mvn

RUN \
  yum update -y -q \
  && yum install -y -q \
  java-11-amazon-corretto-headless \
  git \
  tar \
  wget \
  && wget -q http://apache.mirror.anlx.net/maven/maven-3/"$MVN_VERSION"/binaries/"$MVN"-bin.tar.gz \
  && tar xf "$MVN"-bin.tar.gz

WORKDIR /app

RUN \
  git clone --depth 1 https://github.com/questdb/questdb.git "$(pwd)" \
  && mvn clean package -Dprofile.install-local-nodejs -DskipTests

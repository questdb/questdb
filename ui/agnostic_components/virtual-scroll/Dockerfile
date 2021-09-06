ARG REPOSITORY_HOST
FROM ${REPOSITORY_HOST}node:slim as base
ARG TARGETDIR=/usr/src
RUN apt-get update && apt-get install -y git
RUN apt-get update --fix-missing && apt-get -y upgrade && apt-get install -y wget gnupg2
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list' && apt-get update && apt-get install -y google-chrome-unstable --no-install-recommends && rm -rf /var/lib/apt/lists/* && rm -rf /src/*.deb

ADD https://github.com/Yelp/dumb-init/releases/download/v1.2.0/dumb-init_1.2.0_amd64 /usr/local/bin/dumb-init
RUN chmod +x /usr/local/bin/dumb-init
RUN groupadd --system chrome && useradd --system --create-home --gid chrome --groups audio,video chrome && mkdir --parents /home/chrome/reports && chown --recursive chrome:chrome /home/chrome

COPY ./.yarnrc ${TARGETDIR}/.yarnrc
RUN npm config set unsafe-perm true
RUN npm i -g typescript es-dev-server mocha prettier eslint karma rollup rimraf
# COPY . $TARGETDIR deprecated
WORKDIR $TARGETDIR
RUN yarn

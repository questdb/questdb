FROM node:12-buster-slim
RUN npm install -g create-react-app typescript webpack-dev-server webpack-cli
COPY ./package.json /usr/src/package.json
COPY ./package-lock.json /usr/src/package-lock.json
COPY ./.yarnrc /usr/src/.yarnrc
WORKDIR /usr/src
RUN npm i

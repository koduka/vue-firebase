FROM node:lts-alpine3.14

RUN apk add --no-cache openjdk11-jre-headless bash
RUN npm install -g firebase-tools

WORKDIR /firebase

ENTRYPOINT [ "firebase" ]
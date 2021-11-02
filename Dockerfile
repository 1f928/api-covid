FROM node:16-alpine3.14

WORKDIR /opt/api-covid
COPY . .
RUN npm ci
RUN apk update && apk add bash

EXPOSE 3100

CMD npm start

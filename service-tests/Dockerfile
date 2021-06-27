FROM node:14-alpine

WORKDIR /usr/src/app

COPY package.json tsconfig.json /usr/src/app/
COPY node_modules /usr/src/app/node_modules
COPY src /usr/src/app/src

CMD ["node_modules/.bin/jest", "--verbose", "--runInBand", "--bail"]

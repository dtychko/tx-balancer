FROM node:14-alpine

ARG NODE_ENV
ENV NODE_ENV $NODE_ENV

USER node
WORKDIR /usr/src/app

COPY --chown=node:node node_modules /usr/src/app/node_modules
COPY --chown=node:node bin /usr/src/app/bin

CMD ["node", "bin"]

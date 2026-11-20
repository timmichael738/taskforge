# syntax=docker/dockerfile:1
# Runs the interactive demo (workers + dashboard). The library itself is consumed via npm.
FROM node:24-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src ./src
COPY examples ./examples
COPY migrations ./migrations
COPY public ./public
USER node
EXPOSE 3000
CMD ["npx", "tsx", "examples/demo.ts"]

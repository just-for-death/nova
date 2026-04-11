FROM node:20-alpine

LABEL org.opencontainers.image.source=https://github.com/just-for-death/nova
LABEL org.opencontainers.image.description="Nova File Manager"
RUN apk add --no-cache rsync bash util-linux coreutils findutils unzip build-base python3

WORKDIR /app

COPY package*.json package-lock.json ./
RUN npm install --production
RUN mkdir -p ./public

# Copy UI and all static assets (favicon, manifest, logos)
COPY index.html login.html ./
COPY public/ ./public/
COPY server.js ./

EXPOSE 9898

ENV PORT=9898 \
    NODE_ENV=production

CMD ["node", "server.js"]

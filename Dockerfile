FROM node:20-alpine

RUN apk add --no-cache rsync bash util-linux coreutils findutils

WORKDIR /app

COPY package.json ./
RUN npm install --production

COPY server.js ./
RUN mkdir -p ./public

# Copy UI and all static assets (favicon, manifest, logos)
COPY index.html ./public/
COPY public/ ./public/

EXPOSE 9898

ENV PORT=9898 \
    NODE_ENV=production

CMD ["node", "server.js"]

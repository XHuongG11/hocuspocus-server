# --- Build stage ---
FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json yarn.lock* ./
RUN yarn install --frozen-lockfile

COPY tsconfig.json ./
COPY src ./src

RUN yarn build

# --- Production stage ---
FROM node:20-alpine

WORKDIR /app

COPY package.json yarn.lock* ./
RUN yarn install --frozen-lockfile --production

COPY --from=builder /app/dist ./dist

ENV MONGODB_URL=mongodb://localhost:27017/roadmap_service
ENV REDIS_HOST=localhost
ENV REDIS_PORT=6382
ENV HOCUSPOCUS_PORT=1234

EXPOSE 1234

CMD ["node", "dist/server.js"]

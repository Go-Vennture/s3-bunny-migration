FROM node:24-bookworm-slim AS build

WORKDIR /app

COPY package*.json tsconfig.json ./
COPY src ./src
RUN npm ci && npm run build

FROM node:24-bookworm-slim AS runtime

WORKDIR /app
ENV NODE_ENV=production
ENV PORT=80
ENV HOST=0.0.0.0
LABEL org.opencontainers.image.source="https://github.com/Go-Vennture/s3-bunny-migration"
LABEL org.opencontainers.image.title="s3-bunny-migration"
LABEL org.opencontainers.image.description="Single-process Node app for moving files between Amazon S3 buckets and Bunny Storage zones."

COPY package*.json ./ 
RUN npm ci --omit=dev

COPY --from=build /app/dist ./dist

EXPOSE 80

CMD ["node", "dist/index.js"]

FROM node:24-bookworm-slim AS build

WORKDIR /app

COPY package*.json tsconfig.json ./
COPY src ./src
RUN npm ci && npm run build

FROM node:24-bookworm-slim AS runtime

WORKDIR /app
ENV NODE_ENV=production
ENV PORT=8787
ENV HOST=0.0.0.0

COPY package*.json ./
RUN npm ci --omit=dev

COPY --from=build /app/dist ./dist

EXPOSE 8787

CMD ["node", "dist/index.js"]

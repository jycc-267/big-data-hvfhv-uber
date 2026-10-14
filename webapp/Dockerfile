FROM node:20-alpine

WORKDIR /app
ENV NODE_ENV=production
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY app.js ./
COPY lib ./lib
COPY views ./views
COPY public ./public

USER node
EXPOSE 3012
ENV PORT=3012
CMD ["node", "app.js"]

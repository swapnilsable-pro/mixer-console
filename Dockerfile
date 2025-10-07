FROM node:24 AS builder

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm install
COPY . .

FROM node:24-alpine

WORKDIR /app
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app .

EXPOSE 4000
CMD [ "npm", "start" ]
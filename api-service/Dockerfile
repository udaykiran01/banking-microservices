FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY --chown=node:node . .
RUN mkdir -p /app/logs && chown -R node:node /app
USER node

EXPOSE 3000

CMD ["npm", "start"]

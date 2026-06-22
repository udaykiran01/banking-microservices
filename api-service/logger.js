const winston = require("winston");
const fs = require("fs");

const logDir = "/app/logs";

if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir, { recursive: true });
}

const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [
    new winston.transports.File({
      filename: `${logDir}/app.log`
    }),
    new winston.transports.Console()
  ]
});

module.exports = logger;
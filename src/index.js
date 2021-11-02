
const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const pino = require('pino');
const pinoExpress = require('express-pino-logger');

const appRouter = require('./routes');
const loadCovidData = require('./services/covidData');

const desiredConfig = {
  appName: "APP_NAME",
  appPort: "APP_PORT",
  logLevel: "LOG_LEVEL"
}

const config = require('./config')(desiredConfig);
const logger = pino({ level: config.logLevel });
const app = express();

app.use(helmet());
app.use(cors({ origin: ["*"] }));
app.options('*', cors());
app.use(pinoExpress({ logger: logger }));

app.use(appRouter);

const server = app.listen(config.appPort, () => {
  logger.info(`${config.appName} listening on port ${config.appPort}`);
});

loadCovidData();

// TODO: A more reliable way to execute code at a certain time.
//
// I think nytimes updates data @ midnight, but I'm not sure - so I'll
// set it to 1am for now, to be safe.
const refreshHour = 1;
const interval = 60 * 60 * 1000;
const intervalRef = setInterval(() => {
  const date = new Date();
  const hour = date.getHours();

  if (hour === refreshHour) {
    logger.info('Loading CovidData from interval');
    loadCovidData;
  }
}, interval);

// Does this work?
process.on('SIGTERM', () => {
  logger.info(`SIGTERM signal received - closing Express server`);
  server.close(() => {
    clearInterval(intervalRef);
    logger.info(`Express server closed`);
  });
});

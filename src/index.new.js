
const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const pino = require('pino');
const pinoExpress = require('express-pino-logger');

const covidRouter = require('./routes');
const { loadConfigFromEnv } = require('./util/configManager');

const { loadCovidData } = require('./services/covidData');
const desiredConfig = require('./config');

// --

const config = await loadConfigFromEnv(desiredConfig);
const logger = pino();

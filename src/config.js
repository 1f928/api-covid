const logger = require('pino')();
const dotenv = require('dotenv');

// ---

let config;

// `desiredConfig` is only useful on initialization of singleton
// Expects an object that looks like the desired configuration object,
// except with the expected (names) => (name) => process.env[name] instead
// of the actual values.
//
// E.g.: { database: { url: "DATABASE_URL", auth: "DATABASE_AUTH" }}
// 
// And will return an object that has the env-names replaced with the actual
// values or null (& log fatal message) if no matching env variable was found
function initConfig(desiredConfig) {
  dotenv.config();

  desiredConfig = desiredConfig || {};
  config = getValuesFromEnv(desiredConfig);
  logger.info('Loaded config from environment');
  
  return config;
}

// Recursive
function getValuesFromEnv(obj) {
  const values = {};

  Object.entries(obj).forEach(([key, value]) => {
    if (typeof(value) === "string") {
      values[key] = loadEnvVar(value);
    } else {
      values[key] = getValuesFromEnv(value);
    }
  });

  return values;
}

function loadEnvVar(valueName) {
  if (!(valueName in process.env)) {
    logger.fatal(`CONFIG NOT FOUND: ${valueName} not found in process.env`);
  } else {
    return process.env[valueName];
  }
}

module.exports = initConfig;
module.exports.getConfig = () => config;

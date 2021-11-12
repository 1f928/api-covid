
const express = require('express');
const compression = require('compression');

const getCovidData = require('../services/covidData');
const country = require('./country');
const state = require('./state');
const county = require('./county');
const { filterGroups } = require('../util/data');

const router = express.Router();
router.use(compression());

// router.use('/country', country);
// router.use('/state', state);
// router.use('/county', county);

// --- Default to STL area data

const states = [
  { state: "Missouri" }
];
const counties = [
  { state: "Missouri", county: "St. Louis" },
  { state: "Missouri", county: "St. Louis city" },
  { state: "Missouri", county: "St. Charles" },

  { state: "Illinois", county: "St. Clair" },
  { state: "Illinois", county: "Madison" },
];

router.use('/', async (req, res) => {
  const rawData = await getCovidData();
  if (!rawData || !rawData.countryData || !rawData.stateData || !rawData.countyData) {
    pino.info('No covid data currently loaded to send, sending');
    res.send({});
    return
  }
  const data = {
    countries: rawData.countryData,
    states: filterGroups(states)(rawData.stateData),
    counties: filterGroups(counties)(rawData.countyData)
  };
  res.send(data);
});

module.exports = router;

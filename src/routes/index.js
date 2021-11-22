
const express = require('express');
const compression = require('compression');

const getCovidData = require('../services/covidData');
// const country = require('./country');
// const state = require('./state');
// const county = require('./county');
// const { filterGroups } = require('../util/data');

const router = express.Router();
router.use(compression());

// router.use('/country', country);
// router.use('/state', state);
// router.use('/county', county);

router.use('/', async (req, res) => {
  const rawData = await getCovidData();
  if (!rawData || !rawData.countryData || !rawData.stateData || !rawData.countyData) {
    pino.info('No covid data currently loaded to send, sending');
    res.send({});
    return
  }
  const data = {
    countries: rawData.countryData,
    states: rawData.stateData,
    counties: rawData.countyData
  };
  res.send(data);
});

module.exports = router;

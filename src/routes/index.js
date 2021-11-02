
const express = require('express');

const getCovidData = require('../services/covidData');
// const country = require('./country');
// const state = require('./state');
// const county = require('./county');

const router = express.Router();

// router.use('/country', country);
// router.use('/state', state);
// router.use('/county', county);

// Actually, scrap the above idea - that's fancier than I need.
// All I want for now is an all-purpose interface to get data
// for USA, MO, and the 5 counties in the immediate area -
// (St. Louis City, St. Louis County, St. Charles, Madison [IL],
// and St. Clair [IL])
router.use('/', async (req, res) => {
  const data = await getCovidData();
  res.send(data);
});

module.exports = router;

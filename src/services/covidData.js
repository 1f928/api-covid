
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const pino = require('pino')();

const { initialValue } = require('../util/pipe');
const {
  leftJoin,
  renameColumns,
  addColumn,
  filterAndCastColumns,
  groupBy
} = require('../util/data');


// --- Data manipulation helpers

const downloadFile = async (fileName, endpoint) => {
  const getUrl = `${endpoint}/${fileName}`
  try {
    const response = await axios.get(getUrl);
    return response.data || {};
  } catch (err) {
    pino.error(`Error downloading data from ${getUrl}`);
    pino.error(err)
    return {};
  }
};

// Converts CSV file to array of objects with key-value pairs where the key
// is the associated column header and the value is the column value.
const csvToRowObjects = (csv) => {
  const rows = csv.split('\n').map((row) => row.split(','));
  const columnTitles = rows.shift();

  return rows.map((row) => {
    return row.reduce((obj, columnValue, index) => {
      const column = columnTitles[index];
      obj[column] = columnValue;
      return obj;
    }, {});
  });
};

// --- Data managing

let data;
let isRunning = false;
const dataFilePath = path.join(__dirname, '../../data/covid-19-data.json')

const loadDataFromGithub = async () => {
  if (isRunning) {
    pino.info("covidData.loadDataFromGithub called while already in progress, ignoring second call");
    return
  }
  isRunning = true;

  // Helpers
  const baseEndpoint = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master';
  const raEndpoint = `${baseEndpoint}/rolling-averages`;
  const countryFile = 'us.csv';
  const stateFile = 'us-states.csv';
  const countyFile = 'us-counties-recent.csv';

  const getBaseData = async (fileName) => 
    csvToRowObjects(await downloadFile(fileName, baseEndpoint));

  const getAverageData = async (fileName) =>
    initialValue(
      csvToRowObjects(await downloadFile(fileName, raEndpoint))
    ).pipe(
      renameColumns({
        "cases": "new_cases",
        "deaths": "new_deaths"
      })
    );

  const getData = async (fileName, joinColumns) => {
    const data = leftJoin(
      await getBaseData(fileName),
      await getAverageData(fileName),
      joinColumns
    );
    return data
  };

  // Data gathering and piping
  const baseColumns = {
    "date": String,

    "cases": Number,
    "new_cases": Number,
    "cases_avg": Number,
    "cases_avg_per_100k": Number,
    "deaths": Number,
    "new_deaths": Number,
    "deaths_avg": Number,
    "deaths_avg_per_100k": Number
  };

  const countryColumns = {...baseColumns};
  const countryJoin = ["date"];
  const countryData = initialValue(await getData(countryFile, countryJoin)).pipe(
    filterAndCastColumns(countryColumns),
    addColumn("country", "USA"),
    groupBy(["country"])
  );

  const stateColumns = {...baseColumns, "state": String};
  const stateJoin = ["date", "state"];
  const stateData = initialValue(await getData(stateFile, stateJoin)).pipe(
    filterAndCastColumns(stateColumns),
    groupBy(["state"])
  );

  const countyColumns = {...stateColumns, "county": String};
  const countyJoin = ["date", "state", "county"];
  const countyData = initialValue(await getData(countyFile, countyJoin)).pipe(
    filterAndCastColumns(countyColumns),
    groupBy(["county", "state"])
  );

  // Data persistence

  data = {
    timestamp: Date.now(),
    data: {
      countryData,
      stateData,
      countyData
    }
  };
  pino.info("Loaded COVID data from GitHub")
  
  try {
    fs.writeFileSync(
      dataFilePath,
      JSON.stringify(data)
    );
    pino.info("Saved COVID data to file")
  } catch (err) {
    pino.error(`Failed to save data to file: ${dataFilePath}`);
    pino.error(err);
  } finally {
    isRunning = false;
  }
};

const loadDataFromFile = () => {
  try {
    if (fs.existsSync(dataFilePath)) {
      const jsonData = JSON.parse(fs.readFileSync(dataFilePath));
      data = jsonData;
      pino.info("Loaded COVID data from file")
      return true
    } else {
      return false
    }
  } catch (err) {
    pino.error(`Failed to load data from file: ${dataFilePath}`);
    pino.error(err);
  }
};

const getData = async () => {
  // First, see if data is loaded in-memory
  if (!data) {
    // If not, load into memory from file
    pino.info('COVID data not found in memory, attempting to load from file...');
    if (!loadDataFromFile()) {
      // If file is not there, load from raw.github (original source)
      pino.info('COVID data not found on file, attempting to load from GitHub...');
      await loadDataFromGithub();
    }
  }

  // Validate that data is not stale - this should never be an issue, given
  // the interval job to refresh data daily, but backup check anyways.
  const maxAge = 24 * 60 * 60 * 1000; // hr * min/hr * sec/min * ms/sec
  if (Date.now() - data.timestamp > maxAge) {
    pino.info('Data has reached max age, refreshing from source');
    loadDataFromGithub(); // Don't await - it'll give one request stale data, but it won't take minutes
  }

  return data.data;
};

module.exports = getData;

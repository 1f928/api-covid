
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const pino = require('pino')();

const pipe = require('../util/pipe');


// --- Data manipulation helpers

const downloadFile = async (fileName, endpoint) => {
  const response = await axios.get(`${endpoint}/${fileName}`);
  return response.data;
};

// Converts CSV file to array of objects with key-value pairs where the key
// is the associated column header and the value is the column value.
const csvToRowObjects = (csv) => {
  const rows = csv.split('\n').map((row) => row.split(','));
  const columnTitles = rows.shift();

  return rows.map((row) => {
    return row.reduce((obj, columnValue, index) => {
      const column = columnTitles[index];
      const numColumns = ["cases", "deaths"]
      obj[column] = (numColumns.includes(column)) ? parseInt(columnValue) : columnValue;
      return obj;
    }, {});
  });
};

const select = (key, values) => (rows) => rows.filter(
  (row) => values.includes(row[key])
);

const counties = {
  MO: [
    "St. Louis",
    "St. Louis city",
    "St. Charles"
  ],
  IL: [
    "Madison",
    "St. Clair"
  ]
};
const selectCounties = (rows) => rows.filter(
  (row) => 
    (row.state === "Missouri" && counties.MO.includes(row.county)) ||
    (row.state === "Illinois" && counties.IL.includes(row.county))
);

const remove = (key) => (rows) => rows.map((row) => { delete row[key]; return row });

const groupBy = (groupKey) => (rows) => rows.reduce((obj, row) => ({
  ...obj,
  [row[groupKey]]: [...(obj[row[groupKey]] || []), row]
}), {});

const addDeltas = (rows) => rows.map((row, index, arr) => {
  if (index === 0) return { ...row, dCases: 0, dDeaths: 0 }
  else {
    const prev = arr[index - 1];
    return { 
      ...row,
      dCases: row.cases - prev.cases,
      dDeaths: row.deaths - prev.deaths
    }
  }
});

const addCountyDeltas = (counties) => Object.entries(counties).reduce(
  (obj, [county, data]) => ({
    ...obj,
    [county]: addDeltas(data)
  }), {}
);


// --- Data managing

let data;
const dataFilePath = path.join(__dirname, '../../data/covid-19-data.json')

const loadDataFromGithub = async () => {
  const rawEndpoint = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master';
  const countryFile = 'us.csv';
  const stateFile = 'us-states.csv';
  const countyFile = 'us-counties-recent.csv';

  const countryData = pipe(
    csvToRowObjects,
    addDeltas
  )(await downloadFile(countryFile, rawEndpoint));

  const stateData = pipe(
    csvToRowObjects,
    select("state", ["Missouri"]),
    remove("fips"),
    addDeltas
  )(await downloadFile(stateFile, rawEndpoint));

  const countyData = pipe(
    csvToRowObjects,
    selectCounties,
    remove("fips"),
    groupBy("county"),
    addCountyDeltas
  )(await downloadFile(countyFile, rawEndpoint));

  data = {
    timestamp: Date.now(),
    data: {
      countryData,
      stateData,
      countyData
    }
  };
  
  try {
    fs.writeFileSync(
      dataFilePath,
      JSON.stringify(data)
    );
  } catch (err) {
    pino.error(`Failed to save data to file: ${dataFilePath}`);
    pino.error(err);
  }
};

const loadDataFromFile = () => {
  try {
    if (fs.existsSync(dataFilePath)) {
      const jsonData = JSON.parse(fs.readFileSync(dataFilePath));
      data = jsonData;
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
    pino.info('Covid data not found in memory, attempting to load from file...');
    if (!loadDataFromFile()) {
      // If file is not there, load from raw.github (original source)
      pino.info('Covid data not found on file, attempting to load from github...');
      await loadDataFromGithub();
    }
  }

  // Validate that data is not stale - this should never be an issue, given
  // the interval job to refresh data daily, but backup check anyways.
  const maxAge = 24 * 60 * 60 * 1000; // hr * min/hr * sec/min * ms/sec
  if (Date.now() - data.timestamp > maxAge) {
    pino.info('Data has reached max age, refreshing from source');
    loadDataFromGithub();
  }

  return data.data;
};

module.exports = getData;

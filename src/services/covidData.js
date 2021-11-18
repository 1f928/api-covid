
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
  groupBy,
  forEachGroup
} = require('../util/data');


// --- Data manipulation helpers

const downloadFile = async (fileName, endpoint) => {
  const getUrl = `${endpoint}/${fileName}`
  console.log(`Getting: ${getUrl}`)
  try {
    const response = await axios.get(getUrl);
    console.log(`Got: ${getUrl}`)
    return response.data || {};
  } catch (err) {
    pino.error(`Error downloading data from ${getUrl}`);
    pino.error(err)
    return {};
  }
};

const pipelog = (rows) => { console.log(rows); return rows };

// Converts CSV file to array of objects with key-value pairs where the key
// is the associated column header and the value is the column value.
//
// Also, moved a lot of login into this function for the sake of speed and
// memory consumption reduction. Not as easy to use, but more useful.
const csvToRows = ({
  csv = "",
  columns = {},
  renames = {},
  rowFilter = null
}) => {
  const rows = csv.split('\n').map((row) => row.split(','));
  const columnTitles = rows.shift();
  const renamedTitles = columnTitles.map((title) =>
    renames[title] ? renames[title] : title
  );

  return rows.map((row, i) => {
    const formattedRow = {}
    row.forEach((columnValue, index) => {
      const column = renamedTitles[index];
      if (columns[column]) formattedRow[column] = columns[column](columnValue);
    });
    return formattedRow;
  }).filter((row) => rowFilter ? rowFilter(row) : true);
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
  const countyFile = 'us-counties.csv';

  const getCaseData = async (fileName, columns, joinColumns, rowFilter) => {
    const data = leftJoin(
      csvToRows({
        csv: await downloadFile(fileName, baseEndpoint),
        columns: columns,
        rowFilter: rowFilter
      }),
      csvToRows({
        csv: await downloadFile(fileName, raEndpoint),
        columns: columns,
        renames: {
          "cases": "new_cases",
          "deaths": "new_deaths"
        },
        rowFilter: rowFilter
      }),
      joinColumns
    );
    console.log(`Finished ${fileName}`)
    return data
  };

  // Data gathering and piping

  // Virus data:
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
  const countryData = initialValue(
    await getCaseData(countryFile, countryColumns, countryJoin)
  ).pipe(
    addColumn("country", "USA"),
    groupBy(["country"])
  );

  const stateColumns = {...baseColumns, "state": String};
  const stateJoin = ["date", "state"];
  const stateRowFilter = (row) => row.state && row.state === "Missouri";
  const stateData = initialValue(
    await getCaseData(stateFile, stateColumns, stateJoin, stateRowFilter)
  ).pipe(
    groupBy(["state"])
  );

  const countyColumns = {...stateColumns, "county": String};
  const countyJoin = ["date", "state", "county"];
  const countyRowFilter = (row) => row.state && (row.state === "Missouri" || row.state === "Illinois");
  const countyData = initialValue(
    await getCaseData(countyFile, countyColumns, countyJoin, countyRowFilter)
  ).pipe(
    groupBy(["county", "state"])
  );
  
  // Vaccination data:
  const countyVaccData = csvToRows({
    csv: await downloadFile(
      'data_county_timeseries.csv',
      'https://raw.githubusercontent.com/bansallab/vaccinetracking/main/vacc_data'
    ),
    columns: {
      "date": String,
      "state": String,
      "county": (countyName) => String(countyName).split(' County')[0],
      "type": String,
      "count": Number
    },
    renames: {
      "STATE_NAME": "state",
      "COUNTY_NAME": "county",
      "DATE": "date",
      "CASE_TYPE": "type",
      "CASES": "count"
    },
    rowFilter: countyRowFilter
  });


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

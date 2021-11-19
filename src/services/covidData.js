
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const pino = require('pino')();

const { initialValue } = require('../util/pipe');
const {
  multiJoin,
  addColumn,
  groupBy,
  forEachGroup
} = require('../util/data');
const stateNames = require('../util/states');


// --- Data manipulation helpers

const downloadFile = async (getUrl) => {
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

const stateRowFilter = (row) => row.state && row.state === "Missouri";
const countyRowFilter = (row) => row.state && (row.state === "Missouri" || row.state === "Illinois");

const calcPopulation = (numerator, denominator) => (rows) => {
  const last = rows.pop();
  const population = Math.floor(last[numerator] / last[denominator]);
  rows.forEach((row) => row.pop = population);
  return rows;
}

const patchVaccValues = (rows) => {
  rows.forEach((row, i) => {
    if (!row.pvacc || row.pvacc === 0) {
      (i === 0) ? row.pvacc = 0 : row.pvacc = rows[i - 1].pvacc;
    }
    if (!row.fvacc || row.fvacc === 0) {
      (i === 0) ? row.fvacc = 0 : row.fvacc = rows[i - 1].fvacc;
    };
  });
  return rows;
};

const calcVaccPercents = (rows) => rows.map((row) => ({
  ...row,
  pvacc_pct: (row.pvacc / row.pop) * 100,
  fvacc_pct: (row.fvacc / row.pop) * 100
}));

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

  const getBaseCovidData = async (fileName, columns, rowFilter) => csvToRows({
    csv: await downloadFile(`${baseEndpoint}/${fileName}`),
    columns: columns,
    rowFilter: rowFilter
  });

  const getAvgCovidData = async (fileName, columns, rowFilter) => csvToRows({
    csv: await downloadFile(`${raEndpoint}/${fileName}`),
    columns: columns,
    renames: {
      "cases": "new_cases",
      "deaths": "new_deaths"
    },
    rowFilter: rowFilter
  });

  // Data gathering and piping
  
  // Vaccination data:
  const addVaccPcts = (rows) => rows.map((row) => {
    const total = row.pvacc + row.fvacc;
    return {
      ...row,
      "pvacc_pct": row.pvacc / total,
      "fvacc_pct": row.fvacc / total
    };
  });

  const getCountryVaccData = async () => csvToRows({
    csv: await downloadFile('https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/country_data/United%20States.csv'),
    renames: {
      "people_vaccinated": "pvacc",
      "people_fully_vaccinated": "fvacc"
    },
    columns: {
      "date": String,
      "pvacc": Number,
      "fvacc": Number
    }
  });

  const getStateVaccData = async () => csvToRows({
    csv: await downloadFile('https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/us_state_vaccinations.csv'),
    renames: {
      "location": "state",
      "people_vaccinated": "pvacc",
      "people_fully_vaccinated": "fvacc",
      "people_vaccinated_per_hundred": "pvacc_pct"
    },
    columns: {
      "date": String,
      "state": String,
      "pvacc": Number,
      "fvacc": Number,
      "pvacc_pct": (n) => Number(n) / 100
    },
    rowFilter: stateRowFilter
  });

  const getCountyVaccData = async () => {
    const typeToShort = {
      "Partial": "pvacc",
      "Complete": "fvacc",
      "Partial Coverage": "pvacc_pct",
      "Complete Coverage": "fvacc_pct"
    };

    const combineTypes = (groups) => groups.map((group) => ({
      ...group.keys,
      ...group.rows.reduce((obj, row) => ({...obj, [row.type]: row.count}), {}),
      pop: group.rows[0].pop
    }));

    return initialValue(
      csvToRows({
        csv: await downloadFile(
          'https://raw.githubusercontent.com/bansallab/vaccinetracking/main/vacc_data/data_county_timeseries.csv'
        ),
        renames: {
          "STATE_NAME": "state",
          "COUNTY_NAME": "county",
          "DATE": "date",
          "CASE_TYPE": "type",
          "CASES": "count",
          "POPN": "pop"
        },
        columns: {
          "date": String,
          "state": (abbr) => stateNames[abbr] ? stateNames[abbr] : null,
          "county": (countyName) => countyName.split(' County')[0],
          "type": (type) => typeToShort[type] ? typeToShort[type] : null,
          "count": Number,
          "pop": Number
        },
        rowFilter: countyRowFilter
      })
    ).pipe(
      groupBy(["state", "county", "date"]),
      combineTypes
    )
  };

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
  const countryCovidFile = "us.csv";

  const countryData = initialValue(multiJoin(
    countryJoin,
    await getBaseCovidData(countryCovidFile, countryColumns),
    await getAvgCovidData(countryCovidFile, countryColumns),
    await getCountryVaccData()
  )).pipe(
    addColumn("country", "USA"),
    addColumn("pop", 329_500_000), // Ehh, heh. Dataset didn't have the means to calc
    groupBy(["country"]),
    forEachGroup(patchVaccValues),
    forEachGroup(calcVaccPercents)
  );

  const stateColumns = {...baseColumns, "state": String};
  const stateJoin = ["date", "state"];
  const stateCovidFile = 'us-states.csv';


  const stateData = initialValue(multiJoin(
    stateJoin,
    await getBaseCovidData(stateCovidFile, stateColumns, stateRowFilter),
    await getAvgCovidData(stateCovidFile, stateColumns, stateRowFilter),
    await getStateVaccData()
  )).pipe(
    groupBy(["state"]),
    forEachGroup(calcPopulation("pvacc", "pvacc_pct")),
    forEachGroup(patchVaccValues),
    forEachGroup(calcVaccPercents)
  );

  const countyColumns = {...stateColumns, "county": String};
  const countyJoin = ["date", "state", "county"];
  const countyCovidFile = 'us-counties.csv';

  const countyData = initialValue(multiJoin(
    countyJoin,
    await getBaseCovidData(countyCovidFile, countyColumns, countyRowFilter),
    await getAvgCovidData(countyCovidFile, countyColumns, countyRowFilter),
    await getCountyVaccData()
  )).pipe(
    groupBy(["county", "state"]),
    forEachGroup(patchVaccValues),
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

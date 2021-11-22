
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const pino = require('pino')();

const { initialValue } = require('../util/pipe');
const {
  multiJoin,
  addColumn,
  groupBy,
  forEachGroup,
  filterGroups
} = require('../util/data');
const stateNames = require('../util/states');
const sum = require ('../util/sum');


// --- Data manipulation helpers

const downloadFile = async (getUrl) => {
  pino.info(`Getting: ${getUrl}`)
  try {
    const response = await axios.get(getUrl);
    pino.info(`Got: ${getUrl}`)
    return response.data || {};
  } catch (err) {
    pino.error(`Error downloading data from ${getUrl}`);
    pino.error(err)
    return {};
  }
};

const pipelog = (rows) => { console.log(rows.slice(-10)); return rows };

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
  })
  .filter((row) => rowFilter ? rowFilter(row) : true)
  .filter((row) => row.date ? true : false);
};

// --- Data managing

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

const stateRowFilter = (row) => row.state && row.state === "Missouri";
const countyRowFilter = (row) => row.state && (row.state === "Missouri" || row.state === "Illinois");

const calcPopulation = (numerator, denominator) => (rows) => {
  const last = rows.pop();
  const population = Math.floor(last[numerator] / last[denominator]);
  rows.forEach((row) => row.pop = population);
  return rows;
}

const patchPopulation = (rows) => {
  const population = Math.max(...rows.map((row) => row.pop || 0));
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
  pvacc_pct: parseFloat(((row.pvacc / row.pop) * 100).toFixed(2)),
  fvacc_pct: parseFloat(((row.fvacc / row.pop) * 100).toFixed(2))
}));

const calcActiveEst = (rows) => {
  let cases = [];
  let deaths = [];
  rows.forEach((row) => {
    cases.push(row.cases_avg);
    deaths.push(row.deaths_avg);
    if (cases.length > 15) { cases.shift() }
    if (deaths.length > 15) { deaths.shift() }
    row.active_est = Math.floor(sum(cases) - sum(deaths));
  })
  return rows;
}

const sort = (sortFn) => (rows) => rows.sort(sortFn);

const byDate = (a, b) => {
  if (!a.date) console.log(a)
  const toDateValue = (dateStr) => parseInt(dateStr.split('-').join(''));
  const aDate = toDateValue(a.date);
  const bDate = toDateValue(b.date);
  if (aDate > bDate) return 1
  else if (aDate === bDate) return 0
  else return -1
}

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

  // Case & Average data:
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

  const getAvgCovidCountyData = async (fileNames, columns, rowFilter) => (
    await Promise.all(fileNames.map(async (fileName) =>
      await getAvgCovidData(fileName, columns, rowFilter)
    ))
  ).flat();
  
  // Vaccination data:
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
    forEachGroup(
      sort(byDate),
      patchVaccValues,
      calcVaccPercents,
      calcActiveEst
    )
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
    filterGroups(states),
    forEachGroup(
      sort(byDate),
      patchVaccValues,
      calcPopulation("pvacc", "pvacc_pct"),
      calcVaccPercents,
      calcActiveEst
    )
  );

  const countyColumns = {...stateColumns, "county": String};
  const countyJoin = ["date", "state", "county"];
  const countyCovidFile = 'us-counties.csv';
  const countyCovidAvgFiles = [
    'us-counties-2020.csv',
    'us-counties-2021.csv'
  ];

  const countyData = initialValue(multiJoin(
    countyJoin,
    await getBaseCovidData(countyCovidFile, countyColumns, countyRowFilter),
    await getAvgCovidCountyData(countyCovidAvgFiles, countyColumns, countyRowFilter),
    await getCountyVaccData()
  )).pipe(
    groupBy(["county", "state"]),
    filterGroups(counties),
    forEachGroup(
      patchPopulation,
      sort(byDate),
      patchVaccValues,
      calcVaccPercents,
      calcActiveEst
    )
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

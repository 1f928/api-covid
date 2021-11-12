
// --- Joins

const leftJoin = (a, b, cols) => {
  // Helpers
  const getRowKey = (row) => cols.reduce((keyStr, col) => keyStr + row[col], "");
  const arrayToQuick = (arr) => {
    const quick = {};
    arr.forEach((row) => quick[getRowKey(row)] = row);
    return quick;
  }
  const quickToArray = (quick) => Object.values(quick).map((val) => val);

  // ---
  const aQuick = arrayToQuick(a);
  b.forEach((row, i) => {
    const rowKey = getRowKey(row);
    aQuick[rowKey] = {...aQuick[rowKey], ...row}
  });

  return quickToArray(aQuick);
};

// --- Columns

// Name changes in the format of [{old: new}]
const renameColumns = (changes) => (rows) => rows.map((row) => {
  Object.entries(row).forEach(([key, val]) => {
    if (changes[key]) {
      delete row[key];
      row[changes[key]] = val
    }
  });
  return row;
});

const addColumn = (colName, colValue = null) => (rows) =>
  rows.map((row) => ({...row, [colName]: colValue}));

const removeColumns = (columnsToDelete) => (rows) => rows.map((row) => {
  return Object.entries(row).reduce((newRow, [key, val]) => ({
    ...newRow,
    ...(columnsToDelete.includes(key) ? {} : { key: val })
  }), {})
});

const filterColumns = (columnsToKeep) => (rows) => rows.map((row) => {
  return Object.entries(row).reduce((newRow, [key, val]) => ({
    ...newRow,
    ...(columnsToKeep.includes(key) ? { [key]: val } : {})
  }), {})
});

const castColumns = (columns) => (rows) => rows.map((row) => {
  return Object.entries(row).reduce((newRow, [key, val]) => ({
    ...newRow,
    ...{ [key]: (columns[key] ? columns[key](val) : val) }
  }))
});

const filterAndCastColumns = (columns) => (rows) => rows.map((row) => {
  return Object.entries(row).reduce((newRow, [key, val]) => ({
    ...newRow,
    ...(columns[key] ? {[key]: columns[key](val)} : {})
  }), {})
});

// --- Rows



// --- Groups

const keysMatch = (key1, key2) => {
  return Object.keys(key1).reduce((bool, key) => (
    !bool ? bool : key1[key] === key2[key]
  ), true);
}

// Accepts a list of columns to group by, and transforms the
// given rows into groups of rows - grouped by given columns
// 
// (c1) + ([{c1: v1, c2: v2}, {c1: v2, c2: v2}, {c1: v2, c2: v1}]) =
// [
//   {keys: [c1: v1], rows: [{c1: v1, c2: v2}]}
//   {keys: [c1: v2], rows: [{c1: v2, c2: v2}, {c1: v2, c2: v1}]}
// ]
const groupBy = (groupKeys) => (rows) => {
  const groups = {}; // {[{rowKey}]: [rows], ...}

  rows.forEach((row) => {
    const rowKey = groupKeys.reduce((keys, key) => ({...keys, [key]: row[key]}), {});
    const stringKey = JSON.stringify(rowKey)
    if (!groups[stringKey]) groups[stringKey] = []
    groups[stringKey].push(row);
  });

  return Object.entries(groups).map(([key, value]) => ({
    keys: JSON.parse(key),
    rows: value
  }))
}

const forEachGroup = (rowsFn) => (groups) => groups.map((group) => ({
  ...group,
  rows: rowsFn(group.rows)
}));

const filterGroups = (keysList) => (groups) => groups.filter((group) => {
  const km = keysList.filter((keys) => keysMatch(group.keys, keys))
  return km.length > 0;
}); 

module.exports = {
  leftJoin,

  renameColumns,
  addColumn,
  removeColumns,
  filterColumns,
  castColumns,
  filterAndCastColumns,

  groupBy,
  filterGroups,
  forEachGroup
};

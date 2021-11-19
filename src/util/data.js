
// --- Joins

// Helpers
const quickToArray = (quick) => Object.values(quick).map((val) => val);
const getRowKey = (row, keyCols) => keyCols.reduce((keyStr, col) => keyStr + row[col], "");
const arrayToQuick = (arr, keyCols) => {
  const quick = {};
  arr.forEach((row) => quick[getRowKey(row, keyCols)] = row);
  return quick;
}

const leftJoin = (a, b, cols) => {
  const aQuick = arrayToQuick(a);

  b.forEach((row, i) => {
    const rowKey = getRowKey(row);
    aQuick[rowKey] = {...aQuick[rowKey], ...row}
  });

  return quickToArray(aQuick);
};

const multiJoin = (keyCols, ...arrs) => {
  const quicks = arrs.map((arr) => arrayToQuick(arr, keyCols));
  const join = {};
  quicks.forEach((quick) => Object.entries(quick).forEach(([rowKey, row]) => {
    join[rowKey] = {
      ...(join[rowKey] ? join[rowKey] : {}),
      ...row
    }
  }));
  
  return quickToArray(join);
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
  for (key in key1) { if (key1[key] !== key2[key]) return false };
  return true;
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
    groupKeys.forEach((key) => delete row[key]);
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
  for (keys of keysList) { if (keysMatch(keys, group.keys)) return true };
  return false;
}); 

module.exports = {
  leftJoin,
  multiJoin,

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

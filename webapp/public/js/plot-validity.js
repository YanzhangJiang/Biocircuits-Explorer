// Pure helpers for turning solver validity metadata into Plotly-safe values.
// The backend serializes non-finite Julia values as strings; Plotly must see
// `null`, not "NaN"/"Inf", so those samples render as gaps.

export function finitePlotValue(value) {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function validitySummary(invalidCount, totalCount, declaredPartial) {
  return {
    invalidCount,
    totalCount,
    partial: declaredPartial === true || invalidCount > 0,
  };
}

export function prepareScan1DPlotData(data = {}) {
  const rows = Array.isArray(data.output_traj) ? data.output_traj : [];
  const validity = Array.isArray(data.valid) ? data.valid : null;
  let invalidCount = 0;

  const outputTraj = rows.map((rawRow, rowIndex) => {
    const row = Array.isArray(rawRow) ? rawRow : [];
    const validPoint = validity === null || validity[rowIndex] === true;
    const sanitized = Array.from(row, value => validPoint ? finitePlotValue(value) : null);
    if (!validPoint || sanitized.some(value => value === null)) invalidCount += 1;
    return sanitized;
  });

  return {
    outputTraj,
    ...validitySummary(invalidCount, rows.length, data.partial),
  };
}

function prepareGridPlotData(data = {}) {
  const rows = Array.isArray(data.output_grid) ? data.output_grid : [];
  const validityGrid = Array.isArray(data.validity_grid) ? data.validity_grid : null;
  let invalidCount = 0;
  let totalCount = 0;

  const outputGrid = rows.map((rawRow, rowIndex) => {
    const row = Array.isArray(rawRow) ? rawRow : [];
    const validityRow = validityGrid === null
      ? null
      : (Array.isArray(validityGrid[rowIndex]) ? validityGrid[rowIndex] : []);
    totalCount += row.length;
    return Array.from(row, (value, columnIndex) => {
      const validPoint = validityRow === null || validityRow[columnIndex] === true;
      const sanitized = validPoint ? finitePlotValue(value) : null;
      if (sanitized === null) invalidCount += 1;
      return sanitized;
    });
  });

  return {
    outputGrid,
    ...validitySummary(invalidCount, totalCount, data.partial),
  };
}

export function prepareScan2DPlotData(data = {}) {
  return prepareGridPlotData(data);
}

export function prepareAtlasLandscapePlotData(data = {}) {
  return prepareGridPlotData(data);
}

export function prepareFretHeatmapPlotData(data = {}) {
  return prepareGridPlotData({
    output_grid: data.fret,
    validity_grid: data.validity_grid,
    partial: data.partial,
  });
}

export function prepareRopCloudPlotData(data = {}) {
  const rows = Array.isArray(data.reaction_orders) ? data.reaction_orders : [];
  const fretValues = Array.isArray(data.fret_values) ? data.fret_values : [];
  const validity = Array.isArray(data.valid) ? data.valid : null;
  const reactionOrders = [];
  const colors = [];
  let invalidCount = 0;

  rows.forEach((rawRow, index) => {
    const row = Array.isArray(rawRow) ? Array.from(rawRow, finitePlotValue) : [];
    const color = finitePlotValue(fretValues[index]);
    const declaredValid = validity === null || validity[index] === true;
    const pointValid = declaredValid
      && row.length > 0
      && row.every(value => value !== null)
      && color !== null
      && color > 0;
    if (!pointValid) {
      invalidCount += 1;
      return;
    }
    reactionOrders.push(row);
    colors.push(color);
  });

  return {
    reactionOrders,
    fretValues: colors,
    ...validitySummary(invalidCount, rows.length, data.partial),
  };
}

export function maskGridByOutputValidity(grid, outputGrid) {
  if (!Array.isArray(grid)) return [];
  return grid.map((rawRow, rowIndex) => {
    const row = Array.isArray(rawRow) ? rawRow : [];
    return Array.from(row, (value, columnIndex) => (
      outputGrid?.[rowIndex]?.[columnIndex] === null ? null : finitePlotValue(value)
    ));
  });
}

export function formatPartialValidityNotice(summary = {}) {
  if (summary.partial !== true && !(summary.invalidCount > 0)) return '';
  const invalidCount = Number.isInteger(summary.invalidCount) ? summary.invalidCount : 0;
  const totalCount = Number.isInteger(summary.totalCount) ? summary.totalCount : 0;
  if (invalidCount > 0 && totalCount > 0) {
    const noun = totalCount === 1 ? 'point' : 'points';
    return `Partial result: ${invalidCount}/${totalCount} ${noun} did not converge; gaps are not plotted.`;
  }
  return 'Partial result: some points did not converge; gaps are not plotted.';
}

export function formatAtlasLandscapeResponseSummary(response = {}) {
  const prepared = prepareAtlasLandscapePlotData(response);
  const values = prepared.outputGrid.flat().filter(Number.isFinite);
  const regimes = new Set(
    maskGridByOutputValidity(response.regime_grid, prepared.outputGrid)
      .flat()
      .filter(value => Number.isFinite(value) && value > 0),
  );
  const validityNotice = formatPartialValidityNotice(prepared);
  const coordinates = `Showing ${response.param1_symbol} × ${response.param2_symbol}.`;
  if (!values.length) return [coordinates, validityNotice].filter(Boolean).join(' ');

  const minVal = Math.min(...values);
  const maxVal = Math.max(...values);
  return [
    `Showing ${response.param1_symbol} × ${response.param2_symbol}; log-output ${minVal.toFixed(2)} to ${maxVal.toFixed(2)} across ${regimes.size} regimes.`,
    validityNotice,
  ].filter(Boolean).join(' ');
}

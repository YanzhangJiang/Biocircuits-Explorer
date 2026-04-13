import { applyPlotLayoutTheme, getPlotTheme, themeAxisTitle, applyPlotAxisTheme, themedColorbar, hexToRgba } from './theme.js';
import { setupPlotInteractionGuard } from './nodes.js';

export function formatPolyNumber(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return String(value);
  if (Math.abs(num) < 1e-9) return '0';
  if (Math.abs(num - Math.round(num)) < 1e-9) return String(Math.round(num));
  return num.toFixed(3).replace(/\.?0+$/, '');
}

export function formatPolyConstraint(row, rhs, symbols, isEquality = false) {
  const terms = [];
  row.forEach((coeff, idx) => {
    const num = Number(coeff);
    if (!Number.isFinite(num) || Math.abs(num) < 1e-9) return;
    const absCoeff = Math.abs(num);
    const coeffStr = Math.abs(absCoeff - 1) < 1e-9 ? '' : `${formatPolyNumber(absCoeff)}*`;
    const sign = num < 0 ? '-' : (terms.length ? '+' : '');
    terms.push(`${sign}${coeffStr}${symbols[idx]}`);
  });
  const lhs = terms.length ? terms.join(' ') : '0';
  return `${lhs} ${isEquality ? '=' : '≤'} ${formatPolyNumber(rhs)}`;
}

export function renderPolyCoordinateTable(rows, symbols, kind, linealitySet = new Set()) {
  if (!rows || !rows.length) return '';
  const header = symbols.map(sym => `<th>${sym}</th>`).join('');
  const label = kind === 'rays' ? 'R' : 'V';
  const extraHeader = kind === 'rays' ? '<th>Type</th>' : '';
  const body = rows.map((row, idx) => {
    const cells = row.map(value => `<td class="siso-profile-cell">${formatPolyNumber(value)}</td>`).join('');
    const extraCell = kind === 'rays'
      ? `<td>${linealitySet.has(idx + 1) ? '<span class="tag tag-nonasym">lineality</span>' : '<span class="tag tag-asym">ray</span>'}</td>`
      : '';
    return `<tr><td>${label}${idx + 1}</td>${cells}${extraCell}</tr>`;
  }).join('');
  return `
    <div class="siso-table-wrap scroll-panel">
      <table class="siso-family-table">
        <thead><tr><th>#</th>${header}${extraHeader}</tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

export function convexHull2D(points) {
  if (points.length <= 1) return points.slice();
  const sorted = points
    .map((point, index) => ({ ...point, index }))
    .sort((a, b) => a.x - b.x || a.y - b.y || a.index - b.index);
  const cross = (o, a, b) => (a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x);
  const lower = [];
  sorted.forEach(point => {
    while (lower.length >= 2 && cross(lower[lower.length - 2], lower[lower.length - 1], point) <= 0) lower.pop();
    lower.push(point);
  });
  const upper = [];
  for (let i = sorted.length - 1; i >= 0; i--) {
    const point = sorted[i];
    while (upper.length >= 2 && cross(upper[upper.length - 2], upper[upper.length - 1], point) <= 0) upper.pop();
    upper.push(point);
  }
  lower.pop();
  upper.pop();
  return lower.concat(upper);
}

export function plotTrajectory(data, plotId) {
  const { change_values, logx, regimes, x_sym, change_sym } = data;
  const nSpecies = x_sym.length;
  const nPoints = change_values.length;
  const plotTheme = getPlotTheme();

  const uniqueRegimes = [...new Set(regimes)];
  const palette = ['#6c8cff', '#51cf66', '#ff6b6b', '#ffd43b', '#4ecdc4', '#e599f7', '#ff922b', '#74c0fc', '#f06595', '#a9e34b'];
  const regimeColor = {};
  uniqueRegimes.forEach((r, i) => { regimeColor[r] = palette[i % palette.length]; });

  const speciesPalette = ['#7da2ff', '#4fd67a', '#ffd24d', '#ff7a7a', '#63d4d6', '#d48cff', '#ffb347', '#86caff', '#ff82b2', '#b8f06b'];
  const regimeSegments = [];
  let segStart = 0;
  for (let i = 1; i <= nPoints; i++) {
    if (i === nPoints || regimes[i] !== regimes[segStart]) {
      regimeSegments.push({
        regime: regimes[segStart],
        startIdx: segStart,
        endIdx: i - 1,
        x0: change_values[segStart],
        x1: change_values[i - 1],
      });
      segStart = i;
    }
  }

  const traces = [];
  for (let s = 0; s < nSpecies; s++) {
    const traceX = [];
    const traceY = [];
    const traceRegimes = [];
    regimeSegments.forEach((segment, idx) => {
      for (let i = segment.startIdx; i <= segment.endIdx; i++) {
        traceX.push(change_values[i]);
        traceY.push(logx[i][s]);
        traceRegimes.push(segment.regime);
      }
      if (idx < regimeSegments.length - 1) {
        traceX.push(null);
        traceY.push(null);
        traceRegimes.push(null);
      }
    });

    traces.push({
      x: traceX,
      y: traceY,
      customdata: traceRegimes,
      mode: 'lines',
      type: 'scatter',
      line: { color: speciesPalette[s % speciesPalette.length], width: 2.5 },
      name: x_sym[s],
      hovertemplate: `${x_sym[s]}<br>log ${change_sym}=%{x:.3g}<br>log(x)=%{y:.3g}<br>rgm %{customdata}<extra></extra>`,
    });
  }

  const totalRange = Math.max((change_values[nPoints - 1] ?? 0) - (change_values[0] ?? 0), 1e-9);
  const shapes = regimeSegments.map(segment => ({
    type: 'rect',
    xref: 'x',
    yref: 'paper',
    x0: segment.x0,
    x1: segment.x1,
    y0: 0,
    y1: 1,
    fillcolor: hexToRgba(regimeColor[segment.regime], 0.14),
    line: { width: 0 },
    layer: 'below',
  }));
  const annotations = regimeSegments.map(segment => {
    const widthRatio = Math.abs(segment.x1 - segment.x0) / totalRange;
    return {
      x: (segment.x0 + segment.x1) / 2,
      y: 0.985,
      xref: 'x',
      yref: 'paper',
      text: `rgm ${segment.regime}`,
      showarrow: false,
      textangle: widthRatio < 0.06 ? -90 : 0,
      font: { size: 10, color: regimeColor[segment.regime] },
      bgcolor: plotTheme.annotationBg,
      bordercolor: hexToRgba(regimeColor[segment.regime], 0.4),
      borderwidth: 1,
      borderpad: 2,
      opacity: widthRatio < 0.025 ? 0.75 : 1,
    };
  });

  const layout = {
    showlegend: true,
    margin: { t: 40, b: 60, l: 70, r: 20 },
    title: { text: `Changing ${change_sym}`, font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
    xaxis: { title: `log ${change_sym}` },
    yaxis: { title: 'log(x)' },
    legend: { font: { color: plotTheme.fontColor, size: 9 } },
    shapes,
    annotations,
  };

  Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
  const plotEl = document.getElementById(plotId);
  if (plotEl) setupPlotInteractionGuard(plotEl);
}

export function quantileSorted(sorted, q) {
  if (!sorted.length) return null;
  const pos = Math.max(0, Math.min(sorted.length - 1, (sorted.length - 1) * q));
  const lower = Math.floor(pos);
  const upper = Math.ceil(pos);
  if (lower === upper) return sorted[lower];
  const weight = pos - lower;
  return sorted[lower] * (1 - weight) + sorted[upper] * weight;
}

export function plotHeatmap(data, plotId) {
  const { logq1, logq2, fret, regime, bounds, q_sym } = data;
  const logFret = fret.map(row => row.map(v => Math.log10(v + 1e-30)));
  const plotTheme = getPlotTheme();

  const traces = [
    {
      z: logFret, x: logq1, y: logq2, type: 'heatmap', colorscale: 'Viridis',
      colorbar: themedColorbar('log(FRET)'),
    },
    {
      z: bounds, x: logq1, y: logq2, type: 'contour',
      contours: { start: 0.5, end: 0.5, size: 1, coloring: 'none' },
      line: { color: plotTheme.contourLineColor, width: 2 }, showscale: false,
    },
  ];

  const layout = {
    autosize: true,
    margin: { t: 40, b: 60, l: 70, r: 20 },
    title: { text: 'FRET + Regime Boundaries', font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
    xaxis: { title: `log(${q_sym[0]})` },
    yaxis: { title: `log(${q_sym[1]})` },
  };

  Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
}

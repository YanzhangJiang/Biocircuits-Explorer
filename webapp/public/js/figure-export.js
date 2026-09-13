// Biocircuits Explorer — Shared helpers for the figure-export pages
//
// The four figure-export-*.html pages are one-off capture targets for the
// out-of-repo screenshot pipeline. Each page renders fixed plot panels
// straight from the backend and reports completion through
// window.__figureExport (the contract the pipeline waits on). This module
// holds the pieces that used to be copy-pasted into every inline script:
// the shared demo network, behavior-family selection, Plotly export
// styling, and the status/done reporters.

import { apiSilent } from './api.js';

export { apiSilent };

// Seven-species "minimal multimodal witness" network shared by the node,
// light, and multiswitch export pages. The backend numbers Kd parameters in
// reaction-array order (reaction_parser.jl assigns Kd1..Kdr sequentially),
// so the reaction order is load-bearing: Kd1 must stay the
// C_A_B + B <-> C_A_B_B binding for the {A,Kd1}/{B,Kd1} ROP pairs and the
// resulting '1 → -1 → 1 → -1' exact family to keep their meaning.
export const TRIPLE_SWITCH_NETWORK = {
  species: [
    { name: 'A', role: 'free' },
    { name: 'B', role: 'free' },
    { name: 'C', role: 'free' },
    { name: 'C_A_B', role: 'bound' },
    { name: 'C_A_B_B', role: 'bound' },
    { name: 'C_A_C', role: 'bound' },
    { name: 'C_A_B_C', role: 'bound' },
  ],
  reactions: [
    { kind: 'binding', formula: 'C_A_B + B <-> C_A_B_B', reversible: true, kd: 1.0 },
    { kind: 'binding', formula: 'B + C_A_C <-> C_A_B_C', reversible: true, kd: 1.0 },
    { kind: 'binding', formula: 'A + B <-> C_A_B', reversible: true, kd: 1.0 },
    { kind: 'binding', formula: 'A + C <-> C_A_C', reversible: true, kd: 1.0 },
  ],
};

export function labelSwitches(label) {
  const values = label
    .split('→')
    .map(part => Number(part.trim()))
    .filter(v => Number.isFinite(v) && Math.abs(v) > 1e-9);
  let switches = 0;
  for (let i = 1; i < values.length; i += 1) {
    if (Math.sign(values[i]) !== Math.sign(values[i - 1])) switches += 1;
  }
  return switches;
}

// Pick an exact behavior family. With required:false (default) the picker
// falls back to the cleanest, most-switching family when exactLabel is
// absent; with required:true a missing family is a hard error.
export function chooseExactFamily(behavior, { exactLabel = '1 → -1 → 1 → -1', required = false } = {}) {
  const preferred = behavior.exact_families.find(f => f.exact_label === exactLabel);
  if (preferred) return preferred;
  if (required) throw new Error(`Expected exact family ${exactLabel} not found`);
  return [...behavior.exact_families].sort((a, b) => {
    const cleanA = /NaN|Inf/.test(a.exact_label) ? 0 : 1;
    const cleanB = /NaN|Inf/.test(b.exact_label) ? 0 : 1;
    return (labelSwitches(b.exact_label) - labelSwitches(a.exact_label))
      || (cleanB - cleanA)
      || (b.n_paths - a.n_paths);
  })[0];
}

// Pulse page variant: prefers the bell-shaped family, then a single switch.
export function chooseFamily(behavior) {
  const preferred = behavior.exact_families.find(f => f.exact_label === '1 → 0 → -1');
  if (preferred) return preferred;
  return behavior.exact_families.find(f => f.exact_label === '1 → -1') || behavior.exact_families[0];
}

export function selectRepresentativePath(family) {
  return family.representative_path_idx ?? family.path_indices[0];
}

// Splice the swept parameter back into the trajectory's fixed-qK vector so
// the follow-up scan runs at the trajectory's interior point.
export function fixedQKFromTrajectory(trajectory, behavior) {
  const fixedQK = [...trajectory.parameters];
  fixedQK.splice(behavior.change_qK_idx - 1, 0, 0);
  return fixedQK;
}

// Plotly export styling shared by the light panel pages. The figure font
// stays on the Arial stack (rendered into the exported SVG/PNG artifact)
// even though the page chrome uses the system stack.
const EXPORT_PLOT_FONT = 'Arial, Helvetica, sans-serif';

export async function styleExportRopPlot(plotId = 'ropPlot') {
  await Plotly.relayout(plotId, {
    'title.text': 'Reaction-order geometry',
    'title.font.size': 15,
    'xaxis.title.text': 'reaction order of A',
    'yaxis.title.text': 'reaction order of B',
    'font.family': EXPORT_PLOT_FONT,
  });
}

export async function styleExportScanPlot({
  plotId = 'scanPlot',
  title,
  xTitle,
  yTitle,
  lineColor,
  lineWidth,
  traceName,
}) {
  await Plotly.restyle(plotId, {
    'line.color': [lineColor],
    'line.width': [lineWidth],
    name: [traceName],
  });
  await Plotly.relayout(plotId, {
    'title.text': title,
    'title.font.size': 15,
    'xaxis.title.text': xTitle,
    'yaxis.title.text': yTitle,
    'font.family': EXPORT_PLOT_FONT,
    'showlegend': false,
  });
}

function setExportStatus(text) {
  const el = document.getElementById('status');
  if (el) el.textContent = text;
}

// window.__figureExport is the screenshot pipeline's completion contract.
// The payload keys are part of that contract and differ per page, so the
// caller supplies them verbatim; `done` is always appended last.
export function reportFigureExportDone(payload, statusText = 'done') {
  window.__figureExport = { ...payload, done: true };
  setExportStatus(statusText);
}

export function reportFigureExportError(err) {
  console.error(err);
  const message = String(err && err.message || err);
  window.__figureExport = { done: false, error: message };
  setExportStatus(`error: ${message}`);
}

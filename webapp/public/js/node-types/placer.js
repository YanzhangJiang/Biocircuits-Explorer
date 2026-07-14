// Biocircuits Explorer — ParameterPlacer CHAIN: a "tunable design station".
//
// params (placer-params) + result (placer-result) pair; Quick Add builds
// reaction-network -> model-builder -> placer-params -> placer-result.
// Embodies "tunable" as an experience across multiple design HANDLES:
//   MENU      — "Show achievable menu": the reaction-order ladder this circuit can
//               be tuned to + its regime SEQUENCE (the transitions a threshold can
//               sit at).                                    [/api/v1/placer_menu]
//   SLOPE     — click a rung (or type a target RO + Solve): solve the regime
//               polytope for a concrete kd.                 [/api/v1/place_parameters]
//   THRESHOLD — place a chosen transition (EC50/knee) at a target input dose.
//                                                           [/api/v1/placer_threshold]
//   LIVE TUNE — a Kd slider re-plots the dose-response.     [/api/v1/placer_curve]
import { api, syncSelectOptions, escapeHtml } from '../api.js';
import { setNodeLoading, getModelForNode, setupPlotResize, ensureModelSession,
         setupAutoUpdate, triggerConfigUpdate } from '../nodes.js';
import { plotParameterScan1D } from '../scan.js';
import { commitWorkspaceSnapshot } from '../workspace.js';
import { nodeRegistry, ensureNodeData, connections } from '../state.js';

export const PLACER_TYPES = {
  'placer-params': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'Parameter Placer Config',
    inputs: [{ port: 'model', type: 'ModelArtifact', label: 'Model' }],
    outputs: [{ port: 'params', type: 'ParameterPlacerConfig', label: 'Config' }],
    defaultWidth: 320,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>Input total:</label>
          <select id="${nodeId}-input" class="auto-update"></select>
        </div>
        <div class="param-row">
          <label>Output species:</label>
          <select id="${nodeId}-output" class="auto-update"></select>
        </div>
        <div class="param-row">
          <label>Target reaction order:</label>
          <input type="number" id="${nodeId}-target" value="1" step="1" class="auto-update">
        </div>
        <div class="param-row">
          <label>Kd bounds (log10):</label>
          <div style="display:flex;gap:4px;align-items:center;">
            <input type="number" id="${nodeId}-kdlo" value="-3" step="0.5" style="width:70px;" class="auto-update">
            to
            <input type="number" id="${nodeId}-kdhi" value="3" step="0.5" style="width:70px;" class="auto-update">
          </div>
        </div>
      `;
    },
    onInit(nodeId) { setupAutoUpdate(nodeId, 'placer-params'); },
    async prepare(nodeId) {
      const model = getModelForNode(nodeId);
      if (!model) return;
      syncSelectOptions(document.getElementById(`${nodeId}-input`), model.q_sym);
      syncSelectOptions(document.getElementById(`${nodeId}-output`), model.x_sym);
      triggerConfigUpdate(nodeId, 'placer-params');
    },
  },
  'placer-result': {
    category: 'result',
    headerClass: 'header-result',
    title: 'Parameter Placer',
    inputs: [{ port: 'params', type: 'ParameterPlacerConfig', label: 'Config' }],
    outputs: [],
    defaultWidth: 480,
    createBody(nodeId) {
      return `
        <button class="btn btn-run" data-action="realizePlacerProgram" data-node="${nodeId}">Realize target program</button>
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:6px;">
          <button class="btn btn-small" data-action="loadPlacerMenu" data-node="${nodeId}">Show achievable menu</button>
          <button class="btn btn-small" data-action="executePlacerResult" data-node="${nodeId}">Solve single RO</button>
        </div>
        <div id="${nodeId}-menu" style="margin-top:6px;"></div>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim"><b>Realize target program</b> solves a Kd <i>ordering</i> so the dose-response walks the whole regime program (no sampling). <b>Show achievable menu</b> and <b>Solve single RO</b> are per-regime fine-tuning handles.</span>
        </div>
      `;
    },
    async execute(nodeId) { return realizePlacerProgram(nodeId); },
  },
};

function getPlacerConfig(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!conn) { alert('Please connect to a Parameter Placer Config node'); return null; }
  const paramsNode = nodeRegistry[conn.fromNode];
  if (!paramsNode) { alert('Config node missing'); return null; }
  triggerConfigUpdate(conn.fromNode, paramsNode.type || 'placer-params');
  const config = paramsNode.data?.config;
  if (!config) { alert('Configure the Placer Config node first'); return null; }
  if (!config.input_sym || !config.output_sym) {
    alert('Select an input total and an output species in the config node');
    return null;
  }
  return config;
}

// MENU — achievable RO ladder (clickable rungs) + regime sequence + threshold control.
export async function loadPlacerMenu(nodeId) {
  const config = getPlacerConfig(nodeId);
  if (!config) return;
  const menuEl = document.getElementById(`${nodeId}-menu`);
  setNodeLoading(nodeId, true);
  try {
    const sessionId = await ensureModelSession(nodeId);
    const data = await api('placer_menu', {
      session_id: sessionId, input_sym: config.input_sym, output_sym: config.output_sym,
    });
    const ladder = data.ladder || [];
    const path = data.path || [];
    if (!ladder.length) {
      menuEl.innerHTML = `<span class="text-dim">No achievable reaction orders found for this output.</span>`;
      return;
    }
    // (a) slope rungs
    const chips = ladder.map(ro =>
      `<span class="summary-chip placer-chip" data-ro="${ro}" title="Solve for RO = ${ro}" style="cursor:pointer;">${fmtNum(ro)}</span>`).join(' ');
    // (b) regime sequence + transitions for threshold placement
    const seq = path.map(p => p.ro === null ? '∞' : fmtNum(p.ro)).join(' → ');
    const transitions = [];
    for (let i = 0; i + 1 < path.length; i++) {
      const a = path[i], b = path[i + 1];
      if (a.ro === null || b.ro === null || a.ro === b.ro) continue;
      transitions.push({ from: a.vertex_idx, to: b.vertex_idx, label: `RO ${fmtNum(a.ro)} → ${fmtNum(b.ro)}` });
    }
    const transOpts = transitions.map(t =>
      `<option value="${t.from}:${t.to}">${t.label}</option>`).join('');
    const threshBlock = transitions.length ? `
      <div style="margin-top:8px;border-top:0.5px solid var(--panel-border);padding-top:6px;">
        <div style="font-size:12px;color:var(--text-dim);margin-bottom:3px;">Place a threshold (set where a transition happens):</div>
        <div style="display:flex;gap:5px;align-items:center;flex-wrap:wrap;">
          <select id="${nodeId}-trans">${transOpts}</select>
          <span style="font-size:12px;">at input</span>
          <input type="number" id="${nodeId}-thresh-dose" value="1" step="any" style="width:90px;">
          <button class="btn" id="${nodeId}-thresh-btn">Place</button>
        </div>
      </div>` : '';
    menuEl.innerHTML =
      `<div style="font-size:12px;color:var(--text-dim);margin-bottom:3px;">Achievable reaction orders (click a rung to solve):</div>` +
      `<div style="display:flex;gap:4px;flex-wrap:wrap;">${chips}</div>` +
      (seq ? `<div style="font-size:12px;color:var(--text-dim);margin-top:5px;">Response regimes: ${seq}</div>` : '') +
      threshBlock;
    menuEl.querySelectorAll('.placer-chip').forEach(chip => {
      chip.addEventListener('click', () => solvePlacer(nodeId, parseFloat(chip.dataset.ro)));
    });
    const tb = document.getElementById(`${nodeId}-thresh-btn`);
    if (tb) tb.addEventListener('click', () => placeThreshold(nodeId));
  } catch (e) {
    menuEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

// SLOPE — "Solve (target RO)" button uses the config's typed target.
export async function executePlacerResult(nodeId) {
  const config = getPlacerConfig(nodeId);
  if (!config) return false;
  if (!Number.isFinite(config.target_ro)) { alert('Enter a numeric target reaction order'); return false; }
  return solvePlacer(nodeId, config.target_ro);
}

async function solvePlacer(nodeId, targetRO) {
  const config = getPlacerConfig(nodeId);
  if (!config) return false;
  const kdBounds = (Number.isFinite(config.kd_lo) && Number.isFinite(config.kd_hi)) ? [config.kd_lo, config.kd_hi] : null;
  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);
  try {
    const sessionId = await ensureModelSession(nodeId);
    const data = await api('place_parameters', {
      session_id: sessionId, input_sym: config.input_sym, output_sym: config.output_sym,
      target_ro: targetRO, kd_bounds: kdBounds,
    });
    stashSolved(nodeId, sessionId, config, data, data.kd_bounds || kdBounds || [-3, 3]);
    const summary =
      `<span class="summary-chip">target RO = ${escapeHtml(String(targetRO))}</span>` +
      `<span class="summary-chip">predicted = ${fmtNum(data.predicted_RO)}</span>` +
      `<span class="summary-chip">measured = ${fmtNum(data.measured_RO)}</span>` +
      `<span class="summary-chip">${passBadge(data.pass)}</span>`;
    renderSolved(nodeId, contentEl, data, summary);
    commitWorkspaceSnapshot('parameter-placer');
    return true;
  } catch (e) {
    contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
    return false;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

// PROGRAM — realize the whole regime program (the design target is a program, not a
// single slope): solve a Kd ORDERING so the swept dose-response walks the full
// regime sequence. [/api/v1/placer_realize_program]
export async function realizePlacerProgram(nodeId) {
  const config = getPlacerConfig(nodeId);
  if (!config) return false;
  const kdBounds = (Number.isFinite(config.kd_lo) && Number.isFinite(config.kd_hi)) ? [config.kd_lo, config.kd_hi] : null;
  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);
  try {
    const sessionId = await ensureModelSession(nodeId);
    const data = await api('placer_realize_program', {
      session_id: sessionId, input_sym: config.input_sym, output_sym: config.output_sym,
      kd_bounds: kdBounds,
    });
    stashSolved(nodeId, sessionId, config, data, kdBounds || [-3, 3]);
    const bps = (data.breakpoints || []).map(fmtSci).join(', ');
    const summary =
      `<span class="summary-chip">target ${escapeHtml(signArrows(data.target_signs))}</span>` +
      `<span class="summary-chip">realized ${escapeHtml(signArrows(data.measured_signs))}</span>` +
      `<span class="summary-chip">${passBadge(data.pass)}</span>` +
      (bps ? `<span class="summary-chip">breakpoints: ${escapeHtml(bps)}</span>` : '');
    renderSolved(nodeId, contentEl, data, summary);
    // The realized RO itinerary, as a mono detail line under the summary chips.
    const detail = document.createElement('div');
    detail.className = 'text-dim';
    detail.style.cssText = "font-family:'Consolas',monospace;";
    detail.textContent =
      `target program:  ${fmtSeq(data.target_program)}    realized:  ${fmtSeq(data.measured_program)}`;
    contentEl.querySelector('.siso-summary-line')?.after(detail);
    commitWorkspaceSnapshot('parameter-placer');
    return true;
  } catch (e) {
    contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
    return false;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

// THRESHOLD — place a chosen transition at a target input dose.
async function placeThreshold(nodeId) {
  const config = getPlacerConfig(nodeId);
  if (!config) return;
  const transSel = document.getElementById(`${nodeId}-trans`);
  const doseEl = document.getElementById(`${nodeId}-thresh-dose`);
  if (!transSel || !transSel.value) { alert('Pick a transition'); return; }
  const [fromIdx, toIdx] = transSel.value.split(':').map(Number);
  const target = parseFloat(doseEl?.value);
  if (!Number.isFinite(target) || target <= 0) { alert('Enter a positive target input dose'); return; }
  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);
  try {
    const sessionId = await ensureModelSession(nodeId);
    const data = await api('placer_threshold', {
      session_id: sessionId, input_sym: config.input_sym, output_sym: config.output_sym,
      from_idx: fromIdx, to_idx: toIdx, target_input: target,
    });
    stashSolved(nodeId, sessionId, config, data, [-3, 3]);
    const summary =
      `<span class="summary-chip">${escapeHtml(transSel.options[transSel.selectedIndex].text)}</span>` +
      `<span class="summary-chip">target dose = ${fmtSci(target)}</span>` +
      `<span class="summary-chip">placed at = ${fmtSci(data.measured_breakpoint)}</span>` +
      `<span class="summary-chip">${passBadge(data.pass)}</span>`;
    renderSolved(nodeId, contentEl, data, summary);
    commitWorkspaceSnapshot('parameter-placer');
  } catch (e) {
    contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

function stashSolved(nodeId, sessionId, config, data, kdBounds) {
  if (!nodeRegistry[nodeId]) return;
  ensureNodeData(nodeId).placerResult = {
    sessionId, config,
    kd: (data.kd || []).slice(), totals: data.totals || {}, kdBounds: kdBounds || [-3, 3],
  };
}

// Common render: summary chips + solved kd/totals/dominance + live Kd slider + curve.
function renderSolved(nodeId, contentEl, data, summaryHtml) {
  const kd = data.kd || [];
  const totals = data.totals || {};
  const kdRows = kd.map((v, i) => `Kd${i + 1} = ${fmtSci(v)}`).join(', ');
  const totalRows = Object.keys(totals).sort().map(k => `${escapeHtml(k)} = ${fmtSci(totals[k])}`).join(', ');
  const dominance = (data.dominance_ordering || []).map(escapeHtml).join('; ');
  const st = ensureNodeData(nodeId).placerResult;
  const bounds = st?.kdBounds || [-3, 3];
  const kdOpts = kd.map((_, i) => `<option value="${i}">Kd${i + 1}</option>`).join('');
  const sliderBlock = kd.length ? `
    <div class="param-row" style="display:block;margin-top:8px;border-top:0.5px solid var(--panel-border);padding-top:6px;">
      <div style="font-size:12px;color:var(--text-dim);margin-bottom:3px;">Live tune (drag to move the response along the ladder):</div>
      <div style="display:flex;gap:6px;align-items:center;">
        <select id="${nodeId}-kdsel">${kdOpts}</select>
        <input type="range" id="${nodeId}-kdslider" min="${bounds[0]}" max="${bounds[1]}" step="0.05" value="${Math.log10(kd[0] || 1)}" style="flex:1;">
        <span id="${nodeId}-kdval" style="font-size:12px;min-width:74px;">Kd1 = ${fmtSci(kd[0])}</span>
      </div>
    </div>` : '';
  const totalOpts = Object.keys(totals).map(t => `<option value="${t}">${t}</option>`).join('');
  const levelBlock = (kd.length && totalOpts) ? `
    <div class="param-row" style="display:block;margin-top:8px;border-top:0.5px solid var(--panel-border);padding-top:6px;">
      <div style="font-size:12px;color:var(--text-dim);margin-bottom:3px;">Set output level (numeric — the quantitative layer):</div>
      <div style="display:flex;gap:5px;align-items:center;flex-wrap:wrap;">
        adjust <select id="${nodeId}-leveltotal">${totalOpts}</select>
        so [${escapeHtml(st?.config?.output_sym || '')}]=<input type="number" id="${nodeId}-leveltarget" value="1" step="any" style="width:72px;">
        at input <input type="number" id="${nodeId}-levelinput" value="100" step="any" style="width:72px;">
        <button class="btn" id="${nodeId}-levelbtn">Set</button>
      </div>
      <div id="${nodeId}-levelnote" style="font-size:12px;color:var(--text-dim);margin-top:3px;"></div>
    </div>` : '';

  contentEl.innerHTML = `
    <div class="siso-summary-line" style="flex-wrap:wrap;gap:6px;">${summaryHtml}</div>
    <div class="param-row" style="display:block;margin-top:6px;">
      <div><strong>Solved Kd:</strong> ${kdRows || '<span class="text-dim">none</span>'}</div>
      <div><strong>Totals:</strong> ${totalRows || '<span class="text-dim">none</span>'}</div>
      <div><strong>Dominance:</strong> ${dominance || '<span class="text-dim">n/a</span>'}</div>
    </div>
    ${sliderBlock}
    ${levelBlock}
    <div class="plot-container" id="${nodeId}-plot"></div>
  `;

  setTimeout(() => {
    if (data.dose_response_curve) {
      plotParameterScan1D(data.dose_response_curve, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }
  }, 50);

  const sel = document.getElementById(`${nodeId}-kdsel`);
  const slider = document.getElementById(`${nodeId}-kdslider`);
  if (sel && slider) {
    sel.addEventListener('change', () => {
      const idx = parseInt(sel.value, 10) || 0;
      const cur = (ensureNodeData(nodeId).placerResult?.kd || [])[idx];
      if (Number.isFinite(cur)) slider.value = String(Math.log10(cur));
      const valEl = document.getElementById(`${nodeId}-kdval`);
      if (valEl) valEl.textContent = `Kd${idx + 1} = ${fmtSci(cur)}`;
    });
    let timer = null;
    slider.addEventListener('input', () => { clearTimeout(timer); timer = setTimeout(() => livePlacerTune(nodeId), 120); });
  }
  const levelBtn = document.getElementById(`${nodeId}-levelbtn`);
  if (levelBtn) levelBtn.addEventListener('click', () => placeLevel(nodeId));
}

// LEVEL handle (numeric, quantitative layer): adjust a total so the output reaches
// a target level at an operating input. [/api/v1/placer_level]
async function placeLevel(nodeId) {
  const st = ensureNodeData(nodeId).placerResult;
  if (!st) return;
  const adjust = document.getElementById(`${nodeId}-leveltotal`)?.value;
  const target = parseFloat(document.getElementById(`${nodeId}-leveltarget`)?.value);
  const operating = parseFloat(document.getElementById(`${nodeId}-levelinput`)?.value);
  if (!adjust || !Number.isFinite(target) || target <= 0 || !Number.isFinite(operating) || operating <= 0) {
    alert('Pick a total and enter a positive target level + operating input'); return;
  }
  setNodeLoading(nodeId, true);
  const note = document.getElementById(`${nodeId}-levelnote`);
  try {
    const data = await api('placer_level', {
      session_id: st.sessionId, input_sym: st.config.input_sym, output_sym: st.config.output_sym,
      kd: st.kd, totals: st.totals, operating_input: operating, target_level: target, adjust_sym: adjust,
    });
    if (data.totals) st.totals = data.totals;
    if (note) note.innerHTML = `${escapeHtml(data.adjusted_total)} = ${fmtSci(data.adjusted_value)} → ` +
      `[${escapeHtml(st.config.output_sym)}] ≈ ${fmtSci(data.achieved_level)}` +
      (data.feasible ? '' : ' <span style="color:var(--status-error);">(not reachable in range)</span>');
    if (data.dose_response_curve) {
      plotParameterScan1D(data.dose_response_curve, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }
  } catch (e) {
    if (note) note.innerHTML = `<span class="node-error">${escapeHtml(e.message)}</span>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

async function livePlacerTune(nodeId) {
  const st = ensureNodeData(nodeId).placerResult;
  if (!st) return;
  const sel = document.getElementById(`${nodeId}-kdsel`);
  const slider = document.getElementById(`${nodeId}-kdslider`);
  const valEl = document.getElementById(`${nodeId}-kdval`);
  if (!sel || !slider) return;
  const idx = parseInt(sel.value, 10) || 0;
  const newKd = st.kd.slice();
  newKd[idx] = Math.pow(10, parseFloat(slider.value));
  if (valEl) valEl.textContent = `Kd${idx + 1} = ${fmtSci(newKd[idx])}`;
  try {
    const curve = await api('placer_curve', {
      session_id: st.sessionId, input_sym: st.config.input_sym, output_sym: st.config.output_sym,
      kd: newKd, totals: st.totals,
    });
    st.kd = newKd;
    plotParameterScan1D(curve, `${nodeId}-plot`);
    setupPlotResize(nodeId, `${nodeId}-plot`);
  } catch (e) {
    console.warn('live tune failed:', e.message);
  }
}

function passBadge(pass) {
  return pass ? '<span style="color:var(--status-ok);font-weight:600;">PASS</span>'
              : '<span style="color:var(--status-error);font-weight:600;">FAIL</span>';
}
function fmtSci(x) {
  const n = Number(x);
  if (!Number.isFinite(n)) return String(x);
  return (n !== 0 && (Math.abs(n) < 1e-3 || Math.abs(n) >= 1e4)) ? n.toExponential(3) : Number(n.toPrecision(4)).toString();
}
function fmtNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? (Number.isInteger(n) ? String(n) : n.toFixed(3)) : escapeHtml(String(x));
}
function fmtSeq(arr) {
  return (arr || []).map(fmtNum).join(' → ') || '∅';
}
function signArrows(s) {
  return String(s || '').split('').join(' → ') || '∅';
}

// Biocircuits Explorer — Design Agent view
// ---------------------------------------------------------------------------
// The conversational inverse-design surface. The left pane (chat) is the *front
// end*: describe the behavior you want and the agent compiles it to a behavior
// spec, retrieves verified candidates from the atlas, and returns reaction
// networks with their evidence. It POSTs each turn to the design-chat backend
// (webapp/scripts/chat_api.py — see chatApiUrl()); a backend-status pill at the
// top of the chat reflects /health. The right pane shows the top candidate: a
// per-family visualisation (truth table / surface bars / context strip /
// illustrative dose curve synthesised from the candidate's real metrics) + its
// reaction rules. Nothing here is canned — the first real reply comes from the
// backend; without one the agent shows an actionable offline state.
//
// As with editor-ui.js, all DOM is created programmatically — index-node.html
// only carries the header view-switch markup and the stylesheet link.

import { getLLMConfig } from './llm-settings.js';   // UI key panel -> per-request LLM config

const SVG_NS = 'http://www.w3.org/2000/svg';
const CHATW_KEY = 'bcx-agent-chatw';
const VIEW_KEY = 'bcx-node-view';
const CHAT_API_KEY = 'bcx-chat-api';
const DEFAULT_CHATW = 440;
const DEFAULT_CHAT_API = 'http://127.0.0.1:8765/design-chat';

// Backend chat endpoint (webapp/scripts/chat_api.py), resolved lazily each call so
// the native macOS shell can pin the real port after the page has loaded:
//   window.__BCX_CHAT_API__ (set by setDesignChatEndpoint) > localStorage > default.
function chatApiUrl() {
  if (typeof window !== 'undefined' && window.__BCX_CHAT_API__) return window.__BCX_CHAT_API__;
  try { return localStorage.getItem(CHAT_API_KEY) || DEFAULT_CHAT_API; }
  catch (_) { return DEFAULT_CHAT_API; }
}
function healthUrl() {
  try { return new URL('/health', chatApiUrl()).toString(); }
  catch (_) { return DEFAULT_CHAT_API.replace('/design-chat', '/health'); }
}

// Let the native shell (or the dev console) point the agent at a specific backend.
export function setDesignChatEndpoint(url) {
  if (!url) return;
  window.__BCX_CHAT_API__ = String(url);
  try { localStorage.setItem(CHAT_API_KEY, String(url)); } catch (_) { /* ignore */ }
  refreshBackendStatus();   // re-probe so the status pill reflects the new target
}
if (typeof window !== 'undefined') window.setDesignChatEndpoint = setDesignChatEndpoint;

let agentBuilt = false;
let threadEl = null;
let resultsRulesEl = null;   // the right-pane rules list, updated to the top candidate
let resultsChartEl = null;   // the right-pane chart-wrap, updated to a per-candidate viz
let resultsChartTitleEl = null;
let statusDotEl = null;      // backend-health pill (dot + text) at the top of the chat
let statusTextEl = null;
let chatState = {};          // conversation spec state, echoed to the backend each turn
let activeCandidate = null;  // the candidate currently shown on the right (export target)
let exportBtnEl = null;      // "Export to Workspace" button (enabled once a candidate is active)
let convoLog = [];           // re-renderable turn log {role:'user',text} | {role:'agent',res} —
                             // persisted WITH the workspace document so each project carries its
                             // own Design-Agent conversation (one project = one workspace + one chat).

function setActiveCandidate(card) {
  activeCandidate = card || null;
  if (exportBtnEl) exportBtnEl.disabled = !(card && (card.rules || []).length);
}

// (A,B) corner order (00,01,10,11), output high=1 — mirrors evaluators.jl / cards.py.
const LOGIC_TABLES = {
  AND: [0,0,0,1], OR: [0,1,1,1], NAND: [1,1,1,0], NOR: [1,0,0,0], XOR: [0,1,1,0], XNOR: [1,0,0,1],
  NIMPLY: [0,0,1,0], IMPLY: [1,1,0,1], NOT_A: [1,1,0,0], NOT_B: [1,0,1,0], A: [0,0,1,1], B: [0,1,0,1],
  CIMPLY: [1,0,1,1], BNIMPLY: [0,1,0,0], TRUE: [1,1,1,1], FALSE: [0,0,0,0],
};
function gateTable(gate) {
  if (LOGIC_TABLES[gate]) return LOGIC_TABLES[gate];
  const m = String(gate || '').match(/\(([\d,\s]+)\)/);   // legacy "none(1, 0, 1, 1)"
  return m ? m[1].split(',').map((x) => Number(x.trim())) : null;
}

/* ─── tiny DOM helpers ─── */
function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null) continue;
    if (k === 'class') node.className = v;
    else if (k === 'html') node.innerHTML = v;
    else if (k === 'text') node.textContent = v;
    else if (k === 'dataset') Object.assign(node.dataset, v);
    else if (k.startsWith('on') && typeof v === 'function') node.addEventListener(k.slice(2).toLowerCase(), v);
    else node.setAttribute(k, v);
  }
  for (const c of [].concat(children)) {
    if (c == null) continue;
    node.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
  }
  return node;
}
function svgEl(tag, attrs = {}) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v != null) node.setAttribute(k, String(v));
  }
  return node;
}

function placeholder(text) {
  return el('div', { class: 'agent-placeholder', text });
}

function buildResultsPanel() {
  // The right pane starts empty; showCandidateViz / showCandidateRules fill it in
  // from the top candidate of a real backend reply (no fabricated seed data).
  resultsChartTitleEl = el('span', { class: 'card-title', text: 'Response curve' });
  resultsChartEl = el('div', { class: 'chart-wrap' },
    placeholder('Describe a behavior on the left — the top candidate’s response curve appears here.'));
  const chartCard = el('div', { class: 'card chart-card' }, [
    el('div', { class: 'card-head' }, resultsChartTitleEl),
    resultsChartEl,
  ]);

  resultsRulesEl = el('div', { class: 'rules-list' },
    placeholder('Reaction rules appear here once a candidate is found.'));
  // Export the active (engine-verified) candidate into the node Workspace to analyse it there.
  exportBtnEl = el('button', { class: 'export-ws-btn', type: 'button', text: 'Export to Workspace ↗',
    title: 'Open this design as a Reaction-Network node in the Workspace' });
  exportBtnEl.disabled = true;
  exportBtnEl.addEventListener('click', () => {
    if (activeCandidate && typeof window.exportNetworkToWorkspace === 'function') {
      window.exportNetworkToWorkspace(activeCandidate.rules, activeCandidate.kd);
    }
  });
  const rulesCard = el('div', { class: 'card rules-card' }, [
    el('div', { class: 'card-head' }, [el('span', { class: 'card-title', text: 'Reaction rules' }), exportBtnEl]),
    resultsRulesEl,
  ]);

  return el('div', { class: 'agent-results' }, el('div', { class: 'results-inner' }, [chartCard, rulesCard]));
}

/* ─── conversation ─── */
// An honest opening: what the agent does and how to phrase a request. No
// fabricated conversation or results — the first real reply comes from the backend.
function welcomeMessage() {
  return {
    role: 'agent',
    text: 'I’m the Biocircuits design agent. Describe the <b>behavior</b> you want from a binding network and I’ll compile it to a behavior spec, search the verified atlas, and return candidate reaction networks with their evidence.',
    closing: 'Try: <i>“a bandpass response with a gentle rise, sharp fall and a wide plateau, at most 4 reactions”</i> · <i>“an AND gate on inputs A and B”</i> · <i>“a ratio sensor for A versus B”</i>. Add an LLM key in the ⚙ panel for free-form phrasing — optional, keyword parsing works without it.',
  };
}

/* ─── backend status pill ─── */
function buildStatusBar() {
  statusDotEl = el('span', { class: 'agent-status-dot' });
  statusTextEl = el('span', { class: 'agent-status-text', text: 'Checking backend…' });
  return el('div', { class: 'agent-status', title: 'Design backend (webapp/scripts/chat_api.py)' }, [statusDotEl, statusTextEl]);
}

async function refreshBackendStatus() {
  if (!statusDotEl) return;   // pill not built yet (e.g. endpoint set before first paint)
  statusDotEl.className = 'agent-status-dot checking';
  if (statusTextEl) statusTextEl.textContent = 'Checking backend…';
  try {
    const res = await fetch(healthUrl(), { method: 'GET' });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const h = await res.json();
    // The agent can only return VERIFIED designs when the live compute engine is up; surface
    // that distinctly from the chat backend (the engine does the real ODE/equilibrium solves).
    const engineReady = !(h && h.engine) || h.engine.ready;
    if (engineReady) {
      statusDotEl.className = 'agent-status-dot online';
      if (statusTextEl) statusTextEl.textContent = 'Agent ready';
    } else {
      statusDotEl.className = 'agent-status-dot checking';
      if (statusTextEl) statusTextEl.textContent = 'Chat up, but COMPUTE ENGINE OFFLINE — start the node Workspace server to get verified designs';
    }
  } catch (_) {
    statusDotEl.className = 'agent-status-dot offline';
    if (statusTextEl) statusTextEl.textContent = 'Backend offline — start chat_api.py';
  }
}

function buildMessage(m) {
  if (m.role === 'user') {
    // User text is rendered as textContent (never innerHTML) — no injection.
    return el('div', { class: 'msg user' }, el('div', { class: 'bubble', text: m.text }));
  }

  const parts = [el('div', { class: 'who' }, [el('span', { class: 'dot' }), 'Design Agent'])];
  // m.text / m.closing here are trusted CONSTANT copy (welcome / pending) with inline
  // <b>/<i> emphasis. Dynamic backend text is rendered via textContent elsewhere
  // (buildReplyMessage, candCard, and the submit() error path) — never innerHTML.
  if (m.text) parts.push(el('div', { class: 'agent-text', html: m.text }));
  if (m.closing) parts.push(el('div', { class: 'agent-text', html: m.closing }));

  return el('div', { class: 'msg agent' }, parts);
}

function scrollThreadToBottom() {
  if (threadEl) threadEl.scrollTop = threadEl.scrollHeight;
}

function appendMessage(m) {
  if (!threadEl) return;
  threadEl.appendChild(buildMessage(m));
  scrollThreadToBottom();
}

/* ─── live backend reply rendering ─── */
function rxnChips(rules, networkId, kd) {
  const list = (rules && rules.length) ? rules : (networkId ? [networkId] : []);
  if (!list.length) return null;
  const kds = Array.isArray(kd) ? kd : [];
  return el('div', { class: 'rxn-inline' }, list.map((r, i) =>
    el('span', { class: 'rxn-chip' }, [
      String(r).replace(/<->|<=>/g, '⇌'),
      (kds[i] != null ? el('span', { class: 'chip-kd', text: ' · Kd ' + kds[i] }) : null),
    ])));
}

function candCard(card, family) {
  let head, meta = '';
  if (family === 'logic') {
    head = `${card.realized_gate} gate · ${(card.inputs || []).join(',')}→${card.output}`;
    meta = `support ${card.gate_support} · margin ${card.margin_decades} dec`;
  } else if (family === 'analog_surface') {
    head = `${card.kind} · ${(card.inputs || []).join(',')}→${card.output}`;
    meta = `coact ${card.coactivation_corr} · ratio ${card.ratio_corr} · bump ${card.bump_fraction} · ${card.dynamic_range_decades} dec`;
  } else if (family === 'contextual_versatility') {
    head = `reprogrammable: ${(card.distinct_gates || []).join(' / ')} · ${(card.inputs || []).join(',')}→${card.output}`;
    meta = (card.per_context || []).map((p) => `${card.context_sym}=${p.context}:${p.gate}(${p.support})`).join('  ');
  } else {
    head = `${card.dominant_shape || card.verdict || 'computed'} · r=${card.n_reactions} → ${card.output_symbol}`;
    const bits = [];
    if (card.shape_support != null) bits.push(`shape_support ${card.shape_support}`);
    if (card.evidence_tier) bits.push(card.evidence_tier);
    meta = bits.join(' · ');
  }
  const parts = [el('div', { class: 'cand-head', text: head })];
  if (meta) parts.push(el('div', { class: 'cand-meta', text: meta }));
  const chips = rxnChips(card.rules, card.network_id, card.kd);
  if (chips) parts.push(chips);
  return el('div', { class: 'cand-card fam-' + family }, parts);
}

/* ─── per-candidate visualisation for the results pane ─── */
// "nice" round tick values spanning [min,max] (~target of them) + a compact label formatter.
function niceTicks(min, max, target) {
  const span = max - min;
  if (!(span > 0) || !isFinite(span)) return [min];
  const raw = span / Math.max(1, target);
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const n = raw / mag;
  const step = (n < 1.5 ? 1 : n < 3 ? 2 : n < 7 ? 5 : 10) * mag;
  const out = [];
  for (let v = Math.ceil(min / step) * step; v <= max + 1e-9; v += step) out.push(Math.abs(v) < 1e-9 ? 0 : v);
  return out.length ? out : [min, max];
}
function fmtTick(v) {
  if (v !== 0 && (Math.abs(v) >= 1000 || Math.abs(v) < 0.01)) return v.toExponential(0);
  return String(Math.round(v * 100) / 100);
}
// dark-blue → teal → yellow colour ramp for the 2-input surface heatmap
function _heatColor(t) {
  t = Math.max(0, Math.min(1, isFinite(t) ? t : 0));
  const stops = [[0, [21, 35, 58]], [0.5, [23, 136, 156]], [1, [232, 210, 74]]];
  let a = stops[0], b = stops[stops.length - 1];
  for (let i = 0; i < stops.length - 1; i++) { if (t >= stops[i][0] && t <= stops[i + 1][0]) { a = stops[i]; b = stops[i + 1]; break; } }
  const f = (t - a[0]) / ((b[0] - a[0]) || 1);
  const c = a[1].map((v, k) => Math.round(v + (b[1][k] - v) * f));
  return `rgb(${c[0]},${c[1]},${c[2]})`;
}
// Real 2-input response surface (engine-computed) as a heatmap with axes, units + a colourbar.
function buildHeatmap(card) {
  const s = card.surface;
  const z = s && Array.isArray(s.z) ? s.z : [];
  const flat = z.flat().filter((v) => isFinite(v));
  if (!flat.length) return placeholder('No engine-computed surface for this candidate.');
  const xs = s.x, ys = s.y, nx = xs.length, ny = ys.length;
  const zmin = Math.min(...flat), zmax = Math.max(...flat), zr = (zmax - zmin) || 1;
  const W = 640, H = 360, padL = 58, padR = 92, padT = 16, padB = 48;
  const plotW = W - padL - padR, plotH = H - padT - padB, cw = plotW / nx, ch = plotH / ny;
  const svg = svgEl('svg', { viewBox: `0 0 ${W} ${H}` });
  for (let i = 0; i < nx; i++) for (let j = 0; j < ny; j++) {
    const v = z[i][j]; if (!isFinite(v)) continue;
    svg.appendChild(svgEl('rect', { x: (padL + i * cw).toFixed(1), y: (padT + (ny - 1 - j) * ch).toFixed(1),
      width: (cw + 0.6).toFixed(1), height: (ch + 0.6).toFixed(1), fill: _heatColor((v - zmin) / zr) }));
  }
  const xMin = xs[0], xMax = xs[nx - 1], yMin = ys[0], yMax = ys[ny - 1];
  const sx = (x) => padL + (x - xMin) / ((xMax - xMin) || 1) * plotW;
  const sy = (y) => padT + (yMax - y) / ((yMax - yMin) || 1) * plotH;
  niceTicks(xMin, xMax, 6).forEach((t) => { const X = sx(t); const l = svgEl('text', { class: 'tick-text', x: X, y: padT + plotH + 15, 'text-anchor': 'middle' }); l.textContent = fmtTick(t); svg.appendChild(l); });
  niceTicks(yMin, yMax, 5).forEach((t) => { const Y = sy(t); const l = svgEl('text', { class: 'tick-text', x: padL - 8, y: Y + 3, 'text-anchor': 'end' }); l.textContent = fmtTick(t); svg.appendChild(l); });
  svg.appendChild(svgEl('line', { class: 'axis-line', x1: padL, y1: padT + plotH, x2: padL + plotW, y2: padT + plotH }));
  svg.appendChild(svgEl('line', { class: 'axis-line', x1: padL, y1: padT, x2: padL, y2: padT + plotH }));
  const xt = svgEl('text', { class: 'axis-title', x: padL + plotW / 2, y: H - 8, 'text-anchor': 'middle' }); xt.textContent = 'log₁₀ ' + (s.input1 || 'input 1'); svg.appendChild(xt);
  const ymid = padT + plotH / 2; const yt = svgEl('text', { class: 'axis-title', x: 14, y: ymid, 'text-anchor': 'middle', transform: `rotate(-90 14 ${ymid})` }); yt.textContent = 'log₁₀ ' + (s.input2 || 'input 2'); svg.appendChild(yt);
  // colourbar
  const cbx = padL + plotW + 24, cbw = 12, cbh = plotH;
  const defs = svgEl('defs'), lg = svgEl('linearGradient', { id: 'hmgrad', x1: '0', y1: '1', x2: '0', y2: '0' });
  for (let k = 0; k <= 8; k++) lg.appendChild(svgEl('stop', { offset: (k / 8 * 100) + '%', 'stop-color': _heatColor(k / 8) }));
  defs.appendChild(lg); svg.appendChild(defs);
  svg.appendChild(svgEl('rect', { x: cbx, y: padT, width: cbw, height: cbh, fill: 'url(#hmgrad)' }));
  const ct = svgEl('text', { class: 'tick-text', x: cbx + cbw + 4, y: padT + 9, 'text-anchor': 'start' }); ct.textContent = fmtTick(zmax); svg.appendChild(ct);
  const cb = svgEl('text', { class: 'tick-text', x: cbx + cbw + 4, y: padT + cbh, 'text-anchor': 'start' }); cb.textContent = fmtTick(zmin); svg.appendChild(cb);
  const cl = svgEl('text', { class: 'axis-title', x: cbx + cbw / 2, y: padT - 5, 'text-anchor': 'middle' }); cl.textContent = 'log₁₀[' + (s.observe || 'out') + ']'; svg.appendChild(cl);
  return el('div', { class: 'cand-viz' }, [el('div', { class: 'chart' }, svg)]);
}
function buildCandidateViz(card, family) {
  if (card && card.surface) return buildHeatmap(card);   // engine-computed 2-input surface
  if (family === 'logic') {
    const t = gateTable(card.realized_gate) || [0, 0, 0, 0];
    const cell = (v) => el('div', { class: 'tt-cell ' + (v ? 'hi' : 'lo'), text: String(v) });
    const lbl = (s) => el('div', { class: 'tt-cell lbl', text: s });
    const grid = el('div', { class: 'tt-grid' }, [
      lbl(''), lbl('B=lo'), lbl('B=hi'),
      lbl('A=lo'), cell(t[0]), cell(t[1]),
      lbl('A=hi'), cell(t[2]), cell(t[3]),
    ]);
    return el('div', { class: 'cand-viz' }, [grid]);
  }
  if (family === 'analog_surface') {
    const bar = (name, v) => {
      const fill = el('div', { class: 'bar-fill' }); fill.style.width = Math.round(Math.min(1, Math.abs(v || 0)) * 100) + '%';
      return el('div', { class: 'bar-row' }, [el('span', { text: name }), el('div', { class: 'bar-track' }, fill), el('span', { text: String(v) })]);
    };
    return el('div', { class: 'cand-viz' }, [
      bar('coactivation', card.coactivation_corr), bar('ratio (A/B)', card.ratio_corr), bar('interior bump', card.bump_fraction),
    ]);
  }
  if (family === 'contextual_versatility') {
    const pills = (card.per_context || []).map((p) =>
      el('span', { class: 'ctx-pill' }, [`${card.context_sym}=${p.context} → `, el('b', { text: p.gate }), ` (${p.support})`]));
    return el('div', { class: 'cand-viz' }, [el('div', { class: 'ctx-strip' }, pills)]);
  }
  // dose_shape: plot the REAL engine-computed curve. There is NO synthetic fallback — if no
  // computed curve is present we say so rather than drawing a fabricated one.
  const cs = Array.isArray(card.computed_series)
    ? card.computed_series.map((p) => [Number(p.x), Number(p.y)]).filter((p) => isFinite(p[0]) && isFinite(p[1]))
    : [];
  if (cs.length < 2) {
    return el('div', { class: 'cand-viz' }, placeholder('No engine-computed curve for this candidate.'));
  }
  const W = 640, H = 320, padL = 60, padR = 18, padT = 14, padB = 50;
  const xs = cs.map((p) => p[0]), ys = cs.map((p) => p[1]);
  let xMin = Math.min(...xs), xMax = Math.max(...xs), yMin = Math.min(...ys), yMax = Math.max(...ys);
  if (xMax - xMin < 1e-9) { xMin -= 1; xMax += 1; }
  const yp = (yMax - yMin < 1e-9) ? 1 : (yMax - yMin) * 0.08; yMin -= yp; yMax += yp;
  const x0 = padL, y0 = H - padB, x1 = W - padR, yTop = padT;
  const sx = (x) => x0 + (x - xMin) / (xMax - xMin) * (x1 - x0);
  const sy = (y) => yTop + (yMax - y) / (yMax - yMin) * (y0 - yTop);
  const svg = svgEl('svg', { viewBox: `0 0 ${W} ${H}` });
  // gridlines + numeric ticks — X (log10 input)
  niceTicks(xMin, xMax, 6).forEach((t) => {
    const X = sx(t);
    svg.appendChild(svgEl('line', { class: 'grid-line', x1: X, y1: yTop, x2: X, y2: y0 }));
    const lab = svgEl('text', { class: 'tick-text', x: X, y: y0 + 15, 'text-anchor': 'middle' });
    lab.textContent = fmtTick(t); svg.appendChild(lab);
  });
  // gridlines + numeric ticks — Y (log10 output concentration)
  niceTicks(yMin, yMax, 5).forEach((t) => {
    const Y = sy(t);
    svg.appendChild(svgEl('line', { class: 'grid-line', x1: x0, y1: Y, x2: x1, y2: Y }));
    const lab = svgEl('text', { class: 'tick-text', x: x0 - 8, y: Y + 3, 'text-anchor': 'end' });
    lab.textContent = fmtTick(t); svg.appendChild(lab);
  });
  // axes
  svg.appendChild(svgEl('line', { class: 'axis-line', x1: x0, y1: y0, x2: x1, y2: y0 }));
  svg.appendChild(svgEl('line', { class: 'axis-line', x1: x0, y1: yTop, x2: x0, y2: y0 }));
  // the real engine-computed curve
  let d = ''; cs.forEach((p, i) => { d += (i ? 'L' : 'M') + sx(p[0]).toFixed(1) + ' ' + sy(p[1]).toFixed(1) + ' '; });
  svg.appendChild(svgEl('path', { class: 'agent-series target', pathLength: '1', d: d.trim(), stroke: '#17c4d6', fill: 'none' }));
  // axis titles (units)
  const xt = svgEl('text', { class: 'axis-title', x: (x0 + x1) / 2, y: H - 8, 'text-anchor': 'middle' });
  xt.textContent = ('log₁₀ input total ' + (card.input_symbol || '')).trim(); svg.appendChild(xt);
  const ymid = (y0 + yTop) / 2;
  const yt = svgEl('text', { class: 'axis-title', x: 14, y: ymid, 'text-anchor': 'middle', transform: `rotate(-90 14 ${ymid})` });
  yt.textContent = 'log₁₀ [' + (card.output_symbol || 'output') + ']'; svg.appendChild(yt);
  return el('div', { class: 'cand-viz' }, [el('div', { class: 'chart' }, svg)]);
}
function showCandidateViz(card, family) {
  if (!resultsChartEl || !card) return;
  resultsChartEl.replaceChildren(buildCandidateViz(card, family));
  if (resultsChartTitleEl) resultsChartTitleEl.textContent = 'Top candidate';
}

function showCandidateRules(card) {
  if (!resultsRulesEl || !card) return;
  const rules = (card.rules && card.rules.length) ? card.rules : (card.network_id ? [card.network_id] : []);
  const kd = Array.isArray(card.kd) ? card.kd : [];
  resultsRulesEl.replaceChildren(...rules.map((r, i) =>
    el('div', { class: 'rule' }, [
      el('span', { class: 'eq', text: String(r).replace(/<->|<=>/g, '⇌') }),
      (kd[i] != null ? el('span', { class: 'kd' }, ['Kd ', el('b', { text: String(kd[i]) })]) : null),
    ])));
}

// Make a candidate card the active one: highlight it among its siblings and drive the right pane.
function selectCandidate(cardEl, card, family) {
  const scope = cardEl.closest('.msg.agent') || cardEl.parentNode;
  if (scope) scope.querySelectorAll('.cand-card.active').forEach((e) => e.classList.remove('active'));
  cardEl.classList.add('active');
  setActiveCandidate(card);
  showCandidateRules(card);
  showCandidateViz(card, family);
}

function buildReplyMessage(res) {
  const parts = [el('div', { class: 'who' }, [el('span', { class: 'dot' }), 'Design Agent'])];
  // The agent's natural-language reply IS the content (kind: agent / chat / need_key / error).
  // No "Compiled → …" prefix and no fabricated notes — the LLM wrote this, grounded in tool results.
  if (res.reply) parts.push(el('div', { class: 'agent-text', text: res.reply }));
  const info = res.info || {};
  if (info.engine_offline) {
    parts.push(el('div', { class: 'agent-abstain', text: '⚠ Compute engine offline — no verified design produced. Start the node Workspace server, then retry.' }));
  }
  // Cards shown are ENGINE-VERIFIED candidates; CLICK one to drive the right-pane viz + rules.
  const fam = res.family || 'dose_shape';
  const cards = res.cards || [];
  const explored = (res.info && res.info.explored) || 0;
  if (cards.length > 1) {
    parts.push(el('div', { class: 'cand-hint', text: explored > cards.length
      ? `explored ${explored} designs · showing ${cards.length} best — click a card to view it`
      : `${cards.length} verified designs — click a card to view it` }));
  }
  cards.forEach((c, idx) => {
    const cc = candCard(c, fam);
    cc.addEventListener('click', () => selectCandidate(cc, c, c.family || fam));
    if (idx === 0) cc.classList.add('active');
    parts.push(cc);
  });
  return el('div', { class: 'msg agent' }, parts);
}

async function sendToBackend(text) {
  const res = await fetch(chatApiUrl(), {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message: text, state: chatState, llm: getLLMConfig(), top: 3 }),
  });
  if (!res.ok) throw new Error('HTTP ' + res.status + ' ' + (await res.text()).slice(0, 140));
  return res.json();
}

/* ─── composer ─── */
function buildComposer() {
  const ta = el('textarea', {
    rows: '1',
    'aria-label': 'Message the design agent',
    placeholder: 'Describe the behavior you want, or your available parts…',
  });
  const sendBtn = el('button', { class: 'send-btn', disabled: '', text: 'Send' });

  const grow = () => {
    ta.style.height = 'auto';
    ta.style.height = Math.min(ta.scrollHeight, 120) + 'px';
    sendBtn.disabled = !ta.value.trim();
  };
  const submit = async () => {
    const text = ta.value.trim();
    if (!text) return;
    appendMessage({ role: 'user', text });
    convoLog.push({ role: 'user', text });
    ta.value = ''; ta.style.height = 'auto'; sendBtn.disabled = true;
    const pending = buildMessage({ role: 'agent', text: 'Searching the atlas…' });
    threadEl.appendChild(pending); scrollThreadToBottom();
    try {
      const res = await sendToBackend(text);
      chatState = res.state || chatState;
      threadEl.replaceChild(buildReplyMessage(res), pending);
      convoLog.push({ role: 'agent', res });
      if (convoLog.length > 60) convoLog = convoLog.slice(-60);
      const first = (res.cards || [])[0];
      if (first) { setActiveCandidate(first); showCandidateRules(first); showCandidateViz(first, first.family || res.family); }
      else { setActiveCandidate(null); }
      refreshBackendStatus();
    } catch (e) {
      // Guidance is trusted constant copy (html); the error detail can echo a
      // backend/proxy response body, so render it as textContent — never innerHTML.
      const errMsg = el('div', { class: 'msg agent' }, [
        el('div', { class: 'who' }, [el('span', { class: 'dot' }), 'Design Agent']),
        el('div', { class: 'agent-text', html: 'Backend unreachable — start it with <b>python3 webapp/scripts/chat_api.py</b> (default 127.0.0.1:8765). An LLM key (⚙ panel) is optional; keyword parsing works without one.' }),
        el('div', { class: 'agent-text', text: String(e.message || e) }),
      ]);
      threadEl.replaceChild(errMsg, pending);
      refreshBackendStatus();
    }
    scrollThreadToBottom();
  };

  ta.addEventListener('input', grow);
  ta.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); submit(); }
  });
  sendBtn.addEventListener('click', submit);

  return el('div', { class: 'agent-composer' }, el('div', { class: 'composer-box' }, [ta, sendBtn]));
}

/* ─── splitter ─── */
function clampChatWidth(w, host) {
  // Keep the chat between 320px and (viewport − 360px) so the results pane
  // never collapses — used for both the initial paint and live dragging.
  return Math.max(320, Math.min(w, host.clientWidth - 360));
}

function readChatWidth() {
  let saved = 0;
  try { saved = Number(localStorage.getItem(CHATW_KEY)); } catch (_) { /* ignore */ }
  return saved && saved > 300 ? saved : DEFAULT_CHATW;
}

function installSplitter(host, chat, split) {
  let dragging = false;
  const onMove = (e) => {
    if (!dragging) return;
    const left = host.getBoundingClientRect().left;
    chat.style.width = clampChatWidth(e.clientX - left, host) + 'px';
  };
  const onUp = () => {
    if (!dragging) return;
    dragging = false;
    split.classList.remove('dragging');
    const w = parseInt(chat.style.width, 10);
    if (w) { try { localStorage.setItem(CHATW_KEY, String(w)); } catch (_) { /* ignore */ } }
  };
  split.addEventListener('mousedown', (e) => {
    e.preventDefault();
    dragging = true;
    split.classList.add('dragging');
  });
  window.addEventListener('mousemove', onMove);
  window.addEventListener('mouseup', onUp);
}

/* ─── view assembly + switching ─── */
function buildAgentView() {
  const chat = el('div', { class: 'agent-chat' });
  chat.appendChild(buildStatusBar());
  threadEl = el('div', { class: 'agent-thread' });
  threadEl.appendChild(buildMessage(welcomeMessage()));
  chat.appendChild(threadEl);
  chat.appendChild(buildComposer());
  refreshBackendStatus();   // probe /health so the pill shows connected/offline up front

  const split = el('div', { class: 'agent-split' });
  const host = el('div', { id: 'agent-view' }, [chat, split, buildResultsPanel()]);

  // Place the agent view as a sibling of #editor so the shared header sits above it.
  const editor = document.getElementById('editor');
  if (editor && editor.parentNode) editor.parentNode.insertBefore(host, editor.nextSibling);
  else document.body.appendChild(host);

  // Width is set after insertion (setNodeView flips data-node-view first, so the
  // host is laid out) and clamped to the live viewport — a value persisted on a
  // wide display can't squeeze the results pane to zero on a narrow one.
  chat.style.width = clampChatWidth(readChatWidth(), host) + 'px';

  installSplitter(host, chat, split);
  scrollThreadToBottom();
}

function ensureAgentBuilt() {
  if (agentBuilt) return;
  buildAgentView();
  agentBuilt = true;
}

// ─── Per-project conversation persistence ─────────────────────────────────────
// The workspace document carries the Design-Agent conversation, so each project =
// one workspace + one conversation. workspace.js serializeState()/applyState() call these.
function getDesignAgentConversation() {
  return { convo: convoLog, chatState };
}
function setDesignAgentConversation(rec) {
  ensureAgentBuilt();
  if (!threadEl) return;
  convoLog = (rec && Array.isArray(rec.convo)) ? rec.convo : [];
  chatState = (rec && rec.chatState) ? rec.chatState : {};
  threadEl.replaceChildren();
  if (!convoLog.length) {
    threadEl.appendChild(buildMessage(welcomeMessage()));
  } else {
    let lastCard = null, lastFam = null;
    for (const e of convoLog) {
      if (e.role === 'user') threadEl.appendChild(buildMessage({ role: 'user', text: e.text }));
      else if (e.role === 'agent' && e.res) {
        threadEl.appendChild(buildReplyMessage(e.res));
        const f = (e.res.cards || [])[0];
        if (f) { lastCard = f; lastFam = f.family || e.res.family; }
      }
    }
    if (lastCard) { setActiveCandidate(lastCard); showCandidateRules(lastCard); showCandidateViz(lastCard, lastFam); }
  }
  if (!convoLog.length || !activeCandidate) {
    setActiveCandidate(null);
    if (resultsRulesEl) resultsRulesEl.replaceChildren(placeholder('Reaction rules appear here once a candidate is found.'));
    if (resultsChartEl) resultsChartEl.replaceChildren(placeholder('Describe a behavior on the left — the top candidate’s response curve appears here.'));
  }
  scrollThreadToBottom();
}
if (typeof window !== 'undefined') {
  window.getDesignAgentConversation = getDesignAgentConversation;
  window.setDesignAgentConversation = setDesignAgentConversation;
}

export function setNodeView(view) {
  const v = view === 'agent' ? 'agent' : 'workspace';

  // Flip the surface attribute first so #agent-view is laid out (display:flex)
  // before we build into it — keeps width clamping / measurements correct.
  document.documentElement.dataset.nodeView = v;
  if (v === 'agent') ensureAgentBuilt();

  const agentBtn = document.getElementById('view-switch-agent');
  const wsBtn = document.getElementById('view-switch-workspace');
  if (agentBtn) { agentBtn.classList.toggle('active', v === 'agent'); agentBtn.setAttribute('aria-pressed', String(v === 'agent')); }
  if (wsBtn) { wsBtn.classList.toggle('active', v === 'workspace'); wsBtn.setAttribute('aria-pressed', String(v === 'workspace')); }

  try { localStorage.setItem(VIEW_KEY, v); } catch (_) { /* ignore */ }

  if (v === 'agent') {
    scrollThreadToBottom();
  } else {
    // The canvas was display:none while hidden; nudge size-sensitive bits
    // (grid canvas, Plotly viewers) to recompute now that it is visible again.
    window.dispatchEvent(new Event('resize'));
  }
}

export function initAgentView() {
  const agentBtn = document.getElementById('view-switch-agent');
  const wsBtn = document.getElementById('view-switch-workspace');
  if (agentBtn) agentBtn.addEventListener('click', () => setNodeView('agent'));
  if (wsBtn) wsBtn.addEventListener('click', () => setNodeView('workspace'));

  // The inline bootstrap in index-node.html already set documentElement's
  // data-node-view (from hash / localStorage). Honour it: build + sync now.
  const initial = document.documentElement.dataset.nodeView === 'agent' ? 'agent' : 'workspace';
  setNodeView(initial);
}

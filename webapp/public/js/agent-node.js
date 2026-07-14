// Biocircuits Explorer — AI Import node runtime.
//
// This module owns the event wiring, run state, and downstream-spawning
// logic for the `ai-import` red input node defined in node-types/input.js.
// The node itself never serialises an API key or the source content — its
// purpose is to translate a paper/notebook into populated reaction-network
// + model-builder + analysis chains, then step out of the way.

import { runAgent, detectProvider } from './agent.js';
import { createNode, resolveOverlap, getNodePosition, getNodeSize } from './nodes.js';
import { addReactionRow } from './model.js';
import { commitWorkspaceSnapshot } from './workspace.js';
import { connections, nodeRegistry } from './state.js';
import { updateConnections } from './connections.js';
import { showToast } from './api.js';
import { writeClipboardText } from './native-clipboard.js';

const SESSION_KEY_STORAGE = 'biocircuits.agent.apiKey';
const SESSION_PROVIDER_STORAGE = 'biocircuits.agent.provider';
const SESSION_MODEL_STORAGE = 'biocircuits.agent.model';

export const AGENT_MODEL_OPTIONS = [
  { value: 'claude-opus-4-7',            label: 'Claude Opus 4.7  (most capable)',         provider: 'anthropic' },
  { value: 'claude-sonnet-4-6',          label: 'Claude Sonnet 4.6  (balanced)',           provider: 'anthropic' },
  { value: 'claude-haiku-4-5-20251001',  label: 'Claude Haiku 4.5  (fastest)',             provider: 'anthropic' },
  { value: 'deepseek-v4-pro',            label: 'DeepSeek V4 Pro  (Anthropic-compat)',     provider: 'deepseek' },
  { value: 'deepseek-v4-flash',          label: 'DeepSeek V4 Flash  (Anthropic-compat)',   provider: 'deepseek' },
  { value: '__custom__',                 label: 'Custom…',                                 provider: null },
];

// Each spawnable analysis maps to one or two real node types.
// 'simple' kinds attach directly to the model-builder output.
// 'pair' kinds need a params node between the model and the result.
const CHAIN_MAP = {
  'model-summary':     { kind: 'simple', result: 'model-summary' },
  'vertices-table':    { kind: 'simple', result: 'vertices-table' },
  'regime-graph':      { kind: 'simple', result: 'regime-graph' },
  'siso-analysis':     { kind: 'pair',   params: 'siso-params',    result: 'siso-result' },
  'parameter-scan-1d': { kind: 'pair',   params: 'scan-1d-params', result: 'scan-1d-result' },
  'parameter-scan-2d': { kind: 'pair',   params: 'scan-2d-params', result: 'scan-2d-result' },
  'rop-cloud':         { kind: 'pair',   params: 'rop-cloud-params', result: 'rop-cloud-result', needsReactions: true },
  'rop-polyhedron':    { kind: 'pair',   params: 'rop-poly-params',  result: 'rop-poly-result',  modelOnly: true },
  'fret-heatmap':      { kind: 'pair',   params: 'fret-params',      result: 'fret-result' },
};

// Per-node transient state. Never serialised.
const nodeRuntime = new WeakMap();
function rt(nodeId) {
  const info = nodeRegistry[nodeId];
  if (!info) return null;
  let s = nodeRuntime.get(info);
  if (!s) { s = { abort: null, lastResult: null, files: [] }; nodeRuntime.set(info, s); }
  return s;
}

function $(id) { return document.getElementById(id); }
function $$(nodeId, suffix) { return document.getElementById(`${nodeId}${suffix}`); }

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

// ===== Node body markup =====

export function buildAgentNodeBody(nodeId) {
  const modelOpts = AGENT_MODEL_OPTIONS.map(o =>
    `<option value="${o.value}">${escapeHtml(o.label)}</option>`
  ).join('');
  return `
    <div class="agent-node">
      <div class="agent-node-row">
        <label class="agent-node-label" for="${nodeId}-agent-key">API key (session only, not saved)</label>
        <input id="${nodeId}-agent-key" class="agent-node-input" type="password" autocomplete="off" spellcheck="false" placeholder="sk-ant-… or sk-…">
      </div>
      <div class="agent-node-row agent-node-row-inline">
        <label><input id="${nodeId}-agent-remember" type="checkbox"> Remember for browser session</label>
      </div>
      <div class="agent-node-row">
        <label class="agent-node-label" for="${nodeId}-agent-model">Model</label>
        <select id="${nodeId}-agent-model" class="agent-node-input">${modelOpts}</select>
        <input id="${nodeId}-agent-model-custom" class="agent-node-input agent-node-input-custom" type="text" placeholder="model id" style="display:none;">
      </div>
      <div class="agent-node-row">
        <label class="agent-node-label">Files (multi-select — PDFs, notebooks, code, zips; secrets skipped)</label>
        <div class="agent-node-files">
          <input id="${nodeId}-agent-files" type="file" multiple>
          <ul id="${nodeId}-agent-files-list" class="agent-node-files-list"></ul>
        </div>
      </div>
      <div class="agent-node-row">
        <label class="agent-node-label" for="${nodeId}-agent-text">Additional text (optional)</label>
        <textarea id="${nodeId}-agent-text" class="agent-node-textarea" placeholder="Paste a paper section, supplementary methods, or any extra context — sent alongside the files."></textarea>
      </div>
      <div class="agent-node-actions">
        <button class="btn btn-run" id="${nodeId}-agent-run">Run agent</button>
        <button class="btn agent-node-cancel" id="${nodeId}-agent-cancel" disabled>Cancel</button>
      </div>
      <div id="${nodeId}-agent-status" class="agent-node-status"></div>
      <div id="${nodeId}-agent-result" class="agent-node-result hidden"></div>
    </div>
  `;
}

// ===== Wiring =====

function formatBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(2)} MB`;
}

function renderFileList(nodeId) {
  const list = $$(nodeId, '-agent-files-list');
  const state = rt(nodeId);
  if (!list || !state) return;
  if (state.files.length === 0) {
    list.innerHTML = '<li class="agent-node-files-empty">No files selected.</li>';
    return;
  }
  let total = 0;
  list.innerHTML = state.files.map((f, i) => {
    total += f.size;
    return `<li>
      <span class="agent-node-files-name" title="${escapeHtml(f.name)}">${escapeHtml(f.name)}</span>
      <span class="agent-node-files-size">${formatBytes(f.size)}</span>
      <button type="button" class="agent-node-files-remove" data-idx="${i}" title="Remove">&times;</button>
    </li>`;
  }).join('') + `<li class="agent-node-files-total">${state.files.length} file(s) — total ${formatBytes(total)}</li>`;
  list.querySelectorAll('.agent-node-files-remove').forEach(btn => {
    btn.addEventListener('click', () => {
      const idx = Number(btn.dataset.idx);
      state.files.splice(idx, 1);
      renderFileList(nodeId);
    });
  });
}

export function setupAgentNode(nodeId) {
  const root = document.getElementById(nodeId);
  if (!root) return;

  const sel = $$(nodeId, '-agent-model');
  const custom = $$(nodeId, '-agent-model-custom');
  const key = $$(nodeId, '-agent-key');
  const remember = $$(nodeId, '-agent-remember');
  const filesInput = $$(nodeId, '-agent-files');

  // Restore opt-in remembered key/model if present.
  try {
    const savedKey = sessionStorage.getItem(SESSION_KEY_STORAGE);
    if (savedKey) { key.value = savedKey; remember.checked = true; }
    const savedModel = sessionStorage.getItem(SESSION_MODEL_STORAGE);
    if (savedModel) {
      const known = AGENT_MODEL_OPTIONS.find(o => o.value === savedModel);
      if (known) {
        sel.value = savedModel;
      } else {
        sel.value = '__custom__';
        custom.value = savedModel;
        custom.style.display = '';
      }
    }
  } catch (_) { /* sessionStorage unavailable */ }

  sel.addEventListener('change', () => {
    custom.style.display = sel.value === '__custom__' ? '' : 'none';
    if (sel.value === '__custom__') custom.focus();
  });

  filesInput.addEventListener('change', () => {
    const state = rt(nodeId);
    if (!state) return;
    const incoming = Array.from(filesInput.files || []);
    // Append to existing selection rather than replace (so user can pick from
    // multiple folders across separate Open dialogs).
    incoming.forEach(f => state.files.push(f));
    filesInput.value = '';
    renderFileList(nodeId);
  });

  renderFileList(nodeId);

  $$(nodeId, '-agent-run').addEventListener('click', () => executeAgentNode(nodeId));
  $$(nodeId, '-agent-cancel').addEventListener('click', () => onCancel(nodeId));
}

// ===== Run / cancel =====

function setStatus(nodeId, text, kind = 'info') {
  const el = $$(nodeId, '-agent-status');
  if (!el) return;
  el.textContent = text || '';
  el.dataset.kind = kind;
}

function setThinking(nodeId, on) {
  const root = document.getElementById(nodeId);
  if (!root) return;
  root.classList.toggle('node-thinking', !!on);
  $$(nodeId, '-agent-run').disabled = !!on;
  $$(nodeId, '-agent-cancel').disabled = !on;
}

function readModelChoice(nodeId) {
  const sel = $$(nodeId, '-agent-model');
  const custom = $$(nodeId, '-agent-model-custom');
  if (!sel) return { model: '', provider: 'anthropic' };
  if (sel.value === '__custom__') {
    const m = (custom?.value || '').trim();
    return { model: m, provider: detectProvider(m) };
  }
  const opt = AGENT_MODEL_OPTIONS.find(o => o.value === sel.value);
  return { model: sel.value, provider: opt?.provider || detectProvider(sel.value) };
}

export async function executeAgentNode(nodeId) {
  const state = rt(nodeId);
  if (!state || state.abort) return false;
  const key = ($$(nodeId, '-agent-key')?.value || '').trim();
  const { model, provider } = readModelChoice(nodeId);
  const text = $$(nodeId, '-agent-text')?.value || '';
  const files = state.files.slice();
  const remember = !!$$(nodeId, '-agent-remember')?.checked;

  if (!key) { setStatus(nodeId, 'Enter an API key.', 'error'); return false; }
  if (!model) { setStatus(nodeId, 'Pick or type a model id.', 'error'); return false; }
  if (files.length === 0 && !text.trim()) { setStatus(nodeId, 'Select at least one file or paste some text.', 'error'); return false; }

  try {
    if (remember) {
      sessionStorage.setItem(SESSION_KEY_STORAGE, key);
      sessionStorage.setItem(SESSION_PROVIDER_STORAGE, provider);
      sessionStorage.setItem(SESSION_MODEL_STORAGE, model);
    } else {
      sessionStorage.removeItem(SESSION_KEY_STORAGE);
      sessionStorage.removeItem(SESSION_PROVIDER_STORAGE);
      sessionStorage.removeItem(SESSION_MODEL_STORAGE);
    }
  } catch (_) { /* sessionStorage unavailable */ }

  setThinking(nodeId, true);
  const sourceSummary = files.length > 0
    ? `${files.length} file(s)${text.trim() ? ' + pasted text' : ''}`
    : 'pasted text';
  setStatus(nodeId, `Calling ${provider} with ${sourceSummary}…`, 'busy');
  state.abort = new AbortController();
  try {
    const result = await runAgent({ provider, apiKey: key, model, text, files, signal: state.abort.signal });
    if (!nodeRegistry[nodeId] || !document.getElementById(nodeId)) return false;
    state.lastResult = result;
    renderResult(nodeId, result);
    if (result.networks.length > 0) {
      setStatus(nodeId, `Done. ${result.networks.length} network(s), ${result.warnings.length} warning(s). Auto-spawning…`, 'ok');
      autoSpawn(nodeId, result);
    } else {
      setStatus(nodeId, `Done, but no extractable equilibrium binding networks were found. See summary.`, 'warn');
    }
    return true;
  } catch (e) {
    if (e?.name === 'AbortError') {
      setStatus(nodeId, 'Cancelled.', 'info');
    } else {
      console.error('[agent-node] error:', e);
      setStatus(nodeId, e.message || String(e), 'error');
      if (e?.rawText) renderRawError(nodeId, e);
    }
    return false;
  } finally {
    setThinking(nodeId, false);
    state.abort = null;
  }
}

function onCancel(nodeId) {
  const state = rt(nodeId);
  if (state?.abort) state.abort.abort();
}

// ===== Result rendering =====

function renderRawError(nodeId, err) {
  const block = $$(nodeId, '-agent-result');
  if (!block) return;
  block.classList.remove('hidden');
  const text = err.rawText || '';
  block.innerHTML = `
    <details class="agent-node-warnings" open>
      <summary>Raw model output (parse failed${err.stopReason ? `, stop_reason=${escapeHtml(err.stopReason)}` : ''})</summary>
      <p class="agent-node-meta">Parse error: ${escapeHtml(err.parseError || err.message || 'unknown')}</p>
      <button type="button" class="btn agent-node-copy-raw" data-agent-node="${nodeId}">Copy raw output</button>
      <pre class="agent-node-raw">${escapeHtml(text)}</pre>
    </details>
  `;
  block.querySelector('.agent-node-copy-raw')?.addEventListener('click', () => {
    writeClipboardText(text).then(
      () => showToast('Raw model output copied'),
      () => showToast('Copy failed — select the text manually'),
    );
  });
}

function renderResult(nodeId, result) {
  const block = $$(nodeId, '-agent-result');
  if (!block) return;
  block.classList.remove('hidden');
  const parts = [];
  if (result.summary) {
    parts.push(`<p class="agent-node-summary">${escapeHtml(result.summary)}</p>`);
  }
  result.networks.forEach((n, i) => {
    const analysisLabel = (a) => {
      const cfg = [];
      if (a.output_expression) cfg.push(`out=${a.output_expression}`);
      if (a.scan_param) cfg.push(`p=${a.scan_param}`);
      if (a.scan_param_2) cfg.push(`p2=${a.scan_param_2}`);
      return `<code title="${escapeHtml(cfg.join('  '))}">${escapeHtml(a.name)}${cfg.length ? ` <span class="agent-node-cfg-hint">·</span>` : ''}</code>`;
    };
    parts.push(`<details class="agent-node-network" ${i === 0 ? 'open' : ''}>
      <summary><strong>${escapeHtml(n.name)}</strong> — ${n.reactions.length} reactions, ${n.recommended_analyses.length} analyses</summary>
      <ul class="agent-node-rxn-list">${n.reactions.map(r => `<li><code>${escapeHtml(r.rule)}</code>  Kd=${r.kd}  <span class="agent-conf agent-conf-${escapeHtml(r.confidence)}">${escapeHtml(r.confidence)}</span></li>`).join('')}</ul>
      <p class="agent-node-meta">Will spawn: ${n.recommended_analyses.map(analysisLabel).join(', ')}</p>
      ${n.notes ? `<p class="agent-node-notes">${escapeHtml(n.notes)}</p>` : ''}
    </details>`);
  });
  if (result.warnings.length > 0) {
    parts.push(`<details class="agent-node-warnings">
      <summary>${result.warnings.length} warning(s)</summary>
      <ul>${result.warnings.map(w => `<li>${escapeHtml(w)}</li>`).join('')}</ul>
    </details>`);
  }
  block.innerHTML = parts.join('');
}

// ===== Auto-spawn =====

const COL_GAP = 60;
const NODE_W = { rxn: 280, mb: 260, simple: 320, params: 320, result: 420 };
const DEFAULT_NODE_H = { rxn: 240, mb: 180, simple: 260, params: 280, result: 360 };

function getNodeXY(nodeId) {
  const el = document.getElementById(nodeId);
  if (!el) return { x: 0, y: 0 };
  return { x: parseFloat(el.style.left) || 0, y: parseFloat(el.style.top) || 0 };
}

// Place a node at the given x with overlap avoidance applied to y.
function safeCreate(type, x, desiredY, w, h) {
  const safeY = resolveOverlap(x, desiredY, w, h, null);
  const id = createNode(type, x, safeY);
  return id ? { id, x, y: safeY } : null;
}

function setNodeField(nodeId, suffix, value) {
  if (!value) return;
  const el = document.getElementById(`${nodeId}${suffix}`);
  if (!el) return;
  // Stash for later sync once the select options are populated by execute().
  el.dataset.pendingValue = String(value);
  if (el instanceof HTMLSelectElement) {
    if (Array.from(el.options).some(o => o.value === String(value))) {
      el.value = String(value);
      delete el.dataset.pendingValue;
    }
  } else {
    el.value = String(value);
  }
  el.dispatchEvent(new Event('input', { bubbles: true }));
  el.dispatchEvent(new Event('change', { bubbles: true }));
}

// Write the agent's analysis-specific config into the params node DOM.
function applyAnalysisConfig(paramsId, analysisName, analysis) {
  switch (analysisName) {
    case 'parameter-scan-1d':
      setNodeField(paramsId, '-expr', analysis.output_expression);
      setNodeField(paramsId, '-param', analysis.scan_param);
      break;
    case 'parameter-scan-2d':
      setNodeField(paramsId, '-expr', analysis.output_expression);
      setNodeField(paramsId, '-param1', analysis.scan_param);
      setNodeField(paramsId, '-param2', analysis.scan_param_2);
      break;
    case 'rop-cloud':
      setNodeField(paramsId, '-target-species', analysis.output_expression);
      break;
    case 'fret-heatmap':
      break;
    default:
      break;
  }
}

function placeReactionNetwork(x, y, network) {
  const placed = safeCreate('reaction-network', x, y, NODE_W.rxn, DEFAULT_NODE_H.rxn);
  if (!placed) return null;
  const list = document.getElementById(`${placed.id}-reactions-list`);
  if (list) list.innerHTML = '';
  network.reactions.forEach(r => addReactionRow(placed.id, r.rule, r.kd));
  return placed;
}

function placeModelBuilder(x, y, rnId) {
  const placed = safeCreate('model-builder', x, y, NODE_W.mb, DEFAULT_NODE_H.mb);
  if (!placed) return null;
  connections.push({ fromNode: rnId, fromPort: 'reactions', toNode: placed.id, toPort: 'reactions' });
  return placed;
}

function placeAnalysis(x, y, rnId, mbId, analysis) {
  const def = CHAIN_MAP[analysis.name];
  if (!def) return null;
  if (def.kind === 'simple') {
    const placed = safeCreate(def.result, x, y, NODE_W.simple, DEFAULT_NODE_H.simple);
    if (!placed) return null;
    connections.push({ fromNode: mbId, fromPort: 'model', toNode: placed.id, toPort: 'model' });
    const size = getNodeSize(placed.id);
    return { bottom: placed.y + (size?.h || DEFAULT_NODE_H.simple), ids: [placed.id] };
  }
  // pair: params + result
  const params = safeCreate(def.params, x, y, NODE_W.params, DEFAULT_NODE_H.params);
  if (!params) return null;
  applyAnalysisConfig(params.id, analysis.name, analysis);
  const result = safeCreate(def.result, x + NODE_W.params + COL_GAP, params.y, NODE_W.result, DEFAULT_NODE_H.result);
  if (!result) return null;
  connections.push({ fromNode: mbId, fromPort: 'model', toNode: params.id, toPort: 'model' });
  connections.push({ fromNode: params.id, fromPort: 'params', toNode: result.id, toPort: 'params' });
  if (def.needsReactions) {
    connections.push({ fromNode: rnId, fromPort: 'reactions', toNode: params.id, toPort: 'reactions' });
  }
  const psize = getNodeSize(params.id);
  const rsize = getNodeSize(result.id);
  const bottom = Math.max(
    params.y + (psize?.h || DEFAULT_NODE_H.params),
    result.y + (rsize?.h || DEFAULT_NODE_H.result),
  );
  return { bottom, ids: [params.id, result.id] };
}

function autoSpawn(nodeId, result) {
  if (!nodeRegistry[nodeId] || !document.getElementById(nodeId)) return;
  const aiOrigin = getNodeXY(nodeId);
  const aiSize = getNodeSize(nodeId) || { w: 340, h: 600 };

  // First band starts level with the AI node, columns step rightward from it.
  const rxnX = aiOrigin.x + aiSize.w + COL_GAP;
  const mbX = rxnX + NODE_W.rxn + COL_GAP;
  const analysisX = mbX + NODE_W.mb + COL_GAP;

  let bandTopY = aiOrigin.y;
  let spawnedTotal = 0;
  const modelBuilderIds = [];

  result.networks.forEach((network) => {
    const rn = placeReactionNetwork(rxnX, bandTopY, network);
    if (!rn) return;
    const mb = placeModelBuilder(mbX, rn.y, rn.id);
    if (!mb) return;
    modelBuilderIds.push(mb.id);
    spawnedTotal += 2;

    let analysisY = mb.y;
    let bandBottomY = Math.max(rn.y + DEFAULT_NODE_H.rxn, mb.y + DEFAULT_NODE_H.mb);
    network.recommended_analyses.forEach((analysis) => {
      const placed = placeAnalysis(analysisX, analysisY, rn.id, mb.id, analysis);
      if (!placed) return;
      spawnedTotal += placed.ids.length;
      analysisY = placed.bottom + 30;
      bandBottomY = Math.max(bandBottomY, placed.bottom);
    });

    bandTopY = bandBottomY + 60;
  });

  updateConnections();
  modelBuilderIds.forEach(mbId => {
    setTimeout(() => nodeRegistry[mbId]?._autoBuildCheck?.(), 100);
  });
  commitWorkspaceSnapshot('agent-spawn');
  showToast(`Spawned ${spawnedTotal} nodes from agent output`);
}

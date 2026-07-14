// Biocircuits Explorer — AI Import node runtime.
//
// This module owns the event wiring, run state, and downstream-spawning
// logic for the `ai-import` red input node defined in node-types/input.js.
// The node itself never serialises an API key or the source content — its
// purpose is to translate a paper/notebook into populated reaction-network
// + model-builder + analysis chains, then step out of the way.

import { runAgent, detectProvider } from './agent.js';
import {
  captureEditorGraphPlanningGraph,
  createEditorGraphPatchCommand,
} from './nodes.js';
import { addReactionRow } from './model.js';
import { nodeIdCounter, nodeRegistry } from './state.js';
import { showToast } from './api.js';
import { writeClipboardText } from './native-clipboard.js';
import { dispatch } from './commands.js';
import { planAgentAutoSpawnWorkflow } from './graph-patch.js';

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

// Per-node transient state. Never serialised.
const nodeRuntime = new WeakMap();
function rt(nodeId) {
  const info = nodeRegistry[nodeId];
  if (!info) return null;
  let s = nodeRuntime.get(info);
  if (!s) { s = { abort: null, lastResult: null, files: [] }; nodeRuntime.set(info, s); }
  return s;
}

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
  } catch { /* sessionStorage unavailable */ }

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
  } catch { /* sessionStorage unavailable */ }

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
      const spawn = autoSpawn(nodeId, result);
      if (!spawn.ok) {
        setStatus(nodeId, `Agent output was not added: ${spawn.diagnostic.message}`, 'error');
        return false;
      }
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

function initializeAgentSpawnNode(spec) {
  const initialization = spec.initialization;
  if (!initialization) return;

  if (initialization.kind === 'reaction-network-rules') {
    if (spec.type !== 'reaction-network' || !Array.isArray(initialization.reactions)) {
      throw new Error(`Invalid reaction initialization for ${spec.id}`);
    }
    const list = document.getElementById(`${spec.id}-reactions-list`);
    if (list) list.innerHTML = '';
    initialization.reactions.forEach(reaction => {
      addReactionRow(spec.id, reaction.rule, reaction.kd);
    });
    return;
  }

  if (initialization.kind === 'analysis-config') {
    applyAnalysisConfig(
      spec.id,
      initialization.analysisName,
      initialization.analysis,
    );
    return;
  }

  throw new Error(`Unknown Agent node initialization: ${String(initialization.kind)}`);
}

export function autoSpawn(nodeId, result) {
  const plan = planAgentAutoSpawnWorkflow({
    graph: captureEditorGraphPlanningGraph(),
    nextNodeOrdinal: nodeIdCounter + 1,
    agentNodeId: nodeId,
    result,
  });
  if (!plan.ok) {
    showToast(plan.diagnostic.message);
    return plan;
  }

  try {
    const command = createEditorGraphPatchCommand(plan, {
      initializeNode: initializeAgentSpawnNode,
    });
    dispatch(command);
    showToast(`Spawned ${plan.patch.metadata.spawnedNodeCount} nodes from agent output`);
    return { ...plan, command };
  } catch (error) {
    const diagnostic = {
      kind: 'error',
      code: error?.code || 'agent-auto-spawn-failed',
      message: error?.message || String(error),
    };
    console.error('[agent-auto-spawn] Graph patch failed', error);
    showToast(`Agent auto-spawn failed: ${diagnostic.message}`);
    return { ok: false, patch: null, diagnostic };
  }
}

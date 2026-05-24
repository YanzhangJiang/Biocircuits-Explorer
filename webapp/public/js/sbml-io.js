// SBML import/export node logic. Wired into main.js ACTION_HANDLERS as
// importSbml / exportSbml / loadSbmlFile.

import { api, showToast, handleNodeError, escapeHtml } from './api.js';
import { addReactionRow, getReactionsFromNode, triggerDownstreamNodes } from './model.js';
import { connections } from './state.js';
import { commitWorkspaceSnapshot } from './workspace.js';

// File chooser → fill the node's SBML textarea with the file's text.
export function loadSbmlFile(inputEl) {
  const nodeId = inputEl.dataset.node;
  const file = inputEl.files && inputEl.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = (e) => {
    const ta = document.getElementById(`${nodeId}-sbml-input`);
    if (ta) ta.value = String(e.target.result || '');
    showToast(`Loaded ${file.name}`);
  };
  reader.onerror = () => showToast(`Could not read ${file.name}`);
  reader.readAsText(file);
}

function renderWarnings(nodeId, warnings) {
  const el = document.getElementById(`${nodeId}-sbml-warnings`);
  if (!el) return;
  if (!warnings || warnings.length === 0) {
    el.style.display = 'none';
    el.innerHTML = '';
    return;
  }
  el.style.display = 'block';
  el.innerHTML = `<strong>${warnings.length} warning(s):</strong><ul>` +
    warnings.map((w) => `<li>${escapeHtml(w)}</li>`).join('') +
    '</ul>';
}

// Import: POST the pasted/loaded SBML to the backend, then repopulate the
// node's reaction list from the returned NetworkIR and surface any warnings.
export async function importSbml(nodeId) {
  const ta = document.getElementById(`${nodeId}-sbml-input`);
  const xml = (ta && ta.value ? ta.value : '').trim();
  if (!xml) { showToast('Paste or upload SBML first'); return; }
  try {
    const res = await api('v1/import/sbml', { sbml: xml });
    const reactions = (res.network_ir && res.network_ir.reactions) || [];
    const list = document.getElementById(`${nodeId}-reactions-list`);
    if (list) list.innerHTML = '';
    reactions.forEach((rx) => addReactionRow(nodeId, rx.formula, rx.kd));
    renderWarnings(nodeId, res.warnings || []);
    triggerDownstreamNodes(nodeId);
    commitWorkspaceSnapshot('sbml-import');
    showToast(`Imported ${reactions.length} reaction(s)`);
  } catch (e) {
    handleNodeError(e, nodeId, 'SBML import');
  }
}

function downloadText(text, filename, mime = 'application/xml') {
  const blob = new Blob([text], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

// Export: read the upstream reaction source feeding our `reactions` input,
// POST it to the backend, and download the returned SBML document.
export async function exportSbml(nodeId) {
  const conn = connections.find((c) => c.toNode === nodeId && c.toPort === 'reactions');
  if (!conn) { showToast('Connect a reaction source to the Reactions input first'); return; }
  const { reactions, kds } = getReactionsFromNode(conn.fromNode);
  if (!reactions.length) { showToast('Upstream node has no reactions'); return; }
  try {
    const res = await api('v1/export/sbml', {
      reactions,
      // The backend requires positive Kd; default blanks to the usual 1e-3.
      kd: kds.map((k) => (k == null ? 1e-3 : k)),
    });
    const safeName = (res.label || 'model').replace(/[^A-Za-z0-9_-]/g, '_');
    downloadText(res.sbml, `${safeName}.xml`);
    const status = document.getElementById(`${nodeId}-sbml-export-status`);
    if (status) status.textContent = `Exported ${reactions.length} reaction(s).`;
  } catch (e) {
    handleNodeError(e, nodeId, 'SBML export');
  }
}

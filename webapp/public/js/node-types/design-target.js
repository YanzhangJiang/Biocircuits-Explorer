// Biocircuits Explorer — Design Target node: the target-first design pipeline entry.
// Enter a target reaction-order behavior (qualitative signs `+-+`, or a precise RO
// sequence `1.5, 0, -1`); it queries the enumerated atlas (bundled slices, d≤4/μ≤5)
// for DESIGNABILITY and returns the Pareto-MINIMAL architectures.
//
// The node IS a reaction source: it has a Reactions output port, and SELECTING a
// minimal network (click its row) emits that network downstream like a
// reaction-network / network-id-definition node. "Build & tune →" is a convenience
// that selects a network AND auto-spawns the wired tunable Placer station from this
// node's own output port — so the pipeline is one graph:
//   target → designable? → minimal → (emit) → model-builder → tunable.
//   [/api/design_search]
import { api, escapeHtml, syncSelectOptions } from '../api.js';
import {
  setNodeLoading, createNode, getNodePosition, getNodeSize, resolveOverlap,
  getModelForNode, triggerConfigUpdate, triggerAutoModelBuild,
} from '../nodes.js';
import { buildModel } from '../model.js';
import { connections, nodeRegistry } from '../state.js';
import { updateConnections } from '../connections.js';
import { commitWorkspaceSnapshot } from '../workspace.js';
import { loadPlacerMenu, realizePlacerProgram } from './placer.js';

export const DESIGN_TARGET_TYPES = {
  'design-target': {
    category: 'viewer',
    headerClass: 'header-viewer',
    title: 'Design Target',
    inputs: [],
    outputs: [{ port: 'reactions', label: 'Reactions' }],
    defaultWidth: 460,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>Target kind:</label>
          <select id="${nodeId}-kind">
            <option value="sign">qualitative (signs, e.g. + - +)</option>
            <option value="exact">precise RO (e.g. 1.5, 0, -1)</option>
          </select>
        </div>
        <div class="param-row">
          <label>Target:</label>
          <input type="text" id="${nodeId}-target" placeholder="+-+   or   1, 0, -1" style="flex:1;">
        </div>
        <button class="btn btn-run" data-action="runDesignSearch" data-node="${nodeId}">Search designable</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Enter a target reaction-order behavior. <b>Search designable</b> queries the enumerated atlas (d≤4, r≤5, μ≤5) and returns the minimal architectures that realize it. Click a network to emit it on the Reactions port, or <b>Build &amp; tune →</b> to drop a wired, live tunable station for it.</span>
        </div>
      `;
    },
  },
};

export async function runDesignSearch(nodeId) {
  const kind = document.getElementById(`${nodeId}-kind`).value;
  const raw = (document.getElementById(`${nodeId}-target`).value || '').trim();
  if (!raw) { alert('Enter a target behavior'); return; }
  let target;
  if (kind === 'sign') {
    target = raw.replace(/[^+\-]/g, '');
    if (!target) { alert('A qualitative target looks like  + - +  (use + and -)'); return; }
  } else {
    target = raw.split(/[,\s]+/).map(Number).filter(v => Number.isFinite(v));
    if (!target.length) { alert('A precise target looks like  1, 0, -1'); return; }
  }
  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);
  try {
    const data = await api('design_search', { target_kind: kind, target });
    if (!data.designable) {
      contentEl.innerHTML =
        `<div class="siso-summary-line"><span class="summary-chip"><span class="tag tag-atlas-failed">not designable</span></span></div>` +
        `<div class="text-dim">Within the grammar (d≤4, r≤5, μ≤5). Under dominance-closure this is a parameter-independent impossibility, not merely "unobserved".</div>`;
      return;
    }
    const selectedNid = nodeRegistry[nodeId]?.data?.config?.selectedNid || null;
    const sections = (data.minimal || []).map(m => {
      const n = (m.networks || []).length;
      const head =
        `<div class="siso-section-head">` +
        `<div class="siso-section-title">minimal (d, r, μ) = (${m.d}, ${m.r}, ${m.mu})</div>` +
        `<div class="text-dim">${n} network${n === 1 ? '' : 's'}</div></div>`;
      const rows = (m.networks || []).map(nw => {
        const isSel = selectedNid && nw.nid === selectedNid;
        return `<div class="path-item design-net-row${isSel ? ' selected' : ''}" ` +
          `data-nid="${escapeHtml(nw.nid)}" data-inp="${escapeHtml(nw.inp)}" data-out="${escapeHtml(nw.out)}" ` +
          `role="button" tabindex="0" title="Click to emit this network on the Reactions port">` +
          `<div class="design-net-id">${escapeHtml(nw.nid)}` +
          `<div class="design-net-io">[${escapeHtml(nw.inp)} → ${escapeHtml(nw.out)}]</div></div>` +
          `<div class="design-net-actions">` +
          `<span class="tag tag-atlas-ok design-emit-badge" style="${isSel ? '' : 'display:none;'}">emitting</span>` +
          `<button class="btn btn-small design-build-btn">Build &amp; tune →</button>` +
          `</div></div>`;
      }).join('');
      return `<section class="siso-section">${head}${rows}</section>`;
    }).join('');
    const emitChip = selectedNid
      ? `emitting <strong>${escapeHtml(shortNid(selectedNid))}</strong>`
      : `<span class="text-dim">no network selected</span>`;
    contentEl.innerHTML =
      `<div class="siso-summary-line">` +
      `<span class="summary-chip"><span class="tag tag-atlas-ok">designable</span></span>` +
      `<span class="summary-chip"><strong>${Math.round(data.n_matches)}</strong> realizing slices</span>` +
      `<span class="summary-chip" id="${nodeId}-emit">${emitChip}</span></div>` +
      `<div class="text-dim">Minimal architectures (Pareto frontier over d, r, μ). Click a network to emit it on the Reactions port, or <b>Build &amp; tune →</b> for a wired tunable station.</div>` +
      sections;
    wireNetworkRows(nodeId, contentEl);
  } catch (e) {
    contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

function wireNetworkRows(nodeId, contentEl) {
  contentEl.querySelectorAll('.design-net-row').forEach(row => {
    const pick = () => selectNetwork(nodeId, row.dataset.nid, row.dataset.inp, row.dataset.out);
    row.addEventListener('click', pick);
    row.addEventListener('keydown', e => {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); pick(); }
    });
    const btn = row.querySelector('.design-build-btn');
    if (btn) btn.addEventListener('click', e => {
      e.stopPropagation();   // don't double-fire the row's select handler
      buildAndTune(nodeId, row.dataset.nid, row.dataset.inp, row.dataset.out, btn);
    });
  });
}

// ── nid → reaction rules ───────────────────────────────────────────────────
// nid is the network in monomer-index code, reactions joined by '|', each side a
// '+'-sum of complex tokens `[m1,m2,...]`. The atlas naming (verified against the
// bundled slices) is: monomer [i] → letter (1→A, 2→B, …); complex → 'C_'+letters
// joined by '_'. e.g. [1]+[1]<->[1,1] → "A + A <-> C_A_A"; [1]+[2]<->[1,2] →
// "A + B <-> C_A_B". ReactionParser keeps these names verbatim, so the built
// model's species/totals match the slice's out / inp ("tA") tokens exactly.
function complexToken(monomers) {
  const letters = monomers.map(i => String.fromCharCode(64 + i)); // 1→A, 2→B, …
  return letters.length === 1 ? letters[0] : 'C_' + letters.join('_');
}
function sideToSpecies(side) {
  const toks = side.trim().match(/\[[0-9,]+\]/g) || [];
  return toks.map(t => complexToken(t.replace(/[[\]]/g, '').split(',').map(Number))).join(' + ');
}
export function nidToRules(nid) {
  return String(nid).split('|').map(rxn => {
    const parts = rxn.split(/<->|<=>|↔/);
    if (parts.length !== 2) throw new Error(`Cannot parse reaction in nid: ${rxn}`);
    return `${sideToSpecies(parts[0])} <-> ${sideToSpecies(parts[1])}`;
  });
}
function shortNid(nid) {
  const s = String(nid);
  return s.length > 28 ? s.slice(0, 26) + '…' : s;
}

// ── SELECT → emit on the Reactions output port ─────────────────────────────
// Publishes the chosen network's rules as config.resolvedDefinition.raw_rules —
// the same shape getReactionsFromNode reads for network-id-definition — so any
// model-builder wired to this node's Reactions port rebuilds from it.
function selectNetwork(designNodeId, nid, inp, out) {
  let rules;
  try { rules = nidToRules(nid); }
  catch (e) { alert(`Could not convert network: ${e.message}`); return null; }
  const info = nodeRegistry[designNodeId];
  if (!info) return null;
  info.data = info.data || {};
  const cfg = info.data.config = info.data.config || {};
  cfg.resolvedDefinition = { raw_rules: rules };
  cfg.selectedNid = nid;
  cfg.suggestedInput = inp;
  cfg.suggestedOutput = out;

  const contentEl = document.getElementById(`${designNodeId}-content`);
  if (contentEl) {
    contentEl.querySelectorAll('.design-net-row').forEach(r => {
      const on = r.dataset.nid === nid;
      r.classList.toggle('selected', on);
      const badge = r.querySelector('.design-emit-badge');
      if (badge) badge.style.display = on ? '' : 'none';
    });
    const emit = document.getElementById(`${designNodeId}-emit`);
    if (emit) emit.innerHTML = `emitting <strong>${escapeHtml(shortNid(nid))}</strong>`;
  }
  triggerAutoModelBuild(designNodeId);   // rebuild any model-builder already wired to us
  commitWorkspaceSnapshot('design-target-select');
  return rules;
}

// ── The GLUE: emit the network AND spawn a wired tunable station from our port ──
async function buildAndTune(designNodeId, nid, inp, out, btn) {
  const rules = selectNetwork(designNodeId, nid, inp, out);
  if (!rules) return;
  if (btn) { btn.disabled = true; btn.textContent = 'Building…'; }

  // Lay the chain out to the right of the Design Target node.
  const anchor = getNodePosition(designNodeId);
  const aSize = getNodeSize(designNodeId);
  const x0 = (anchor?.x || 80) + (aSize?.w || 460) + 60;
  const mbY = resolveOverlap(x0, (anchor?.y || 120), 260, 200, null);

  // model-builder pulls reactions through THIS node's own Reactions output port.
  const mbId = createNode('model-builder', x0, mbY);
  connections.push({ fromNode: designNodeId, fromPort: 'reactions', toNode: mbId, toPort: 'reactions' });

  // placer-params + placer-result, connected.
  const mbSize = getNodeSize(mbId);
  const pX = x0 + (mbSize?.w || 260) + 60;
  const pY = resolveOverlap(pX, mbY, 320, 320, null);
  const paramsId = createNode('placer-params', pX, pY);
  const pSize = getNodeSize(paramsId);
  const rX = pX + (pSize?.w || 320) + 60;
  const rY = resolveOverlap(rX, pY, 480, 360, null);
  const resultId = createNode('placer-result', rX, rY);
  connections.push({ fromNode: mbId, fromPort: 'model', toNode: paramsId, toPort: 'model' });
  connections.push({ fromNode: paramsId, fromPort: 'params', toNode: resultId, toPort: 'params' });
  updateConnections();

  const contentEl = document.getElementById(`${designNodeId}-content`);
  try {
    // Build the model from this node's emitted network.
    await buildModel(mbId, { triggerDownstream: false, throwOnFailure: true });
    const model = getModelForNode(paramsId);
    if (!model) throw new Error('Model build did not produce a model');

    // Configure the Placer with this slice's input total + output species.
    // The atlas tokens must exist in the built model — surface it loudly if not
    // (that would be the naming gotcha), instead of silently mis-tuning.
    const xSyms = (model.x_sym || []).map(String);
    const qSyms = (model.q_sym || []).map(String);
    if (!qSyms.includes(inp)) throw new Error(`input total "${inp}" not in built model (${qSyms.join(', ')})`);
    if (!xSyms.includes(out)) throw new Error(`output species "${out}" not in built model (${xSyms.join(', ')})`);
    syncSelectOptions(document.getElementById(`${paramsId}-input`), qSyms, inp);
    syncSelectOptions(document.getElementById(`${paramsId}-output`), xSyms, out);
    triggerConfigUpdate(paramsId, 'placer-params');

    // Populate the fine-tune menu, then REALIZE THE WHOLE PROGRAM (the design
    // target is a program, not a single slope) — the primary, no-re-ask action.
    await loadPlacerMenu(resultId);
    await realizePlacerProgram(resultId);
    if (btn) { btn.textContent = 'Built ✓'; }
  } catch (e) {
    if (btn) { btn.disabled = false; btn.textContent = 'Build & tune →'; }
    if (contentEl) {
      const err = document.createElement('div');
      err.className = 'node-error';
      err.textContent = `Build & tune failed: ${e.message}`;
      contentEl.appendChild(err);
    }
  }
}

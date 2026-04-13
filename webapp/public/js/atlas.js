import { nodeRegistry, connections, ATLAS_ROLE_OPTIONS, ATLAS_ORDER_OPTIONS, ATLAS_SINGULAR_OPTIONS } from './state.js';
import { api, showToast, handleNodeError, escapeHtml, splitCommaList, parseOptionalInteger, parseOptionalFloat, parseOptionalJson, normalizePredicateArray, cloneSerializable } from './api.js';
import { applyPlotLayoutTheme, getPlotTheme } from './theme.js';
import { setNodeLoading, getModelContextForNode, findUpstreamNodeByType, getSessionIdForNode } from './nodes.js';
import { commitWorkspaceSnapshot } from './workspace.js';
import { triggerConfigUpdate } from './nodes.js';
import { triggerDownstreamNodes } from './model.js';
import { getNodeSerialData } from './workspace.js';
import { getFamilyColor, hexToRgba } from './theme.js';

/* ------------------------------------------------------------------ */
/*  Atlas-specific predicate helpers                                   */
/* ------------------------------------------------------------------ */

export function normalizePredicateSequenceArray(value, label) {
  if (!Array.isArray(value)) {
    throw new Error(`${label} must be a JSON array.`);
  }
  if (!value.length) return [];
  if (Array.isArray(value[0])) return value;
  if (value[0] && typeof value[0] === 'object') return [value];
  throw new Error(`${label} must be an array of predicate arrays.`);
}

export function parseAtlasExplicitNetworks(text) {
  const trimmed = String(text || '').trim();
  if (!trimmed) return [];

  const parsed = JSON.parse(trimmed);
  if (Array.isArray(parsed)) return parsed;
  if (parsed && Array.isArray(parsed.networks)) return parsed.networks;
  throw new Error('Explicit networks must be a JSON array or an object with a `networks` array.');
}

/* ------------------------------------------------------------------ */
/*  HTML helpers                                                       */
/* ------------------------------------------------------------------ */

export function atlasOptionHtml(value, selectedValue, label = value || 'any') {
  const selected = String(value) === String(selectedValue ?? '') ? ' selected' : '';
  return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(label || 'any')}</option>`;
}

export function atlasRoleSelectHtml(selectedValue = '') {
  return `
    <select class="atlas-builder-input atlas-role">
      ${ATLAS_ROLE_OPTIONS.map(value => atlasOptionHtml(value, selectedValue, value || 'any role')).join('')}
    </select>
  `;
}

export function atlasOrderSelectHtml(selectedValue = '') {
  return `
    <select class="atlas-builder-input atlas-order">
      ${ATLAS_ORDER_OPTIONS.map(value => atlasOptionHtml(value, selectedValue, value || 'any order')).join('')}
    </select>
  `;
}

export function atlasSingularSelectHtml(selectedValue = '') {
  return `
    <select class="atlas-builder-input atlas-singular">
      ${ATLAS_SINGULAR_OPTIONS.map(value => atlasOptionHtml(value, selectedValue, value || 'any singularity')).join('')}
    </select>
  `;
}

/* ------------------------------------------------------------------ */
/*  Builder rows                                                       */
/* ------------------------------------------------------------------ */

export function atlasBuilderRowHtml(kind, value = {}) {
  if (kind === 'transition') {
    return `
      <div class="atlas-builder-row" data-builder-kind="transition">
        <div class="atlas-builder-pair">
          ${atlasRoleSelectHtml(value.from?.role || '')}
          ${atlasOrderSelectHtml(value.from?.output_order_token || '')}
        </div>
        <span class="atlas-builder-arrow">\u2192</span>
        <div class="atlas-builder-pair">
          ${atlasRoleSelectHtml(value.to?.role || '')}
          ${atlasOrderSelectHtml(value.to?.output_order_token || '')}
        </div>
        <button type="button" class="btn btn-small atlas-builder-remove" title="Remove condition">\u00d7</button>
      </div>
    `;
  }

  return `
    <div class="atlas-builder-row" data-builder-kind="${escapeHtml(kind)}">
      ${atlasRoleSelectHtml(value.role || '')}
      ${atlasOrderSelectHtml(value.output_order_token || '')}
      ${atlasSingularSelectHtml(
        value.singular === true ? 'singular' :
        value.singular === false ? 'regular' : ''
      )}
      <button type="button" class="btn btn-small atlas-builder-remove" title="Remove condition">\u00d7</button>
    </div>
  `;
}

export function bindAtlasBuilderRowEvents(nodeId, row) {
  row.querySelectorAll('input, select').forEach(input => {
    const eventType = input.tagName === 'SELECT' ? 'change' : 'input';
    input.addEventListener(eventType, () => triggerConfigUpdate(nodeId, 'atlas-query-config'));
  });
  row.querySelector('.atlas-builder-remove')?.addEventListener('click', () => {
    row.remove();
    triggerConfigUpdate(nodeId, 'atlas-query-config');
  });
}

export function addAtlasBuilderRow(nodeId, containerKey, kind, value = {}) {
  const container = document.getElementById(`${nodeId}-${containerKey}`);
  if (!container) return;
  const wrapper = document.createElement('div');
  wrapper.innerHTML = atlasBuilderRowHtml(kind, value);
  const row = wrapper.firstElementChild;
  if (!row) return;
  bindAtlasBuilderRowEvents(nodeId, row);
  container.appendChild(row);
  triggerConfigUpdate(nodeId, 'atlas-query-config');
}

export function clearAtlasBuilderRows(nodeId, containerKey) {
  const container = document.getElementById(`${nodeId}-${containerKey}`);
  if (container) container.innerHTML = '';
}

/* ------------------------------------------------------------------ */
/*  Collecting builder state                                           */
/* ------------------------------------------------------------------ */

export function collectAtlasRegimeRows(nodeId, containerKey) {
  const container = document.getElementById(`${nodeId}-${containerKey}`);
  if (!container) return [];
  const rows = [];
  container.querySelectorAll('.atlas-builder-row').forEach(row => {
    const role = row.querySelector('.atlas-role')?.value || '';
    const outputOrder = row.querySelector('.atlas-order')?.value || '';
    const singular = row.querySelector('.atlas-singular')?.value || '';
    const predicate = {};
    if (role) predicate.role = role;
    if (outputOrder) predicate.output_order_token = outputOrder;
    if (singular === 'singular') predicate.singular = true;
    if (singular === 'regular') predicate.singular = false;
    if (Object.keys(predicate).length) rows.push(predicate);
  });
  return rows;
}

export function collectAtlasTransitionRows(nodeId, containerKey) {
  const container = document.getElementById(`${nodeId}-${containerKey}`);
  if (!container) return [];
  const rows = [];
  container.querySelectorAll('.atlas-builder-row').forEach(row => {
    const fromRole = row.querySelectorAll('.atlas-role')[0]?.value || '';
    const fromOrder = row.querySelectorAll('.atlas-order')[0]?.value || '';
    const toRole = row.querySelectorAll('.atlas-role')[1]?.value || '';
    const toOrder = row.querySelectorAll('.atlas-order')[1]?.value || '';
    const from = {};
    const to = {};
    if (fromRole) from.role = fromRole;
    if (fromOrder) from.output_order_token = fromOrder;
    if (toRole) to.role = toRole;
    if (toOrder) to.output_order_token = toOrder;
    if (!Object.keys(from).length || !Object.keys(to).length) return;
    const predicate = { from, to };
    if (from.output_order_token && to.output_order_token) {
      predicate.transition_token = `${from.output_order_token}->${to.output_order_token}`;
    }
    rows.push(predicate);
  });
  return rows;
}

export function readAtlasQueryBuilderState(nodeId) {
  return {
    builderRequiredRegimes: collectAtlasRegimeRows(nodeId, 'builder-required-regimes'),
    builderForbiddenRegimes: collectAtlasRegimeRows(nodeId, 'builder-forbidden-regimes'),
    builderRequiredTransitions: collectAtlasTransitionRows(nodeId, 'builder-required-transitions'),
    builderWitnessSequence: collectAtlasRegimeRows(nodeId, 'builder-witness-sequence'),
  };
}

export function restoreAtlasQueryBuilderState(nodeId, data = {}) {
  clearAtlasBuilderRows(nodeId, 'builder-required-regimes');
  clearAtlasBuilderRows(nodeId, 'builder-forbidden-regimes');
  clearAtlasBuilderRows(nodeId, 'builder-required-transitions');
  clearAtlasBuilderRows(nodeId, 'builder-witness-sequence');
  (data.builderRequiredRegimes || []).forEach(item => addAtlasBuilderRow(nodeId, 'builder-required-regimes', 'regime', item));
  (data.builderForbiddenRegimes || []).forEach(item => addAtlasBuilderRow(nodeId, 'builder-forbidden-regimes', 'regime', item));
  (data.builderRequiredTransitions || []).forEach(item => addAtlasBuilderRow(nodeId, 'builder-required-transitions', 'transition', item));
  (data.builderWitnessSequence || []).forEach(item => addAtlasBuilderRow(nodeId, 'builder-witness-sequence', 'regime', item));
}

/* ------------------------------------------------------------------ */
/*  Behavior sketch                                                    */
/* ------------------------------------------------------------------ */

export function atlasSketchSeriesFromMotif(label) {
  const key = String(label || '').toLowerCase();
  if (!key) return null;
  const presets = {
    activation_with_saturation: [0.12, 0.2, 0.52, 0.82, 0.88],
    thresholded_activation: [0.12, 0.12, 0.18, 0.74, 0.92],
    monotone_activation: [0.12, 0.28, 0.46, 0.68, 0.9],
    thresholded_repression: [0.9, 0.88, 0.8, 0.34, 0.1],
    repression_with_floor: [0.92, 0.72, 0.4, 0.18, 0.12],
    monotone_repression: [0.92, 0.74, 0.52, 0.28, 0.1],
    biphasic_peak: [0.12, 0.32, 0.9, 0.46, 0.16],
    biphasic_valley: [0.88, 0.62, 0.12, 0.54, 0.84],
    flat: [0.5, 0.5, 0.5, 0.5, 0.5],
  };
  return presets[key] || null;
}

export function atlasSketchSeriesFromWitness(sequence) {
  if (!Array.isArray(sequence) || !sequence.length) return null;
  const mapToken = (token) => {
    const text = String(token || '').trim();
    if (!text) return 0.5;
    if (text === '+Inf') return 0.92;
    if (text === '-Inf') return 0.08;
    const num = Number(text);
    if (Number.isFinite(num)) {
      const clamped = Math.max(-2, Math.min(2, num));
      return 0.5 - clamped * 0.18;
    }
    return 0.5;
  };
  return sequence.map(item => mapToken(item.output_order_token || item));
}

export function renderAtlasBehaviorSketch(payload) {
  const query = payload?.query || {};
  const goal = query.goal || {};
  const motifLabel = Array.isArray(goal.motif) ? goal.motif[0] : (Array.isArray(query.motif_labels) ? query.motif_labels[0] : goal.motif);
  let series = atlasSketchSeriesFromMotif(motifLabel);
  let caption = motifLabel ? `motif: ${motifLabel}` : 'behavior sketch';

  if (!series) {
    const witnessSeq = Array.isArray(query.required_path_sequences) && query.required_path_sequences.length
      ? query.required_path_sequences[0]
      : [];
    series = atlasSketchSeriesFromWitness(witnessSeq);
    if (series) caption = 'witness path sketch';
  }

  if (!series || !series.length) {
    return `
      <div class="atlas-sketch-empty">
        <span class="text-dim">Add a goal motif or witness stages to preview a qualitative behavior sketch.</span>
      </div>
    `;
  }

  const width = 260;
  const height = 108;
  const left = 18;
  const top = 12;
  const plotWidth = width - left * 2;
  const plotHeight = 64;
  const points = series.map((value, idx) => {
    const x = left + (plotWidth * idx) / Math.max(series.length - 1, 1);
    const y = top + (1 - value) * plotHeight;
    return [x, y];
  });
  const pathD = points.map(([x, y], idx) => `${idx === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
  const pointDots = points.map(([x, y]) => `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="3.2"></circle>`).join('');

  return `
    <div class="atlas-sketch-card">
      <svg class="atlas-sketch-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
        <line x1="${left}" y1="${top + plotHeight}" x2="${width - left}" y2="${top + plotHeight}" class="atlas-sketch-axis"></line>
        <line x1="${left}" y1="${top}" x2="${left}" y2="${top + plotHeight}" class="atlas-sketch-axis"></line>
        <path d="${pathD}" class="atlas-sketch-line"></path>
        ${pointDots}
      </svg>
      <div class="atlas-sketch-caption">${escapeHtml(caption)}</div>
    </div>
  `;
}

/* ------------------------------------------------------------------ */
/*  Query designer                                                     */
/* ------------------------------------------------------------------ */

export function refreshAtlasQueryDesigner(nodeId) {
  const previewEl = document.getElementById(`${nodeId}-query-preview`);
  const sketchEl = document.getElementById(`${nodeId}-behavior-sketch`);
  if (!previewEl && !sketchEl) return;
  try {
    const payload = atlasQueryPayloadFromState(readAtlasQueryEditorState(nodeId));
    if (previewEl) previewEl.textContent = JSON.stringify(payload.query, null, 2);
    if (sketchEl) sketchEl.innerHTML = renderAtlasBehaviorSketch(payload);
  } catch (error) {
    if (previewEl) previewEl.textContent = error.message;
    if (sketchEl) sketchEl.innerHTML = '<div class="atlas-sketch-empty"><span class="text-dim">Fix the query fields to preview the behavior sketch.</span></div>';
  }
}

/* ------------------------------------------------------------------ */
/*  Spec editor                                                        */
/* ------------------------------------------------------------------ */

export function readAtlasSpecEditorState(nodeId) {
  return {
    sourceLabel: document.getElementById(`${nodeId}-source-label`)?.value || '',
    libraryLabel: document.getElementById(`${nodeId}-library-label`)?.value || '',
    sqlitePath: document.getElementById(`${nodeId}-sqlite-path`)?.value || '',
    persistSqlite: document.getElementById(`${nodeId}-persist-sqlite`)?.checked ?? false,
    skipExisting: document.getElementById(`${nodeId}-skip-existing`)?.checked ?? true,
    profileName: document.getElementById(`${nodeId}-profile-name`)?.value || 'binding_small_v0',
    maxBaseSpecies: parseInt(document.getElementById(`${nodeId}-max-base-species`)?.value || '4', 10),
    maxReactions: parseInt(document.getElementById(`${nodeId}-max-reactions`)?.value || '5', 10),
    maxSupport: parseInt(document.getElementById(`${nodeId}-max-support`)?.value || '3', 10),
    pathScope: document.getElementById(`${nodeId}-path-scope`)?.value || 'feasible',
    minVolumeMean: parseFloat(document.getElementById(`${nodeId}-min-volume`)?.value || '0.01'),
    keepSingular: document.getElementById(`${nodeId}-keep-singular`)?.checked ?? true,
    keepNonasymptotic: document.getElementById(`${nodeId}-keep-nonasym`)?.checked ?? false,
    includePathRecords: document.getElementById(`${nodeId}-include-path-records`)?.checked ?? false,
    enableEnumeration: document.getElementById(`${nodeId}-enable-enumeration`)?.checked ?? true,
    enumerationMode: document.getElementById(`${nodeId}-enum-mode`)?.value || 'pairwise_binding',
    baseSpeciesCountsText: document.getElementById(`${nodeId}-base-species-counts`)?.value || '2,3',
    minEnumerationReactions: parseInt(document.getElementById(`${nodeId}-min-enum-reactions`)?.value || '1', 10),
    maxEnumerationReactions: parseInt(document.getElementById(`${nodeId}-max-enum-reactions`)?.value || '2', 10),
    enumerationLimit: parseInt(document.getElementById(`${nodeId}-enum-limit`)?.value || '0', 10),
    explicitNetworksText: document.getElementById(`${nodeId}-explicit-networks`)?.value || '',
  };
}

export function atlasSpecPayloadFromState(rawState) {
  const state = { ...rawState };
  const explicitNetworks = parseAtlasExplicitNetworks(state.explicitNetworksText);
  const baseSpeciesCounts = splitCommaList(state.baseSpeciesCountsText)
    .map(item => parseInt(item, 10))
    .filter(Number.isFinite);

  const spec = {
    search_profile: {
      name: state.profileName || 'binding_small_v0',
      max_base_species: state.maxBaseSpecies,
      max_reactions: state.maxReactions,
      max_support: state.maxSupport,
    },
    behavior_config: {
      path_scope: state.pathScope,
      min_volume_mean: state.minVolumeMean,
      keep_singular: state.keepSingular,
      keep_nonasymptotic: state.keepNonasymptotic,
      include_path_records: state.includePathRecords,
    },
  };

  const sourceLabel = String(state.sourceLabel || '').trim();
  const libraryLabel = String(state.libraryLabel || '').trim();
  const sqlitePath = String(state.sqlitePath || '').trim();
  if (sourceLabel) spec.source_label = sourceLabel;
  if (libraryLabel) spec.library_label = libraryLabel;
  if (sqlitePath) spec.sqlite_path = sqlitePath;
  spec.skip_existing = !!state.skipExisting;
  if (sqlitePath) spec.persist_sqlite = !!state.persistSqlite;

  if (explicitNetworks.length) {
    spec.networks = explicitNetworks;
  }

  if (state.enableEnumeration) {
    spec.enumeration = {
      mode: state.enumerationMode || 'pairwise_binding',
      base_species_counts: baseSpeciesCounts.length ? baseSpeciesCounts : [2, 3],
      min_reactions: Math.min(state.minEnumerationReactions, state.maxEnumerationReactions),
      max_reactions: Math.max(state.minEnumerationReactions, state.maxEnumerationReactions),
      limit: Math.max(0, state.enumerationLimit || 0),
    };
  }

  if (!spec.networks && !spec.enumeration) {
    throw new Error('Atlas spec must include explicit networks or enable enumeration.');
  }

  return { serial: state, spec };
}

export function getConnectedAtlasSpec(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'atlas-spec');
  if (!conn) return null;
  const sourceNodeId = conn.fromNode;
  return atlasSpecPayloadFromState(getNodeSerialData(sourceNodeId, 'atlas-spec'));
}

/* ------------------------------------------------------------------ */
/*  Query editor                                                       */
/* ------------------------------------------------------------------ */

export function readAtlasQueryEditorState(nodeId) {
  return {
    sqlitePath: document.getElementById(`${nodeId}-query-sqlite-path`)?.value || '',
    preferPersistedAtlas: document.getElementById(`${nodeId}-prefer-persisted-atlas`)?.checked ?? true,
    goalIoText: document.getElementById(`${nodeId}-goal-io`)?.value || '',
    goalMotifText: document.getElementById(`${nodeId}-goal-motif`)?.value || '',
    goalExactText: document.getElementById(`${nodeId}-goal-exact`)?.value || '',
    goalWitnessText: document.getElementById(`${nodeId}-goal-witness`)?.value || '',
    goalTransitionsText: document.getElementById(`${nodeId}-goal-transitions`)?.value || '',
    goalForbidRegimesText: document.getElementById(`${nodeId}-goal-forbid-regimes`)?.value || '',
    goalRobust: document.getElementById(`${nodeId}-goal-robust`)?.checked ?? false,
    goalFeasible: document.getElementById(`${nodeId}-goal-feasible`)?.checked ?? false,
    goalMinVolumeMean: parseOptionalFloat(document.getElementById(`${nodeId}-goal-min-volume`)?.value),
    motifLabelsText: document.getElementById(`${nodeId}-motif-labels`)?.value || '',
    motifMatchMode: document.getElementById(`${nodeId}-motif-match-mode`)?.value || 'any',
    exactLabelsText: document.getElementById(`${nodeId}-exact-labels`)?.value || '',
    exactMatchMode: document.getElementById(`${nodeId}-exact-match-mode`)?.value || 'any',
    inputSymbolsText: document.getElementById(`${nodeId}-input-symbols`)?.value || '',
    outputSymbolsText: document.getElementById(`${nodeId}-output-symbols`)?.value || '',
    requireRobust: document.getElementById(`${nodeId}-require-robust`)?.checked ?? false,
    minRobustPathCount: parseInt(document.getElementById(`${nodeId}-min-robust-path-count`)?.value || '0', 10),
    maxBaseSpecies: parseOptionalInteger(document.getElementById(`${nodeId}-query-max-base-species`)?.value),
    maxReactions: parseOptionalInteger(document.getElementById(`${nodeId}-query-max-reactions`)?.value),
    maxSupport: parseOptionalInteger(document.getElementById(`${nodeId}-query-max-support`)?.value),
    maxSupportMass: parseOptionalInteger(document.getElementById(`${nodeId}-query-max-support-mass`)?.value),
    requiredRegimesText: document.getElementById(`${nodeId}-required-regimes`)?.value || '',
    forbiddenRegimesText: document.getElementById(`${nodeId}-forbidden-regimes`)?.value || '',
    requiredTransitionsText: document.getElementById(`${nodeId}-required-transitions`)?.value || '',
    forbiddenTransitionsText: document.getElementById(`${nodeId}-forbidden-transitions`)?.value || '',
    requiredPathSequencesText: document.getElementById(`${nodeId}-required-path-sequences`)?.value || '',
    forbidSingularOnWitness: document.getElementById(`${nodeId}-forbid-singular-on-witness`)?.checked ?? false,
    maxWitnessPathLength: parseOptionalInteger(document.getElementById(`${nodeId}-max-witness-path-length`)?.value),
    requireWitnessFeasible: document.getElementById(`${nodeId}-require-witness-feasible`)?.checked ?? false,
    requireWitnessRobust: document.getElementById(`${nodeId}-require-witness-robust`)?.checked ?? false,
    minWitnessVolumeMean: parseOptionalFloat(document.getElementById(`${nodeId}-min-witness-volume-mean`)?.value),
    rankingMode: document.getElementById(`${nodeId}-ranking-mode`)?.value || 'minimal_first',
    collapseByNetwork: document.getElementById(`${nodeId}-collapse-by-network`)?.checked ?? true,
    paretoOnly: document.getElementById(`${nodeId}-pareto-only`)?.checked ?? false,
    limit: parseInt(document.getElementById(`${nodeId}-query-limit`)?.value || '20', 10),
    inverseSourceLabel: document.getElementById(`${nodeId}-inverse-source-label`)?.value || 'inverse_design_run',
    inverseSkipExisting: document.getElementById(`${nodeId}-inverse-skip-existing`)?.checked ?? true,
    inverseBuildLibraryIfMissing: document.getElementById(`${nodeId}-inverse-build-library-if-missing`)?.checked ?? true,
    allowDuplicateAtlas: document.getElementById(`${nodeId}-allow-duplicate-atlas`)?.checked ?? false,
    refinementEnabled: document.getElementById(`${nodeId}-refinement-enabled`)?.checked ?? false,
    refinementTopK: parseInt(document.getElementById(`${nodeId}-refinement-top-k`)?.value || '3', 10),
    refinementTrials: parseInt(document.getElementById(`${nodeId}-refinement-trials`)?.value || '6', 10),
    refinementNPoints: parseInt(document.getElementById(`${nodeId}-refinement-n-points`)?.value || '200', 10),
    refinementIncludeTraces: document.getElementById(`${nodeId}-refinement-include-traces`)?.checked ?? false,
    refinementRerank: document.getElementById(`${nodeId}-refinement-rerank`)?.checked ?? true,
    ...readAtlasQueryBuilderState(nodeId),
  };
}

export function atlasQueryPayloadFromState(rawState) {
  const state = { ...rawState };
  const dedupeObjects = (items) => {
    const seen = new Set();
    return (items || []).filter(item => {
      const key = JSON.stringify(item);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  };
  const requiredRegimes = dedupeObjects([
    ...normalizePredicateArray(parseOptionalJson(state.requiredRegimesText, [], 'Required regimes'), 'Required regimes'),
    ...(Array.isArray(state.builderRequiredRegimes) ? state.builderRequiredRegimes : []),
  ]);
  const forbiddenRegimes = dedupeObjects([
    ...normalizePredicateArray(parseOptionalJson(state.forbiddenRegimesText, [], 'Forbidden regimes'), 'Forbidden regimes'),
    ...(Array.isArray(state.builderForbiddenRegimes) ? state.builderForbiddenRegimes : []),
  ]);
  const requiredTransitions = dedupeObjects([
    ...normalizePredicateArray(parseOptionalJson(state.requiredTransitionsText, [], 'Required transitions'), 'Required transitions'),
    ...(Array.isArray(state.builderRequiredTransitions) ? state.builderRequiredTransitions : []),
  ]);
  const forbiddenTransitions = normalizePredicateArray(parseOptionalJson(state.forbiddenTransitionsText, [], 'Forbidden transitions'), 'Forbidden transitions');
  const requiredPathSequences = normalizePredicateSequenceArray(parseOptionalJson(state.requiredPathSequencesText, [], 'Required path sequences'), 'Required path sequences');
  if (Array.isArray(state.builderWitnessSequence) && state.builderWitnessSequence.length) {
    requiredPathSequences.push(state.builderWitnessSequence);
  }

  const query = {
    motif_labels: splitCommaList(state.motifLabelsText),
    motif_match_mode: state.motifMatchMode || 'any',
    exact_labels: splitCommaList(state.exactLabelsText),
    exact_match_mode: state.exactMatchMode || 'any',
    input_symbols: splitCommaList(state.inputSymbolsText),
    output_symbols: splitCommaList(state.outputSymbolsText),
    require_robust: !!state.requireRobust,
    min_robust_path_count: Math.max(0, state.minRobustPathCount || 0),
    ranking_mode: state.rankingMode || 'minimal_first',
    collapse_by_network: !!state.collapseByNetwork,
    pareto_only: !!state.paretoOnly,
    limit: Math.max(1, state.limit || 20),
    required_regimes: requiredRegimes,
    forbidden_regimes: forbiddenRegimes,
    required_transitions: requiredTransitions,
    forbidden_transitions: forbiddenTransitions,
    required_path_sequences: requiredPathSequences,
    forbid_singular_on_witness: !!state.forbidSingularOnWitness,
    require_witness_feasible: !!state.requireWitnessFeasible,
    require_witness_robust: !!state.requireWitnessRobust,
  };

  if (state.maxBaseSpecies != null) query.max_base_species = state.maxBaseSpecies;
  if (state.maxReactions != null) query.max_reactions = state.maxReactions;
  if (state.maxSupport != null) query.max_support = state.maxSupport;
  if (state.maxSupportMass != null) query.max_support_mass = state.maxSupportMass;
  if (state.maxWitnessPathLength != null) query.max_witness_path_length = state.maxWitnessPathLength;
  if (state.minWitnessVolumeMean != null) query.min_witness_volume_mean = state.minWitnessVolumeMean;

  query.graph_spec = {
    required_regimes: requiredRegimes,
    forbidden_regimes: forbiddenRegimes,
    required_transitions: requiredTransitions,
    forbidden_transitions: forbiddenTransitions,
  };
  query.path_spec = {
    required_path_sequences: requiredPathSequences,
    forbid_singular_on_witness: !!state.forbidSingularOnWitness,
  };
  query.polytope_spec = {
    require_feasible: !!state.requireWitnessFeasible,
    require_robust: !!state.requireWitnessRobust,
  };
  if (state.maxWitnessPathLength != null) query.path_spec.max_path_length = state.maxWitnessPathLength;
  if (state.minWitnessVolumeMean != null) query.polytope_spec.min_volume_mean = state.minWitnessVolumeMean;

  const goal = {};
  const goalIo = String(state.goalIoText || '').trim();
  const goalMotif = String(state.goalMotifText || '').trim();
  const goalExact = String(state.goalExactText || '').trim();
  const goalWitness = String(state.goalWitnessText || '').trim();
  const goalTransitions = splitCommaList(state.goalTransitionsText);
  const goalForbidRegimes = splitCommaList(state.goalForbidRegimesText);
  if (goalIo) goal.io = goalIo;
  if (goalMotif) goal.motif = splitCommaList(goalMotif);
  if (goalExact) goal.exact = splitCommaList(goalExact);
  if (goalWitness) goal.witness = goalWitness;
  if (goalTransitions.length) goal.must_transitions = goalTransitions;
  if (goalForbidRegimes.length) goal.forbid_regimes = goalForbidRegimes;
  if (state.goalRobust) goal.robust = true;
  if (state.goalFeasible) goal.feasible = true;
  if (state.goalMinVolumeMean != null) goal.min_volume = state.goalMinVolumeMean;
  if (Object.keys(goal).length) query.goal = goal;

  const inverseDesign = {
    source_label: String(state.inverseSourceLabel || '').trim() || 'inverse_design_run',
    skip_existing: !!state.inverseSkipExisting,
    build_library_if_missing: !!state.inverseBuildLibraryIfMissing,
    return_library: false,
    return_delta_atlas: false,
  };

  const refinement = {
    enabled: !!state.refinementEnabled,
    top_k: Math.max(1, state.refinementTopK || 3),
    trials: Math.max(1, state.refinementTrials || 6),
    n_points: Math.max(20, state.refinementNPoints || 200),
    include_traces: !!state.refinementIncludeTraces,
    rerank_by_refinement: !!state.refinementRerank,
  };

  return {
    serial: state,
    query,
    inverseDesign,
    refinement,
    allowDuplicateAtlas: !!state.allowDuplicateAtlas,
    sqlitePath: String(state.sqlitePath || '').trim(),
    preferPersistedAtlas: !!state.preferPersistedAtlas,
  };
}

export function getConnectedAtlasQuery(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'atlas-query');
  if (!conn) return null;
  const sourceNodeId = conn.fromNode;
  return atlasQueryPayloadFromState(getNodeSerialData(sourceNodeId, 'atlas-query-config'));
}

export function getConnectedAtlasData(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'atlas');
  if (!conn) return null;
  return nodeRegistry[conn.fromNode]?.data?.atlasData || null;
}

/* ------------------------------------------------------------------ */
/*  Rendering helpers                                                  */
/* ------------------------------------------------------------------ */

export function formatAtlasStatusTag(status) {
  const label = String(status || 'unknown');
  let cls = 'tag-atlas-neutral';
  if (label === 'ok') cls = 'tag-atlas-ok';
  else if (label === 'failed') cls = 'tag-atlas-failed';
  else if (label === 'excluded_by_search_profile') cls = 'tag-atlas-excluded';
  return `<span class="tag ${cls}">${escapeHtml(label)}</span>`;
}

export function renderAtlasLabelRefs(labels) {
  const unique = Array.from(new Set((labels || []).filter(Boolean)));
  if (!unique.length) return '<span class="text-dim">none</span>';
  return unique.map(label => `<span class="family-ref">${escapeHtml(label)}</span>`).join('');
}

export function renderTokenRefs(tokens) {
  const items = Array.isArray(tokens) ? tokens.filter(Boolean) : [];
  if (!items.length) return '<span class="text-dim">none</span>';
  return items.map(token => `<span class="family-ref">${escapeHtml(token)}</span>`).join('');
}

export function renderWitnessPathSummary(path) {
  if (!path) return '<span class="text-dim">none</span>';
  const orderTokens = renderTokenRefs(path.output_order_tokens || []);
  const transitionTokens = renderTokenRefs(path.transition_tokens || []);
  const volumeMean = path.volume?.mean;
  return `
    <div class="family-meta">
      <span class="family-metric">path ${path.path_idx ?? '-'}</span>
      <span class="family-metric">${path.robust ? 'robust' : 'non-robust'}</span>
      <span class="family-metric">feasible ${path.feasible ? 'yes' : 'no'}</span>
      <span class="family-metric">vol ${Number.isFinite(volumeMean) ? Number(volumeMean).toFixed(3) : 'n/a'}</span>
    </div>
    <div>
      <div class="family-kicker">Regime Tokens</div>
      <div class="siso-wrap-cell">${orderTokens}</div>
    </div>
    <div>
      <div class="family-kicker">Transition Tokens</div>
      <div class="siso-wrap-cell">${transitionTokens}</div>
    </div>
  `;
}

export function renderAtlasRules(rules) {
  const list = Array.isArray(rules) ? rules : [];
  if (!list.length) return '<span class="text-dim">No reactions recorded.</span>';
  return `<div class="atlas-rule-list">${list.map(rule => `<code>${escapeHtml(rule)}</code>`).join('')}</div>`;
}

export function renderAtlasCodeBlock(value, fallback = 'none') {
  if (value == null) return `<span class="text-dim">${escapeHtml(fallback)}</span>`;
  const text = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
  return `<pre class="atlas-code-block">${escapeHtml(text)}</pre>`;
}

export function formatAtlasRegimeRecord(record) {
  if (!record || typeof record !== 'object') return 'regime';
  const role = String(record.role || 'node');
  const token = String(record.output_order_token || '?');
  const singular = record.singular === true ? ' singular' : (record.singular === false ? ' regular' : '');
  const vertex = record.vertex_idx != null ? ` v${record.vertex_idx}` : '';
  return `${role}:${token}${singular}${vertex}`;
}

export function formatAtlasTransitionRecord(record) {
  if (!record || typeof record !== 'object') return 'transition';
  if (record.transition_token) return String(record.transition_token);
  const fromRole = String(record.from_role || record.from?.role || 'from');
  const fromToken = String(record.from_output_order_token || record.from?.output_order_token || '?');
  const toRole = String(record.to_role || record.to?.role || 'to');
  const toToken = String(record.to_output_order_token || record.to?.output_order_token || '?');
  return `${fromRole}:${fromToken} -> ${toRole}:${toToken}`;
}

export function formatAtlasBucketRecord(bucket) {
  if (!bucket || typeof bucket !== 'object') return 'bucket';
  const kind = String(bucket.family_kind || 'family');
  const label = String(bucket.family_label || bucket.bucket_id || kind);
  const robust = bucket.robust_path_count != null ? ` robust ${bucket.robust_path_count}` : '';
  const paths = bucket.path_count != null ? ` paths ${bucket.path_count}` : '';
  return `${kind} ${label}${paths}${robust}`;
}

export function renderAtlasMetricChipRows(rows) {
  const items = Array.isArray(rows) ? rows.filter(Boolean) : [];
  if (!items.length) return '';
  return `<div class="family-meta">${items.map(item => `<span class="family-metric">${escapeHtml(item)}</span>`).join('')}</div>`;
}

export function renderAtlasRecordRefs(records, formatter, emptyLabel = 'none') {
  const items = Array.isArray(records) ? records : [];
  if (!items.length) return `<span class="text-dim">${escapeHtml(emptyLabel)}</span>`;
  return items.map(item => `<span class="family-ref">${escapeHtml(formatter(item))}</span>`).join('');
}

export function renderAtlasStageSummary(stage) {
  if (!stage || typeof stage !== 'object') return '';
  const bits = [];
  if (stage.reason) bits.push(`reason ${stage.reason}`);
  if (stage.motif_bucket_count != null) bits.push(`motif ${stage.motif_bucket_count}`);
  if (stage.exact_bucket_count != null) bits.push(`exact ${stage.exact_bucket_count}`);
  if (stage.materialized_bucket_count != null) bits.push(`buckets ${stage.materialized_bucket_count}`);
  if (stage.result && typeof stage.result === 'object') {
    if (stage.result.pass != null) bits.push(`pass ${stage.result.pass}`);
    if (stage.result.reason) bits.push(stage.result.reason);
    if (stage.result.support_count != null) bits.push(`support ${stage.result.support_count}`);
  }
  if (stage.certificate?.reason) bits.push(`certificate ${stage.certificate.reason}`);
  if (stage.note?.reason) bits.push(`note ${stage.note.reason}`);
  return renderAtlasMetricChipRows(bits);
}

export function renderAtlasStageDetails(stage, idx) {
  if (!stage || typeof stage !== 'object') return '';
  const detailBlocks = [];
  if (stage.result) {
    detailBlocks.push(`
      <div class="atlas-detail-block">
        <div class="family-kicker">Result</div>
        ${renderAtlasCodeBlock(stage.result)}
      </div>
    `);
  }
  if (stage.certificate) {
    detailBlocks.push(`
      <div class="atlas-detail-block">
        <div class="family-kicker">Certificate</div>
        ${renderAtlasCodeBlock(stage.certificate)}
      </div>
    `);
  }
  if (stage.note) {
    detailBlocks.push(`
      <div class="atlas-detail-block">
        <div class="family-kicker">Note</div>
        ${renderAtlasCodeBlock(stage.note)}
      </div>
    `);
  }

  return `
    <div class="atlas-trace-stage">
      <div class="atlas-trace-stage-head">
        <div>
          <div class="family-kicker">Stage ${idx + 1}</div>
          <div class="atlas-trace-stage-title">${escapeHtml(stage.stage || 'stage')}</div>
        </div>
        ${formatTraceStatusTag(stage.status || 'unknown')}
      </div>
      ${renderAtlasStageSummary(stage)}
      ${detailBlocks.join('')}
    </div>
  `;
}

export function renderAtlasTraceDetails(trace) {
  const stages = Array.isArray(trace?.stages) ? trace.stages : [];
  const focus = atlasTraceFocus(trace);
  return `
    <details class="atlas-detail-card">
      <summary class="atlas-detail-summary">
        <div>
          <strong>${escapeHtml(atlasTraceLabel(trace))}</strong>
          <div class="text-dim">${escapeHtml(focus || String(trace?.network_id || 'candidate'))}</div>
        </div>
        <div class="family-meta">
          ${formatTraceStatusTag(trace?.status)}
          <span class="family-metric">${stages.length} stages</span>
        </div>
      </summary>
      <div class="atlas-detail-body">
        ${stages.length ? stages.map((stage, idx) => renderAtlasStageDetails(stage, idx)).join('') : '<span class="text-dim">No stage details recorded.</span>'}
      </div>
    </details>
  `;
}

export function renderAtlasMaterializationEventCards(events, limit = 8) {
  const items = Array.isArray(events) ? events.slice(0, limit) : [];
  if (!items.length) return '<span class="text-dim">No materialization events recorded.</span>';
  return items.map(event => `
    <details class="atlas-detail-card">
      <summary class="atlas-detail-summary">
        <div>
          <strong>${escapeHtml(event.bucket_id || 'bucket')}</strong>
          <div class="text-dim">${escapeHtml(event.reason || 'materialization')}</div>
        </div>
        <div class="family-meta">
          <span class="family-metric">paths ${event.materialized_path_count ?? 0}</span>
          <span class="family-metric">accepted ${event.accepted_path_count ?? 0}</span>
          <span class="family-metric">${escapeHtml(event.mat_state || 'summary')}</span>
        </div>
      </summary>
      <div class="atlas-detail-body">
        ${renderAtlasMetricChipRows([
          event.slice_id ? `slice ${event.slice_id}` : '',
          event.volume_policy ? `volume ${event.volume_policy}` : '',
          event.budget != null ? `budget ${event.budget}` : '',
          event.materialized_at ? `at ${event.materialized_at}` : '',
        ])}
        ${renderAtlasCodeBlock(event)}
      </div>
    </details>
  `).join('');
}

export function atlasResultEvidence(result, resultUnit) {
  if (resultUnit === 'network') {
    return {
      motifBuckets: result.best_matched_motif_buckets || [],
      exactBuckets: result.best_matched_exact_buckets || [],
      regimeRecords: result.best_matched_regime_records || [],
      transitionRecords: result.best_matched_transition_records || [],
      witnessPaths: result.best_witness_path ? [result.best_witness_path] : [],
      trace: [],
      materializationRecords: [],
    };
  }
  return {
    motifBuckets: result.matched_motif_buckets || [],
    exactBuckets: result.matched_exact_buckets || [],
    regimeRecords: result.matched_regime_records || [],
    transitionRecords: result.matched_transition_records || [],
    witnessPaths: result.matched_witness_paths || [],
    trace: result.pruning_trace || [],
    materializationRecords: result.materialization_records || [],
  };
}

export function renderAtlasResultExplain(result, resultUnit) {
  const evidence = atlasResultEvidence(result, resultUnit);
  return `
    <details class="atlas-detail-card">
      <summary class="atlas-detail-summary">
        <div>
          <strong>Explain Match</strong>
          <div class="text-dim">Matched evidence, witness materialization, and stage trace</div>
        </div>
        <div class="family-meta">
          <span class="family-metric">buckets ${(evidence.motifBuckets.length || 0) + (evidence.exactBuckets.length || 0)}</span>
          <span class="family-metric">regimes ${evidence.regimeRecords.length}</span>
          <span class="family-metric">transitions ${evidence.transitionRecords.length}</span>
          <span class="family-metric">witness ${evidence.witnessPaths.length}</span>
        </div>
      </summary>
      <div class="atlas-detail-body">
        <div class="atlas-detail-grid">
          <div class="atlas-detail-block">
            <div class="family-kicker">Matched Buckets</div>
            <div class="siso-wrap-cell">
              ${renderAtlasRecordRefs([...evidence.motifBuckets, ...evidence.exactBuckets], formatAtlasBucketRecord, 'none')}
            </div>
          </div>
          <div class="atlas-detail-block">
            <div class="family-kicker">Matched Regimes</div>
            <div class="siso-wrap-cell">
              ${renderAtlasRecordRefs(evidence.regimeRecords, formatAtlasRegimeRecord, 'none')}
            </div>
          </div>
          <div class="atlas-detail-block">
            <div class="family-kicker">Matched Transitions</div>
            <div class="siso-wrap-cell">
              ${renderAtlasRecordRefs(evidence.transitionRecords, formatAtlasTransitionRecord, 'none')}
            </div>
          </div>
        </div>
        ${evidence.witnessPaths.length ? `
          <div class="atlas-detail-block">
            <div class="family-kicker">Witness Paths</div>
            <div class="atlas-stack">
              ${evidence.witnessPaths.slice(0, 3).map(path => `
                <div class="atlas-inline-card">${renderWitnessPathSummary(path)}</div>
              `).join('')}
            </div>
          </div>
        ` : ''}
        ${evidence.materializationRecords.length ? `
          <div class="atlas-detail-block">
            <div class="family-kicker">Materialization Records</div>
            <div class="atlas-stack">
              ${evidence.materializationRecords.slice(0, 3).map(record => `
                <div class="atlas-inline-card">
                  ${renderAtlasMetricChipRows([
                    record.status ? `status ${record.status}` : '',
                    record.bucket_id ? `bucket ${record.bucket_id}` : '',
                    record.slice_id ? `slice ${record.slice_id}` : '',
                    record.exhaustive != null ? `exhaustive ${record.exhaustive}` : '',
                  ])}
                  ${record.event ? renderAtlasCodeBlock(record.event) : renderAtlasCodeBlock(record)}
                </div>
              `).join('')}
            </div>
          </div>
        ` : ''}
        ${evidence.trace.length ? `
          <div class="atlas-detail-block">
            <div class="family-kicker">Stage Trace</div>
            ${evidence.trace.map((stage, idx) => renderAtlasStageDetails(stage, idx)).join('')}
          </div>
        ` : ''}
      </div>
    </details>
  `;
}

export function formatTraceStatusTag(status) {
  const label = String(status || 'unknown');
  let cls = 'tag-atlas-neutral';
  if (label === 'accepted' || label === 'ok' || label === 'queued' || label === 'pass') cls = 'tag-atlas-ok';
  else if (label === 'excluded') cls = 'tag-atlas-excluded';
  else if (label === 'pruned' || label === 'soft_fail' || label === 'exact_reject' || label === 'partial') cls = 'tag-atlas-failed';
  return `<span class="tag ${cls}">${escapeHtml(label)}</span>`;
}

export function atlasInterestingTraceStage(trace) {
  const stages = Array.isArray(trace?.stages) ? trace.stages : [];
  const interesting = stages.filter(stage => {
    const status = String(stage?.status || '');
    return status && !['pass', 'pass_cached', 'pass_unresolved', 'queued', 'not_needed'].includes(status);
  });
  return interesting.length ? interesting[interesting.length - 1] : (stages[stages.length - 1] || null);
}

export function atlasTraceLabel(trace) {
  const candidateLabel = String(trace?.candidate_label || '').trim();
  if (candidateLabel) return candidateLabel;
  const sourceLabel = String(trace?.source_label || '').trim();
  if (sourceLabel) return sourceLabel;
  const networkId = String(trace?.network_id || '').trim();
  return networkId || 'candidate';
}

export function atlasTraceFocus(trace) {
  const input = String(trace?.input_symbol || '').trim();
  const output = String(trace?.output_symbol || '').trim();
  if (input || output) return `${input || '?'} -> ${output || '?'}`;
  return '';
}

export function summarizeAtlasTraces(traces) {
  const summary = {
    total: Array.isArray(traces) ? traces.length : 0,
    statuses: {},
    reasons: {},
  };
  (traces || []).forEach(trace => {
    const status = String(trace?.status || 'unknown');
    summary.statuses[status] = (summary.statuses[status] || 0) + 1;
    const stage = atlasInterestingTraceStage(trace);
    if (!stage) return;
    const stageStatus = String(stage.status || '');
    if (status === 'accepted' && !stage.reason && ['pass', 'pass_cached', 'pass_unresolved', 'queued', 'not_needed'].includes(stageStatus)) {
      return;
    }
    const reason = String(stage.reason || stage.stage || status || 'unknown');
    summary.reasons[reason] = (summary.reasons[reason] || 0) + 1;
  });
  summary.topReasons = Object.entries(summary.reasons)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 6);
  return summary;
}

export function renderAtlasTraceCards(traces, limit = 8) {
  const items = Array.isArray(traces) ? traces.slice(0, limit) : [];
  if (!items.length) return '<span class="text-dim">No candidate traces recorded.</span>';
  return `<div class="atlas-stack">${items.map(trace => renderAtlasTraceDetails(trace)).join('')}</div>`;
}

export function renderAtlasDiagnosticsSection(data, title = 'Execution Diagnostics') {
  const traces = Array.isArray(data?.candidate_traces) ? data.candidate_traces : [];
  const summary = summarizeAtlasTraces(traces);
  const negatives = Array.isArray(data?.new_negative_certificates) ? data.new_negative_certificates : [];
  const materializationEvents = Array.isArray(data?.materialization_events) ? data.materialization_events : [];
  const versions = data?.versions || {};
  const policies = data?.policies || {};
  const queryHash = String(data?.compiled_query?.h_Q || '').trim();
  const compilerVersion = String(versions.compiler_version || '').trim();
  const profileVersion = String(versions.profile_version || '').trim();
  const volumePolicy = String(policies.volume_policy || versions.volume_policy || '').trim();

  if (!summary.total && !negatives.length && !materializationEvents.length && !queryHash) return '';

  return `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">${escapeHtml(title)}</div>
        <div class="text-dim">${summary.total ? `${summary.total} traces` : 'summary only'}</div>
      </div>
      <div class="siso-summary-line">
        ${Object.entries(summary.statuses).map(([status, count]) => `<span class="summary-chip">${escapeHtml(status)} ${count}</span>`).join('')}
        ${negatives.length ? `<span class="summary-chip">new negatives ${negatives.length}</span>` : ''}
        ${materializationEvents.length ? `<span class="summary-chip">materializations ${materializationEvents.length}</span>` : ''}
        ${volumePolicy ? `<span class="summary-chip">volume ${escapeHtml(volumePolicy)}</span>` : ''}
        ${profileVersion ? `<span class="summary-chip">profile ${escapeHtml(profileVersion)}</span>` : ''}
        ${compilerVersion ? `<span class="summary-chip">compiler ${escapeHtml(compilerVersion)}</span>` : ''}
      </div>
      ${queryHash ? `<div class="text-dim">query hash: ${escapeHtml(queryHash)}</div>` : ''}
      ${summary.topReasons.length ? `
        <div class="siso-summary-line">
          ${summary.topReasons.map(([reason, count]) => `<span class="summary-chip">${escapeHtml(reason)} ${count}</span>`).join('')}
        </div>
      ` : ''}
      ${summary.total ? `
        <details>
          <summary>Show sample candidate traces</summary>
          ${renderAtlasTraceCards(traces)}
        </details>
      ` : ''}
      ${negatives.length ? `
        <details>
          <summary>Show new negative certificates</summary>
          <div class="atlas-dedup-list">
            ${negatives.slice(0, 8).map(cert => `
              <div class="atlas-inline-card">
                <div><strong>${escapeHtml(cert.scope || 'scope')}</strong></div>
                <div class="text-dim">${escapeHtml(cert.reason || 'negative certificate')}</div>
                ${renderAtlasMetricChipRows([
                  cert.signature ? `signature ${cert.signature}` : '',
                  cert.kind ? `kind ${cert.kind}` : '',
                ])}
                ${renderAtlasCodeBlock(cert)}
              </div>
            `).join('')}
          </div>
        </details>
      ` : ''}
      ${materializationEvents.length ? `
        <details>
          <summary>Show materialization events</summary>
          <div class="atlas-stack">
            ${renderAtlasMaterializationEventCards(materializationEvents)}
          </div>
        </details>
      ` : ''}
    </section>
  `;
}

/* ------------------------------------------------------------------ */
/*  Result renderers                                                   */
/* ------------------------------------------------------------------ */

export function renderAtlasBuilderResult(data) {
  const entries = Array.isArray(data.network_entries) ? data.network_entries : [];
  const enumeration = data.enumeration || null;
  const sqliteSummary = data.sqlite_library_summary || null;
  const previewEntries = entries.slice(0, 12);

  let html = `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">Atlas Summary</div>
        <div class="text-dim">${data.generated_at || ''}</div>
      </div>
      <div class="siso-summary-grid">
        <div class="siso-stat-card"><div class="siso-stat-label">Input Networks</div><div class="siso-stat-value">${data.input_network_count ?? 0}</div></div>
        <div class="siso-stat-card"><div class="siso-stat-label">Unique Networks</div><div class="siso-stat-value">${data.unique_network_count ?? 0}</div></div>
        <div class="siso-stat-card"><div class="siso-stat-label">Successful</div><div class="siso-stat-value">${data.successful_network_count ?? 0}</div></div>
        <div class="siso-stat-card"><div class="siso-stat-label">Deduplicated</div><div class="siso-stat-value">${data.deduplicated_network_count ?? 0}</div></div>
      </div>
      <div class="siso-summary-line">
        ${data.pruned_against_library ? '<span class="summary-chip">reused library</span>' : ''}
        ${data.pruned_against_sqlite ? '<span class="summary-chip">reused sqlite</span>' : ''}
        ${data.sqlite_persisted ? '<span class="summary-chip">persisted sqlite</span>' : ''}
        <span class="summary-chip">skipped slices ${data.skipped_existing_slice_count ?? 0}</span>
        <span class="summary-chip">skipped networks ${data.skipped_existing_network_count ?? 0}</span>
      </div>
    </section>
  `;

  if (data.sqlite_path || sqliteSummary) {
    html += `
      <section class="siso-section">
        <div class="siso-section-head">
          <div class="siso-section-title">SQLite Store</div>
          <div class="text-dim">${data.sqlite_persisted ? 'updated' : (data.pruned_against_sqlite ? 'read-only reuse' : 'not used')}</div>
        </div>
        <div class="atlas-inline-card">
          <div><strong>Path</strong></div>
          <div class="text-dim atlas-path-inline">${escapeHtml(data.sqlite_path || 'n/a')}</div>
        </div>
        ${sqliteSummary ? `
          <div class="siso-summary-grid">
            <div class="siso-stat-card"><div class="siso-stat-label">Atlases</div><div class="siso-stat-value">${sqliteSummary.atlas_count ?? 0}</div></div>
            <div class="siso-stat-card"><div class="siso-stat-label">Networks</div><div class="siso-stat-value">${sqliteSummary.unique_network_count ?? 0}</div></div>
            <div class="siso-stat-card"><div class="siso-stat-label">Slices</div><div class="siso-stat-value">${sqliteSummary.behavior_slice_count ?? 0}</div></div>
            <div class="siso-stat-card"><div class="siso-stat-label">Buckets</div><div class="siso-stat-value">${sqliteSummary.family_bucket_count ?? 0}</div></div>
          </div>
        ` : ''}
      </section>
    `;
  }

  if (enumeration) {
    html += `
      <section class="siso-section">
        <div class="siso-section-head">
          <div class="siso-section-title">Enumeration</div>
          <div class="text-dim">${enumeration.truncated ? 'truncated' : 'complete'}</div>
        </div>
        <div class="siso-summary-line">
          <span class="summary-chip">generated ${enumeration.generated_network_count ?? 0}</span>
          <span class="summary-chip">mode ${escapeHtml(enumeration.enumeration_spec?.mode || 'unknown')}</span>
          <span class="summary-chip">base counts ${(enumeration.enumeration_spec?.base_species_counts || []).join(', ') || 'n/a'}</span>
        </div>
      </section>
    `;
  }

  html += `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">Networks</div>
        <div class="text-dim">showing ${previewEntries.length} of ${entries.length}</div>
      </div>
      <div class="siso-table-wrap scroll-panel">
        <table class="siso-family-table">
          <thead>
            <tr>
              <th>Label</th>
              <th>Status</th>
              <th>d</th>
              <th>r</th>
              <th>Support</th>
              <th>Motifs</th>
            </tr>
          </thead>
          <tbody>
            ${previewEntries.map(entry => `
              <tr>
                <td class="siso-wrap-cell">${escapeHtml(entry.source_label || entry.network_id || 'network')}</td>
                <td>${formatAtlasStatusTag(entry.analysis_status)}</td>
                <td>${entry.base_species_count ?? '-'}</td>
                <td>${entry.reaction_count ?? '-'}</td>
                <td>${entry.max_support ?? '-'}</td>
                <td class="siso-wrap-cell">${renderAtlasLabelRefs(entry.motif_union || [])}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;

  if (data.duplicate_inputs?.length) {
    html += `
      <section class="siso-section">
        <div class="siso-section-head">
          <div class="siso-section-title">Deduplicated Inputs</div>
          <div class="text-dim">${data.duplicate_inputs.length}</div>
        </div>
        <div class="atlas-dedup-list">
          ${data.duplicate_inputs.slice(0, 8).map(item => `
            <div class="atlas-inline-card">
              <div><strong>${escapeHtml(item.source_label || 'duplicate')}</strong></div>
              <div class="text-dim">canonical ${escapeHtml(item.duplicate_of_network_id || '')}</div>
            </div>
          `).join('')}
        </div>
      </section>
    `;
  }

  return html;
}

export function renderAtlasQueryResult(data) {
  const results = Array.isArray(data.results) ? data.results : [];
  const resultUnit = data.result_unit || 'slice';
  const query = data.query || {};
  const querySource = data.query_source || 'atlas';
  const sqlitePath = data.sqlite_path || '';
  const goal = query.goal || {};
  const hasGraphSpec = (query.required_regimes?.length || 0) > 0 || (query.forbidden_regimes?.length || 0) > 0 ||
    (query.required_transitions?.length || 0) > 0 || (query.forbidden_transitions?.length || 0) > 0;
  const hasPathSpec = (query.required_path_sequences?.length || 0) > 0 || query.forbid_singular_on_witness || query.max_witness_path_length != null;
  const hasPolytopeSpec = query.require_witness_feasible || query.require_witness_robust || query.min_witness_volume_mean != null;
  const hasGoal = Object.keys(goal).length > 0;

  let html = `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">Query Summary</div>
        <div class="text-dim">${data.result_count ?? 0} matches</div>
      </div>
      <div class="siso-summary-line">
        <span class="summary-chip">unit ${escapeHtml(resultUnit)}</span>
        <span class="summary-chip">ranking ${escapeHtml(query.ranking_mode || 'minimal_first')}</span>
        <span class="summary-chip">limit ${query.limit ?? '-'}</span>
        <span class="summary-chip">source ${escapeHtml(querySource)}</span>
        ${hasGoal ? '<span class="summary-chip">goal dsl</span>' : ''}
        ${query.pareto_only ? '<span class="summary-chip">pareto only</span>' : ''}
        ${hasGraphSpec ? '<span class="summary-chip">graph spec</span>' : ''}
        ${hasPathSpec ? '<span class="summary-chip">path spec</span>' : ''}
        ${hasPolytopeSpec ? '<span class="summary-chip">polytope spec</span>' : ''}
      </div>
      ${sqlitePath ? `<div class="text-dim atlas-path-inline">${escapeHtml(sqlitePath)}</div>` : ''}
      ${hasGoal ? `<div class="text-dim">goal: ${escapeHtml(JSON.stringify(goal))}</div>` : ''}
    </section>
  `;

  if (!results.length) {
    html += '<div class="text-dim">No atlas entries matched the current query. The diagnostics below show where candidates were filtered out.</div>';
    html += renderAtlasDiagnosticsSection(data);
    return html;
  }

  html += `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">Top Matches</div>
        <div class="text-dim">ranked</div>
      </div>
      <div class="family-grid">
        ${results.map((result, idx) => {
          const accent = getFamilyColor(idx + 1, 1);
          const motifLabels = resultUnit === 'network'
            ? (result.motif_union || [])
            : (result.matched_motif_buckets || []).map(bucket => bucket.family_label);
          const exactLabels = resultUnit === 'network'
            ? (result.exact_union || [])
            : (result.matched_exact_buckets || []).map(bucket => bucket.family_label);
          const witnessPath = resultUnit === 'network' ? result.best_witness_path : result.best_witness_path;
          const title = resultUnit === 'network'
            ? escapeHtml(result.source_label || result.network_id || `network_${idx + 1}`)
            : `${escapeHtml(result.source_label || result.network_id || `slice_${idx + 1}`)} <span class="text-dim">${escapeHtml(result.input_symbol || '')} -> ${escapeHtml(result.output_symbol || '')}</span>`;
          return `
            <div class="family-card" style="--family-accent:${accent}; --family-soft:${hexToRgba(accent, 0.16)};">
              <div class="family-card-header">
                <div>
                  <div class="family-kicker">Rank ${result.rank ?? idx + 1}</div>
                  <div class="family-title">${title}</div>
                  <div class="family-subtitle">${escapeHtml(resultUnit === 'network' ? `${result.matching_slice_count || 1} matching slices` : `${result.slice_id || 'slice'} in ${result.network_id || 'network'}`)}</div>
                </div>
              </div>
              <div class="family-meta">
                <span class="family-metric">d ${result.base_species_count ?? '-'}</span>
                <span class="family-metric">r ${result.reaction_count ?? '-'}</span>
                <span class="family-metric">support ${result.max_support ?? '-'}</span>
                <span class="family-metric">mass ${result.support_mass ?? '-'}</span>
                <span class="family-metric">robust ${Number(result.robustness_score || 0).toFixed(2)}</span>
              </div>
              <div class="family-meta">
                <span class="family-metric">regimes ${result.matched_regime_count ?? 0}</span>
                <span class="family-metric">transitions ${result.matched_transition_count ?? 0}</span>
                <span class="family-metric">witness ${result.witness_path_count ?? 0}</span>
              </div>
              <div>
                <div class="family-kicker">Motifs</div>
                <div class="siso-wrap-cell">${renderAtlasLabelRefs(motifLabels)}</div>
              </div>
              <div>
                <div class="family-kicker">Exact Families</div>
                <div class="siso-wrap-cell">${renderAtlasLabelRefs(exactLabels)}</div>
              </div>
              <div>
                <div class="family-kicker">Rules</div>
                ${renderAtlasRules(result.raw_rules || [])}
              </div>
              <div>
                <div class="family-kicker">Best Witness Path</div>
                ${renderWitnessPathSummary(witnessPath)}
              </div>
              ${renderAtlasResultExplain(result, resultUnit)}
            </div>
          `;
        }).join('')}
      </div>
    </section>
  `;

  html += renderAtlasDiagnosticsSection(data);

  return html;
}

export function renderAtlasInverseDesignResult(data) {
  const inverse = data?.inverse_design || {};
  const buildPlan = data?.build_plan || {};
  const queryResult = data?.query_result || {};
  const refinement = data?.refinement_result || {};
  const bestDesign = data?.best_design || null;
  const planTraces = Array.isArray(buildPlan.candidate_traces) ? buildPlan.candidate_traces : [];
  const refinementResults = Array.isArray(refinement.results) ? refinement.results : [];
  const bestCandidate = refinement.best_candidate || refinementResults[0] || null;

  let html = `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">Pipeline Summary</div>
        <div class="text-dim">${escapeHtml(data.generated_at || '')}</div>
      </div>
      <div class="siso-summary-grid">
        <div class="siso-stat-card"><div class="siso-stat-label">Build Candidates</div><div class="siso-stat-value">${buildPlan.candidate_count ?? 0}</div></div>
        <div class="siso-stat-card"><div class="siso-stat-label">Queued Builds</div><div class="siso-stat-value">${buildPlan.build_candidate_count ?? 0}</div></div>
        <div class="siso-stat-card"><div class="siso-stat-label">Query Matches</div><div class="siso-stat-value">${queryResult.result_count ?? 0}</div></div>
        <div class="siso-stat-card"><div class="siso-stat-label">Refined</div><div class="siso-stat-value">${refinement.evaluated_count ?? 0}</div></div>
      </div>
      <div class="siso-summary-line">
        ${data.build_requested ? '<span class="summary-chip">build requested</span>' : ''}
        ${data.build_performed ? '<span class="summary-chip">delta built</span>' : ''}
        ${data.merge_performed ? '<span class="summary-chip">library merged</span>' : ''}
        ${data.library_created ? '<span class="summary-chip">library created</span>' : ''}
        ${data.sqlite_path ? '<span class="summary-chip">sqlite attached</span>' : ''}
        ${data.build_source_mode ? `<span class="summary-chip">source ${escapeHtml(data.build_source_mode)}</span>` : ''}
        ${data.query_target_kind ? `<span class="summary-chip">target ${escapeHtml(data.query_target_kind)}</span>` : ''}
      </div>
      <div class="text-dim">source label: ${escapeHtml(data.source_label || inverse.source_label || 'inverse_design_run')}</div>
      ${data.sqlite_path ? `<div class="text-dim atlas-path-inline">${escapeHtml(data.sqlite_path)}</div>` : ''}
    </section>
  `;

  if (data.delta_atlas_summary || data.library_summary) {
    html += `
      <section class="siso-section">
        <div class="siso-section-head">
          <div class="siso-section-title">Atlas Reuse</div>
          <div class="text-dim">${data.library_summary ? 'library-aware' : 'delta-only'}</div>
        </div>
        <div class="siso-summary-line">
          ${data.delta_atlas_summary ? `<span class="summary-chip">delta slices ${data.delta_atlas_summary.behavior_slice_count ?? 0}</span>` : ''}
          ${data.delta_atlas_summary ? `<span class="summary-chip">delta paths ${data.delta_atlas_summary.path_record_count ?? 0}</span>` : ''}
          ${data.library_summary ? `<span class="summary-chip">library atlases ${data.library_summary.atlas_count ?? 0}</span>` : ''}
          ${data.library_summary ? `<span class="summary-chip">library slices ${data.library_summary.behavior_slice_count ?? 0}</span>` : ''}
        </div>
      </section>
    `;
  }

  if (buildPlan.candidate_count != null || planTraces.length) {
    html += `
      <section class="siso-section">
        <div class="siso-section-head">
          <div class="siso-section-title">Build Screening</div>
          <div class="text-dim">${planTraces.length} traces</div>
        </div>
        <div class="siso-summary-line">
          <span class="summary-chip">input candidates ${buildPlan.candidate_count ?? 0}</span>
          <span class="summary-chip">queued ${buildPlan.build_candidate_count ?? 0}</span>
          <span class="summary-chip">negative updates ${(buildPlan.negative_certificate_updates || []).length}</span>
          <span class="summary-chip">cache updates ${(buildPlan.support_screen_cache_updates || []).length}</span>
        </div>
        ${renderAtlasDiagnosticsSection({ candidate_traces: planTraces }, 'Support-First Build Trace')}
      </section>
    `;
  }

  if (refinement.enabled) {
    html += `
      <section class="siso-section">
        <div class="siso-section-head">
          <div class="siso-section-title">Refinement</div>
          <div class="text-dim">${refinement.reranked ? 'reranked' : 'annotated only'}</div>
        </div>
        <div class="siso-summary-line">
          <span class="summary-chip">top k ${data.refinement?.top_k ?? 0}</span>
          <span class="summary-chip">trials ${data.refinement?.trials ?? 0}</span>
          <span class="summary-chip">points ${data.refinement?.n_points ?? 0}</span>
          <span class="summary-chip">evaluated ${refinement.evaluated_count ?? 0}</span>
        </div>
        ${bestCandidate ? `
          <div class="atlas-inline-card">
            <div><strong>${escapeHtml(bestCandidate.source_label || bestCandidate.network_id || 'best candidate')}</strong></div>
            <div class="family-meta">
              <span class="family-metric">score ${Number(bestCandidate.refinement_score || 0).toFixed(3)}</span>
              <span class="family-metric">seed ${escapeHtml(bestCandidate.best_trial?.seed_source || 'n/a')}</span>
              <span class="family-metric">${escapeHtml(bestCandidate.input_symbol || bestCandidate.candidate?.input_symbol || '?')} -> ${escapeHtml(bestCandidate.output_symbol || bestCandidate.candidate?.output_symbol || '?')}</span>
            </div>
          </div>
        ` : '<div class="text-dim">No refinement candidates were evaluated.</div>'}
      </section>
    `;
  }

  if (bestDesign?.candidate) {
    const selected = bestDesign.candidate;
    html += `
      <section class="siso-section">
        <div class="siso-section-head">
          <div class="siso-section-title">Best Design</div>
          <div class="text-dim">${escapeHtml(bestDesign.selection_source || 'query')}</div>
        </div>
        <div class="atlas-inline-card">
          <div><strong>${escapeHtml(selected.source_label || selected.network_id || 'candidate')}</strong></div>
          <div class="family-meta">
            <span class="family-metric">${escapeHtml(selected.input_symbol || selected.best_input_symbol || '?')} -> ${escapeHtml(selected.output_symbol || selected.best_output_symbol || '?')}</span>
            ${selected.refinement_score != null ? `<span class="family-metric">score ${Number(selected.refinement_score).toFixed(3)}</span>` : ''}
            ${selected.result_unit ? `<span class="family-metric">${escapeHtml(selected.result_unit)}</span>` : ''}
          </div>
        </div>
      </section>
    `;
  }

  html += renderAtlasQueryResult({
    ...queryResult,
    query_source: 'inverse_design',
    sqlite_path: data.sqlite_path || '',
    policies: data.policies || {},
  });

  return html;
}

/* ------------------------------------------------------------------ */
/*  Execution                                                          */
/* ------------------------------------------------------------------ */

export function resolveAtlasExecutionContext(nodeId, queryPayload) {
  const atlas = getConnectedAtlasData(nodeId);
  const specPayload = getConnectedAtlasSpec(nodeId);
  const configuredSqlitePath = queryPayload?.sqlitePath || '';
  const persistedAtlasSqlitePath = queryPayload?.preferPersistedAtlas && atlas?.sqlite_persisted ? atlas.sqlite_path : '';
  return {
    atlas,
    specPayload,
    sqlitePath: configuredSqlitePath || persistedAtlasSqlitePath || '',
  };
}

export async function executeAtlasBuilder(nodeId) {
  const contentEl = document.getElementById(`${nodeId}-content`);
  let payload;
  try {
    payload = getConnectedAtlasSpec(nodeId);
  } catch (e) {
    if (contentEl) contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
    return;
  }

  if (!payload) {
    showToast('Connect an Atlas Spec node first');
    return;
  }

  setNodeLoading(nodeId, true);
  try {
    const data = await api('build_atlas', payload.spec);
    const info = nodeRegistry[nodeId];
    if (info) {
      info.data = info.data || {};
      info.data.atlasData = data;
      info.data.lastSpec = payload.serial;
      info.data.sqlitePath = data.sqlite_path || payload.spec.sqlite_path || '';
    }
    if (contentEl) contentEl.innerHTML = renderAtlasBuilderResult(data);
    commitWorkspaceSnapshot('atlas-built');
    triggerDownstreamNodes(nodeId, 'atlas');
  } catch (e) {
    handleNodeError(e, nodeId, 'Atlas build');
    if (contentEl) contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

export async function executeAtlasQueryResult(nodeId) {
  const contentEl = document.getElementById(`${nodeId}-content`);
  let queryPayload;
  try {
    queryPayload = getConnectedAtlasQuery(nodeId);
  } catch (e) {
    if (contentEl) contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
    return;
  }

  if (!queryPayload) {
    showToast('Connect an Atlas Query Config node first');
    return;
  }

  let executionContext;
  try {
    executionContext = resolveAtlasExecutionContext(nodeId, queryPayload);
  } catch (e) {
    if (contentEl) contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
    return;
  }

  const { atlas, sqlitePath } = executionContext;

  if (!atlas && !sqlitePath) {
    showToast('Build an atlas first, or provide a SQLite path in Atlas Query Config');
    return;
  }

  setNodeLoading(nodeId, true);
  try {
    const request = sqlitePath
      ? { sqlite_path: sqlitePath, query: queryPayload.query }
      : { atlas, query: queryPayload.query };
    const data = await api('query_atlas', request);
    const renderData = {
      ...data,
      query_source: sqlitePath ? 'sqlite' : 'atlas',
      sqlite_path: sqlitePath || '',
    };
    const info = nodeRegistry[nodeId];
    if (info) {
      info.data = info.data || {};
      info.data.queryData = renderData;
      info.data.lastQuery = queryPayload.serial;
    }
    if (contentEl) contentEl.innerHTML = renderAtlasQueryResult(renderData);
    commitWorkspaceSnapshot('atlas-query');
  } catch (e) {
    handleNodeError(e, nodeId, 'Atlas query');
    if (contentEl) contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

export async function executeAtlasInverseDesignResult(nodeId) {
  const contentEl = document.getElementById(`${nodeId}-content`);

  let queryPayload;
  try {
    queryPayload = getConnectedAtlasQuery(nodeId);
  } catch (e) {
    if (contentEl) contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
    return;
  }

  if (!queryPayload) {
    showToast('Connect an Atlas Query Config node first');
    return;
  }

  let executionContext;
  try {
    executionContext = resolveAtlasExecutionContext(nodeId, queryPayload);
  } catch (e) {
    if (contentEl) contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
    return;
  }

  const { atlas, specPayload, sqlitePath } = executionContext;
  if (!specPayload && !atlas && !sqlitePath) {
    showToast('Connect an Atlas Spec or Atlas Builder node, or provide a SQLite path in Atlas Query Config');
    return;
  }

  const request = {
    query: queryPayload.query,
    inverse_design: queryPayload.inverseDesign,
    refinement: queryPayload.refinement,
  };
  if (queryPayload.allowDuplicateAtlas) request.allow_duplicate_atlas = true;
  if (sqlitePath) request.sqlite_path = sqlitePath;
  if (specPayload) request.atlas_spec = specPayload.spec;
  else if (atlas) request.atlas = atlas;

  setNodeLoading(nodeId, true);
  try {
    const data = await api('run_inverse_design', request);
    const info = nodeRegistry[nodeId];
    if (info) {
      info.data = info.data || {};
      info.data.inverseDesignData = data;
      info.data.lastInverseRequest = {
        query: queryPayload.serial,
        spec: specPayload?.serial || null,
        sqlitePath,
      };
    }
    if (contentEl) contentEl.innerHTML = renderAtlasInverseDesignResult(data);
    commitWorkspaceSnapshot('atlas-inverse-design');
  } catch (e) {
    handleNodeError(e, nodeId, 'Atlas inverse design');
    if (contentEl) contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

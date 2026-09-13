// Biocircuits Explorer — Declarative Node Serialization Schemas
//
// Each node type can declare a `serialization` descriptor that tells the
// generic serialize / restore helpers which DOM elements to read/write and
// which runtime-data keys to clone.  Types with complex logic still use
// custom `serialize` / `restore` functions.

import { getWorkspaceRuntimeEpoch, nodeRegistry } from './state.js';
import { cloneSerializable } from './api.js';
import { INVERSE_DESIGN_DEFAULTS } from './inverse-design-core.js';
import { DEFAULT_TARGET } from './design-target-adapters.js';
import { inspectExecutionLifecycle } from './execution-lifecycle-core.js';
import {
  readCurrentModelBuildResult,
  stripSessionIdentifiers,
} from './model-lifecycle.js';

// Lazy imports for afterRestore hooks (avoids circular deps at load time)
let _updateROPCloudMode, _updateRegimeGraphMode, _updateROPPolyDimension;
let _restoreRopShapeResultView, _updateRopShapeIntentVisibility;
let _restorePlacerResultView, _restoreAtlasNodeExecution;
let _restoreInverseDesignTargetView, _restoreInverseDesignResultView, _restoreDesignedNetworkView;
async function ensureHookImports() {
  if (_updateROPCloudMode && _updateRegimeGraphMode &&
      _updateROPPolyDimension && _restoreRopShapeResultView &&
      _updateRopShapeIntentVisibility && _restorePlacerResultView &&
      _restoreAtlasNodeExecution && _restoreInverseDesignTargetView && _restoreInverseDesignResultView &&
      _restoreDesignedNetworkView) return;
  const [ropCloud, regimeGraph, scan, ropShape, placer, executionLifecycle, inverseDesign] = await Promise.all([
    import('./rop-cloud.js'),
    import('./regime-graph.js'),
    import('./scan.js'),
    import('./node-types/rop-shape.js'),
    import('./node-types/placer.js'),
    import('./execution-lifecycle.js'),
    import('./node-types/inverse-design.js'),
  ]);
  _updateROPCloudMode = ropCloud.updateROPCloudMode;
  _updateRegimeGraphMode = regimeGraph.updateRegimeGraphMode;
  _updateROPPolyDimension = scan.updateROPPolyDimension;
  _restoreRopShapeResultView = ropShape.restoreRopShapeResultView;
  _updateRopShapeIntentVisibility = ropShape.updateRopShapeIntentVisibility;
  _restorePlacerResultView = placer.restorePlacerResultView;
  _restoreAtlasNodeExecution = executionLifecycle.restoreAtlasNodeExecution;
  _restoreInverseDesignTargetView = inverseDesign.restoreInverseDesignTargetView;
  _restoreInverseDesignResultView = inverseDesign.restoreInverseDesignResultView;
  _restoreDesignedNetworkView = inverseDesign.restoreDesignedNetworkView;
}
// Pre-load hooks at module init (non-blocking)
ensureHookImports();

function restoreInverseViewWhenReady(nodeId, data, resolveHook) {
  const hook = resolveHook();
  if (hook) return hook(nodeId, data);
  const owner = nodeRegistry[nodeId];
  const epoch = getWorkspaceRuntimeEpoch();
  const lifecycle = owner?._inverseDesignLifecycle;
  const runtime = lifecycle ? inspectExecutionLifecycle(lifecycle) : null;
  // A deferred module load must not restore into a replaced workspace or
  // overwrite a run that started while the restore hook was loading.
  ensureHookImports().then(() => {
    if (nodeRegistry[nodeId] !== owner || getWorkspaceRuntimeEpoch() !== epoch ||
        owner?._inverseDesignLifecycle !== lifecycle) return;
    const current = lifecycle ? inspectExecutionLifecycle(lifecycle) : null;
    if (current?.revision !== runtime?.revision || current?.state !== runtime?.state) return;
    resolveHook()?.(nodeId, data);
  });
}

// ===== Field type readers / writers ================================

const READERS = {
  string:   (el, def) => el?.value || def || '',
  int:      (el, def) => parseInt(el?.value || def || '0', 10),
  float:    (el, def) => parseFloat(el?.value || def || '0'),
  bool:     (el, def) => el?.checked ?? def ?? false,
  expr:     (el)      => (el?.value || '').trim(),
};

function readField(nodeId, fd) {
  const el = document.getElementById(`${nodeId}${fd.suffix}`);
  return READERS[fd.type](el, fd.default);
}

function writeField(nodeId, fd, value) {
  if (value == null) return;
  const el = document.getElementById(`${nodeId}${fd.suffix}`);
  if (!el) return;
  if (fd.type === 'bool') {
    el.checked = value;
    return;
  }

  if (el instanceof HTMLSelectElement) {
    const stringValue = String(value);
    el.dataset.pendingValue = stringValue;
    if (Array.from(el.options).some(option => option.value === stringValue)) {
      el.value = stringValue;
      delete el.dataset.pendingValue;
    }
    return;
  }

  el.value = value;
}

// ===== Generic serialize / restore =================================

export function serializeBySchema(nodeId, schema) {
  const result = {};

  // 1. Read DOM fields
  for (const [key, fd] of Object.entries(schema.fields || {})) {
    if (fd.serializeAs) {
      // Custom key mapping (e.g. DOM value "param" serialized as "param_symbol")
      result[fd.serializeAs] = readField(nodeId, fd);
    } else {
      result[key] = readField(nodeId, fd);
    }
  }

  // 2. Handle special expr→array pattern
  for (const [key, fd] of Object.entries(schema.fields || {})) {
    if (fd.type === 'expr' && fd.arrayKey) {
      const val = result[key];
      delete result[key];
      result[fd.arrayKey] = val ? [val] : [];
    }
  }

  // 3. Clone runtime data keys
  const info = nodeRegistry[nodeId]?.data || {};
  for (const dk of (schema.data || [])) {
    result[dk] = cloneSerializable(info[dk]);
  }

  // 4. Include data keys without cloning (for primitives / booleans)
  for (const dk of (schema.dataRaw || [])) {
    result[dk] = info[dk] ?? null;
  }

  // 5. Include config if requested
  if (schema.includeConfig) {
    result.config = cloneSerializable(info.config);
  }

  return result;
}

export function restoreBySchema(nodeId, schema, data) {
  // 1. Restore DOM fields
  for (const [key, fd] of Object.entries(schema.fields || {})) {
    const dataKey = fd.serializeAs || key;

    // Handle array→expr reverse mapping
    if (fd.type === 'expr' && fd.arrayKey) {
      const arr = data[fd.arrayKey];
      if (arr && arr[0]) writeField(nodeId, fd, arr[0]);
      continue;
    }

    writeField(nodeId, fd, data[dataKey]);
  }

  // 2. Restore config if needed
  if (schema.includeConfig && data.config && nodeRegistry[nodeId]) {
    nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
    nodeRegistry[nodeId].data.config = data.config;
  }

  // 3. Restore targetSpecies to data (ROP cloud pattern)
  for (const dk of (schema.restoreToData || [])) {
    if (data[dk] != null && nodeRegistry[nodeId]) {
      nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
      nodeRegistry[nodeId].data[dk] = data[dk];
    }
  }

  // 4. Call afterRestore hook if defined
  if (schema.afterRestore) {
    schema.afterRestore(nodeId, data);
  }
}

// ===== Node Serialization Schemas ==================================

// Field groups shared by the new params nodes and their legacy merged
// counterparts, whose DOM ids are identical by design.
const SCAN_1D_FIELDS = {
  param_symbol:  { suffix: '-param',  type: 'string' },
  param_min:     { suffix: '-min',    type: 'float', default: '-6' },
  param_max:     { suffix: '-max',    type: 'float', default: '6' },
  n_points:      { suffix: '-points', type: 'int',   default: '200' },
  _expr:         { suffix: '-expr',   type: 'expr',  arrayKey: 'output_exprs' },
};

const ROP_CLOUD_FIELDS = {
  mode:          { suffix: '-sampling-mode',  type: 'string', default: 'x_space' },
  samples:       { suffix: '-samples',        type: 'int',    default: '10000' },
  span:          { suffix: '-span',           type: 'int',    default: '6' },
  logxMin:       { suffix: '-logx-min',       type: 'float',  default: '-6' },
  logxMax:       { suffix: '-logx-max',       type: 'float',  default: '6' },
  targetSpecies: { suffix: '-target-species',  type: 'string' },
};

function restoreROPCloudMode(nodeId) {
  _updateROPCloudMode?.(nodeId);
}

const ROP_POLY_FIELDS = {
  dimension:        { suffix: '-dimension',        type: 'int',   default: '2' },
  add_inner_points: { suffix: '-add-inner-points', type: 'bool',  default: true },
  npoints:          { suffix: '-npoints',          type: 'int',   default: '5000' },
  singular_extends: { suffix: '-singular-extends', type: 'float', default: '2' },
};

function serializeROPPolyPairs(nodeId, result) {
  const dim = result.dimension || 2;
  const axisCount = dim === 3 ? 3 : 2;
  result.pairs = [];
  for (let i = 1; i <= axisCount; i++) {
    result.pairs.push({
      x_symbol:  document.getElementById(`${nodeId}-x${i}`)?.value || '',
      qk_symbol: document.getElementById(`${nodeId}-qk${i}`)?.value || '',
    });
  }
}

function restoreROPPolyPairs(nodeId, data) {
  (data.pairs || []).forEach((pair, idx) => {
    const axis = idx + 1;
    const xEl = document.getElementById(`${nodeId}-x${axis}`);
    const qkEl = document.getElementById(`${nodeId}-qk${axis}`);
    if (xEl && pair.x_symbol) xEl.value = pair.x_symbol;
    if (qkEl && pair.qk_symbol) qkEl.value = pair.qk_symbol;
  });
  _updateROPPolyDimension?.(nodeId);
}

export const NODE_SCHEMAS = {
  'siso-params': {
    fields: {
      changeQK:          { suffix: '-siso-select',   type: 'string' },
      observeX:          { suffix: '-target-x',      type: 'string' },
      pathScope:         { suffix: '-path-scope',    type: 'string', default: 'feasible' },
      minVolumeMean:     { suffix: '-min-volume',    type: 'float',  default: '0' },
      keepSingular:      { suffix: '-keep-singular', type: 'bool',   default: true },
      keepNonasymptotic: { suffix: '-keep-nonasym',  type: 'bool',   default: false },
      min:               { suffix: '-min',           type: 'float',  default: '-6' },
      max:               { suffix: '-max',           type: 'float',  default: '6' },
    },
    includeConfig: true,
  },

  'scan-1d-params': {
    data: ['fixedParameterOverrides'],
    restoreToData: ['fixedParameterOverrides'],
    fields: SCAN_1D_FIELDS,
  },
  'placer-params': {
    fields: {
      input_sym:  { suffix: '-input',  type: 'string' },
      output_sym: { suffix: '-output', type: 'string' },
      target_ro:  { suffix: '-target', type: 'float', default: '1' },
      kd_lo:      { suffix: '-kdlo',   type: 'float', default: '-3' },
      kd_hi:      { suffix: '-kdhi',   type: 'float', default: '3' },
    },
  },

  'placer-result': {
    data: ['placerResult', 'placerMenu'],
    restoreToData: ['placerResult', 'placerMenu'],
    afterRestore(nodeId, data) {
      if (_restorePlacerResultView) {
        _restorePlacerResultView(nodeId, data);
        return;
      }
      ensureHookImports().then(() => _restorePlacerResultView?.(nodeId, data));
    },
  },

  'design-spec-config': {
    fields: {
      target_kind:      { suffix: '-spec-kind',            type: 'string', default: 'sign' },
      target:           { suffix: '-spec-target',          type: 'string' },
      behavior_input:   { suffix: '-spec-input',           type: 'string' },
      behavior_output:  { suffix: '-spec-output',          type: 'string' },
      kd_lo:            { suffix: '-spec-kd-lo',           type: 'float',  default: '-3' },
      kd_hi:            { suffix: '-spec-kd-hi',           type: 'float',  default: '3' },
      total_lo:         { suffix: '-spec-total-lo',        type: 'float',  default: '-3' },
      total_hi:         { suffix: '-spec-total-hi',        type: 'float',  default: '3' },
      min_radius:       { suffix: '-spec-radius',          type: 'float',  default: '0' },
      min_volume:       { suffix: '-spec-volume',          type: 'string' },
      window_lo:        { suffix: '-spec-window-lo',       type: 'string' },
      window_hi:        { suffix: '-spec-window-hi',       type: 'string' },
      operating_points: { suffix: '-spec-operating-points', type: 'string' },
      output_feature:   { suffix: '-spec-output-feature',  type: 'string' },
      output_value:     { suffix: '-spec-output-value',    type: 'string' },
      output_samples:   { suffix: '-spec-output-samples',  type: 'string' },
      output_tolerance: { suffix: '-spec-output-tolerance', type: 'string' },
      shape:            { suffix: '-spec-shape',           type: 'string' },
      shape_monotonicity: { suffix: '-spec-shape-monotonicity', type: 'string', default: 'any' },
      shape_prominence: { suffix: '-spec-shape-prominence', type: 'string' },
      shape_samples:    { suffix: '-spec-shape-samples',   type: 'string' },
      shape_tolerance:  { suffix: '-spec-shape-tolerance', type: 'string' },
      dynamic_range:    { suffix: '-spec-dynamic-range',   type: 'string' },
      dynamic_samples:  { suffix: '-spec-dynamic-samples', type: 'string' },
      transition_spacing: { suffix: '-spec-transition-spacing', type: 'string' },
      transition_order: { suffix: '-spec-transition-order', type: 'string' },
      rank_primary:     { suffix: '-spec-rank-primary',    type: 'string' },
      rank_secondary:   { suffix: '-spec-rank-secondary',  type: 'string' },
      max_species:      { suffix: '-spec-max-species',     type: 'string' },
      max_reactions:    { suffix: '-spec-max-reactions',   type: 'string' },
      max_mu:           { suffix: '-spec-max-mu',          type: 'string' },
      allow_near_minimal: { suffix: '-spec-allow-near-minimal', type: 'bool', default: true },
      max_exact:        { suffix: '-spec-max-exact',       type: 'int',    default: '3' },
      max_extra_species:   { suffix: '-spec-extra-species',   type: 'int', default: '1' },
      max_extra_reactions: { suffix: '-spec-extra-reactions', type: 'int', default: '1' },
      max_extra_mu:        { suffix: '-spec-extra-mu',        type: 'int', default: '1' },
      block_hard:       { suffix: '-spec-block-hard',      type: 'bool',   default: true },
      spec_json:        { suffix: '-spec-json',            type: 'string' },
    },
    includeConfig: true,
  },

  'inverse-design-target': {
    fields: {
      inverseDescription: { suffix: '-description', type: 'string', default: DEFAULT_TARGET.description },
      inverseTargetMode: { suffix: '-target-mode', type: 'string', default: 'curve' },
      inverseTargetJSON: { suffix: '-target-json', type: 'string', default: JSON.stringify(DEFAULT_TARGET) },
      inverseDrawingJSON: { suffix: '-drawing-json', type: 'string', default: '' },
      inverseTargetPoints: { suffix: '-target-points', type: 'int', default: 24 },
      inverseImageResolution: { suffix: '-image-resolution', type: 'int', default: 12 },
      inverseImageInvert: { suffix: '-image-invert', type: 'string', default: 'false' },
      inverseAuxiliaryMonomers: { suffix: '-aux-monomers', type: 'int', default: INVERSE_DESIGN_DEFAULTS.chemistry.auxiliary_monomers },
      inverseMaxComplexSize: { suffix: '-max-complex-size', type: 'int', default: INVERSE_DESIGN_DEFAULTS.chemistry.max_complex_size },
      inverseMaxReactions: { suffix: '-max-reactions', type: 'int', default: INVERSE_DESIGN_DEFAULTS.chemistry.max_reactions },
      inverseAllowHomomers: { suffix: '-allow-homomers', type: 'string', default: 'true' },
      inverseChemistryJSON: { suffix: '-chemistry-json', type: 'string', default: '{}' },
      inverseReferenceState: { suffix: '-reference-state', type: 'string', default: '' },
    },
    data: ['inverseDesignRequest'],
    restoreToData: ['inverseDesignRequest'],
    afterRestore(nodeId, data) {
      restoreInverseViewWhenReady(nodeId, data, () => _restoreInverseDesignTargetView);
    },
  },

  'gradient-design': {
    fields: {
      inverseLearningRate: { suffix: '-learning-rate', type: 'float', default: INVERSE_DESIGN_DEFAULTS.optimization.learning_rate },
      inverseEpochs: { suffix: '-epochs', type: 'int', default: INVERSE_DESIGN_DEFAULTS.optimization.epochs },
      inverseRestarts: { suffix: '-restarts', type: 'int', default: INVERSE_DESIGN_DEFAULTS.optimization.restarts },
      inversePruneRounds: { suffix: '-prune-rounds', type: 'int', default: INVERSE_DESIGN_DEFAULTS.optimization.prune_rounds },
      inversePruneFraction: { suffix: '-prune-fraction', type: 'float', default: INVERSE_DESIGN_DEFAULTS.optimization.prune_fraction },
      inversePruneTolerance: { suffix: '-prune-tolerance', type: 'float', default: INVERSE_DESIGN_DEFAULTS.optimization.prune_tolerance },
      inverseMaxRMSE: { suffix: '-max-rmse', type: 'float', default: INVERSE_DESIGN_DEFAULTS.optimization.max_rmse },
      inverseOptimizeTotals: { suffix: '-optimize-totals', type: 'string', default: 'true' },
      inverseSeed: { suffix: '-seed', type: 'int', default: INVERSE_DESIGN_DEFAULTS.optimization.seed },
    },
    data: ['inverseDesignRequest', 'inverseDesignResult', 'inverseDesignDrawing'],
    restoreToData: ['inverseDesignRequest', 'inverseDesignResult', 'inverseDesignDrawing'],
    afterRestore(nodeId, data) {
      restoreInverseViewWhenReady(nodeId, data, () => _restoreInverseDesignResultView);
    },
  },

  'designed-network': {
    data: ['designedNetwork'],
    restoreToData: ['designedNetwork'],
    afterRestore(nodeId, data) {
      restoreInverseViewWhenReady(nodeId, data, () => _restoreDesignedNetworkView);
    },
  },

  'rop-shape-edit-config': {
    fields: {
      ropShapeKind:            { suffix: '-rop-shape-kind',            type: 'string', default: 'broaden' },
      intentId:                { suffix: '-intent-id',                 type: 'string', default: 'shape-edit' },
      leftSpan:                { suffix: '-left-span',                 type: 'string', default: '0, 2' },
      rightSpan:               { suffix: '-right-span',                type: 'string', default: '4, 6' },
      steps:                   { suffix: '-steps',                     type: 'string', default: '1, 5' },
      group:                   { suffix: '-group',                     type: 'string', default: '4, 5, 6' },
      preserve:                { suffix: '-preserve',                  type: 'string', default: '0, 1, 2, 3' },
      anchorStep:              { suffix: '-anchor-step',               type: 'int',    default: 3 },
      anchorTolerance:         { suffix: '-anchor-tolerance',          type: 'float',  default: 0.2 },
      midpointTolerance:       { suffix: '-midpoint-tolerance',        type: 'float',  default: 0.2 },
      effectTolerance:         { suffix: '-effect-tolerance',          type: 'float',  default: 0.02 },
      minimumParameterMargin:  { suffix: '-minimum-parameter-margin',  type: 'float',  default: 0.01 },
      sense:                   { suffix: '-sense',                     type: 'string', default: 'positive' },
      shared:                  { suffix: '-shared',                    type: 'bool',   default: true },
      maxPaths:                { suffix: '-max-paths',                 type: 'int',    default: 2000 },
      maxCells:                { suffix: '-max-cells',                 type: 'int',    default: 256 },
      maxReplays:              { suffix: '-max-replays',               type: 'int',    default: 1 },
      requireExhaustive:       { suffix: '-require-exhaustive',        type: 'bool',   default: true },
      replaySamplePoints:      { suffix: '-replay-sample-points',      type: 'int',    default: 281 },
      replayMinProminence:     { suffix: '-replay-min-prominence',     type: 'float',  default: 0.5 },
      linearIntentJson:        { suffix: '-linear-intent-json',        type: 'string' },
    },
    data: ['ropShapeRequest'],
    restoreToData: ['ropShapeRequest'],
    afterRestore(nodeId) {
      if (_updateRopShapeIntentVisibility) {
        _updateRopShapeIntentVisibility(nodeId);
        return;
      }
      ensureHookImports().then(() => _updateRopShapeIntentVisibility?.(nodeId));
    },
  },

  'scan-2d-params': {
    data: ['fixedParameterOverrides'],
    restoreToData: ['fixedParameterOverrides'],
    fields: {
      param1_symbol: { suffix: '-param1', type: 'string' },
      param2_symbol: { suffix: '-param2', type: 'string' },
      param1_min:    { suffix: '-min1',   type: 'float', default: '-6' },
      param1_max:    { suffix: '-max1',   type: 'float', default: '6' },
      param2_min:    { suffix: '-min2',   type: 'float', default: '-6' },
      param2_max:    { suffix: '-max2',   type: 'float', default: '6' },
      n_grid:        { suffix: '-points', type: 'int',   default: '50' },
      output_expr:   { suffix: '-expr',   type: 'expr' },
    },
  },

  'rop-cloud-params': {
    fields: ROP_CLOUD_FIELDS,
    includeConfig: true,
    restoreToData: ['targetSpecies'],
    afterRestore: restoreROPCloudMode,
  },

  'fret-params': {
    fields: {
      grid: { suffix: '-grid', type: 'int',   default: '80' },
      min:  { suffix: '-min',  type: 'float', default: '-6' },
      max:  { suffix: '-max',  type: 'float', default: '6' },
    },
    includeConfig: true,
  },

  'rop-poly-params': {
    fields: ROP_POLY_FIELDS,
    includeConfig: true,
    // pairs handled via custom serialize/restore hooks
    customSerialize: serializeROPPolyPairs,
    customRestore: restoreROPPolyPairs,
  },

  // Legacy combined nodes (params + viewer in one node)
  'siso-analysis': {
    fields: {
      changeQK: { suffix: '-siso-select', type: 'string' },
    },
    data: ['behaviorData', 'trajectoryData', 'overlayTrajectoryData'],
    dataRaw: ['selectedPath', 'sisoPlotMode'],
  },

  'parameter-scan-1d': {
    fields: SCAN_1D_FIELDS,
    data: ['scan1DResult', 'scan1DResultMeta'],
  },

  'parameter-scan-2d': {
    fields: {
      param1_symbol: { suffix: '-param1', type: 'string' },
      param2_symbol: { suffix: '-param2', type: 'string' },
      param1_min:    { suffix: '-min1',   type: 'float', default: '-6' },
      param1_max:    { suffix: '-max1',   type: 'float', default: '6' },
      param2_min:    { suffix: '-min2',   type: 'float', default: '-6' },
      param2_max:    { suffix: '-max2',   type: 'float', default: '6' },
      n_grid:        { suffix: '-grid',   type: 'int',   default: '80' },
      output_expr:   { suffix: '-expr',   type: 'expr' },
    },
    data: ['scan2DResult', 'scan2DResultMeta'],
  },

  'rop-cloud': {
    fields: ROP_CLOUD_FIELDS,
    data: ['ropCloudData', 'ropCloudRanges'],
    dataRaw: ['ropCloudPreset'],
    restoreToData: ['targetSpecies'],
    afterRestore: restoreROPCloudMode,
  },

  'fret-heatmap': {
    fields: {
      grid: { suffix: '-grid', type: 'int', default: '80' },
    },
    data: ['fretHeatmapData'],
  },

  'rop-polyhedron': {
    fields: ROP_POLY_FIELDS,
    includeConfig: true,
    data: ['ropPlotData'],
    dataRaw: ['fitInnerPoints'],
    customSerialize: serializeROPPolyPairs,
    customRestore: restoreROPPolyPairs,
  },

  'regime-graph': {
    fields: {
      graphMode: { suffix: '-graph-mode', type: 'string', default: 'qk' },
      changeQK:  { suffix: '-change-qk',  type: 'string' },
    },
    includeConfig: true,
    data: ['graphData'],
    customSerialize(nodeId, result) {
      result.viewMode = '3d';
    },
    afterRestore(nodeId, data) {
      const info = nodeRegistry[nodeId];
      if (info) {
        info.data = info.data || {};
        info.data.config = data.config || data;
      }
      _updateRegimeGraphMode?.(nodeId);
    },
  },

  'markdown-note': {
    fields: {
      markdown: { suffix: '-markdown', type: 'string' },
    },
    afterRestore(nodeId, data) {
      const info = nodeRegistry[nodeId];
      if (info) {
        info.data = info.data || {};
        info.data.markdown = data.markdown || '';
      }
    },
  },

  // Data-only result nodes
  'model-builder': {
    data: ['modelContext'],
    dataRaw: ['built'],
    customSerialize(nodeId, result) {
      const current = readCurrentModelBuildResult(nodeId);
      result.modelContext = stripSessionIdentifiers(current || result.modelContext);
      result.built = !!current;
    },
  },

  'siso-result': {
    dataRaw: ['selectedPath', 'sisoPlotMode'],
    data: ['behaviorData', 'trajectoryData', 'overlayTrajectoryData'],
  },

  'qk-poly-result': {
    data: ['selection', 'polyhedronPayload'],
  },

  'scan-1d-result': {
    data: ['scan1DResult', 'scan1DResultMeta'],
  },

  'rop-cloud-result': {
    data: ['ropCloudData', 'ropCloudRanges'],
    dataRaw: ['ropCloudPreset'],
  },

  'fret-result': {
    data: ['fretHeatmapData'],
  },

  'scan-2d-result': {
    data: ['scan2DResult', 'scan2DResultMeta'],
  },

  'rop-poly-result': {
    data: ['ropPlotData'],
    dataRaw: ['fitInnerPoints'],
  },

  'rop-shape-result': {
    data: ['ropShapeRequest', 'ropShapeResult'],
    restoreToData: ['ropShapeRequest', 'ropShapeResult'],
    afterRestore(nodeId, data) {
      if (_restoreRopShapeResultView) {
        _restoreRopShapeResultView(nodeId, data);
        return;
      }
      ensureHookImports().then(() => _restoreRopShapeResultView?.(nodeId, data));
    },
  },

  'atlas-builder': {
    data: ['atlasData', 'lastSpec'],
    dataRaw: ['sqlitePath'],
    afterRestore(nodeId, data) {
      if (_restoreAtlasNodeExecution) {
        _restoreAtlasNodeExecution(nodeId, data);
        return;
      }
      ensureHookImports().then(() => _restoreAtlasNodeExecution?.(nodeId, data));
    },
  },

  'atlas-query-result': {
    data: ['queryData', 'lastQuery'],
    afterRestore(nodeId, data) {
      if (_restoreAtlasNodeExecution) {
        _restoreAtlasNodeExecution(nodeId, data);
        return;
      }
      ensureHookImports().then(() => _restoreAtlasNodeExecution?.(nodeId, data));
    },
  },

  'atlas-inverse-result': {
    data: ['inverseDesignData', 'lastInverseRequest'],
    afterRestore(nodeId, data) {
      if (_restoreAtlasNodeExecution) {
        _restoreAtlasNodeExecution(nodeId, data);
        return;
      }
      ensureHookImports().then(() => _restoreAtlasNodeExecution?.(nodeId, data));
    },
  },
};

// ===== Full serialize with schema + custom fallback ================

export function serializeNodeBySchema(nodeId, type) {
  const schema = NODE_SCHEMAS[type];
  if (!schema) return {};

  const result = serializeBySchema(nodeId, schema);
  if (schema.customSerialize) schema.customSerialize(nodeId, result);
  return result;
}

export function restoreNodeBySchema(nodeId, type, data) {
  const schema = NODE_SCHEMAS[type];
  if (!schema) return false;

  restoreBySchema(nodeId, schema, data);
  if (schema.customRestore) schema.customRestore(nodeId, data);
  return true;
}

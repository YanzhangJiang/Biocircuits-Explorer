// Biocircuits Explorer — Declarative Node Serialization Schemas
//
// Each node type can declare a `serialization` descriptor that tells the
// generic serialize / restore helpers which DOM elements to read/write and
// which runtime-data keys to clone.  Types with complex logic still use
// custom `serialize` / `restore` functions.

import { nodeRegistry } from './state.js';
import { cloneSerializable } from './api.js';

// Lazy imports for afterRestore hooks (avoids circular deps at load time)
let _updateROPCloudMode, _updateRegimeGraphMode, _updateROPPolyDimension;
async function ensureHookImports() {
  if (_updateROPCloudMode) return;
  const [ropCloud, regimeGraph, scan] = await Promise.all([
    import('./rop-cloud.js'),
    import('./regime-graph.js'),
    import('./scan.js'),
  ]);
  _updateROPCloudMode = ropCloud.updateROPCloudMode;
  _updateRegimeGraphMode = regimeGraph.updateRegimeGraphMode;
  _updateROPPolyDimension = scan.updateROPPolyDimension;
}
// Pre-load hooks at module init (non-blocking)
ensureHookImports();

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
    fields: {
      param_symbol:  { suffix: '-param',  type: 'string' },
      param_min:     { suffix: '-min',    type: 'float', default: '-6' },
      param_max:     { suffix: '-max',    type: 'float', default: '6' },
      n_points:      { suffix: '-points', type: 'int',   default: '200' },
      _expr:         { suffix: '-expr',   type: 'expr',  arrayKey: 'output_exprs' },
    },
  },

  'scan-2d-params': {
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
    fields: {
      mode:          { suffix: '-sampling-mode',  type: 'string', default: 'x_space' },
      samples:       { suffix: '-samples',        type: 'int',    default: '10000' },
      span:          { suffix: '-span',           type: 'int',    default: '6' },
      logxMin:       { suffix: '-logx-min',       type: 'float',  default: '-6' },
      logxMax:       { suffix: '-logx-max',       type: 'float',  default: '6' },
      targetSpecies: { suffix: '-target-species',  type: 'string' },
    },
    includeConfig: true,
    restoreToData: ['targetSpecies'],
    afterRestore(nodeId) { _updateROPCloudMode?.(nodeId); },
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
    fields: {
      dimension:        { suffix: '-dimension',        type: 'int',   default: '2' },
      add_inner_points: { suffix: '-add-inner-points', type: 'bool',  default: true },
      npoints:          { suffix: '-npoints',          type: 'int',   default: '5000' },
      singular_extends: { suffix: '-singular-extends', type: 'float', default: '2' },
    },
    includeConfig: true,
    // pairs handled via custom serialize/restore hooks
    customSerialize(nodeId, result) {
      const dim = result.dimension || 2;
      const axisCount = dim === 3 ? 3 : 2;
      result.pairs = [];
      for (let i = 1; i <= axisCount; i++) {
        result.pairs.push({
          x_symbol:  document.getElementById(`${nodeId}-x${i}`)?.value || '',
          qk_symbol: document.getElementById(`${nodeId}-qk${i}`)?.value || '',
        });
      }
    },
    customRestore(nodeId, data) {
      (data.pairs || []).forEach((pair, idx) => {
        const axis = idx + 1;
        const xEl = document.getElementById(`${nodeId}-x${axis}`);
        const qkEl = document.getElementById(`${nodeId}-qk${axis}`);
        if (xEl && pair.x_symbol) xEl.value = pair.x_symbol;
        if (qkEl && pair.qk_symbol) qkEl.value = pair.qk_symbol;
      });
      _updateROPPolyDimension?.(nodeId);
    },
  },

  // Legacy combined nodes (params + viewer in one node)
  'parameter-scan-1d': {
    fields: {
      param_symbol: { suffix: '-param',  type: 'string' },
      param_min:    { suffix: '-min',    type: 'float', default: '-6' },
      param_max:    { suffix: '-max',    type: 'float', default: '6' },
      n_points:     { suffix: '-points', type: 'int',   default: '200' },
      _expr:        { suffix: '-expr',   type: 'expr',  arrayKey: 'output_exprs' },
    },
    data: ['scan1DResult'],
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
    data: ['scan2DResult'],
  },

  'rop-cloud': {
    fields: {
      mode:          { suffix: '-sampling-mode',  type: 'string', default: 'x_space' },
      samples:       { suffix: '-samples',        type: 'int',    default: '10000' },
      span:          { suffix: '-span',           type: 'int',    default: '6' },
      logxMin:       { suffix: '-logx-min',       type: 'float',  default: '-6' },
      logxMax:       { suffix: '-logx-max',       type: 'float',  default: '6' },
      targetSpecies: { suffix: '-target-species',  type: 'string' },
    },
    data: ['ropCloudData', 'ropCloudRanges'],
    dataRaw: ['ropCloudPreset'],
    restoreToData: ['targetSpecies'],
    afterRestore(nodeId) { _updateROPCloudMode?.(nodeId); },
  },

  'fret-heatmap': {
    fields: {
      grid: { suffix: '-grid', type: 'int', default: '80' },
    },
    data: ['fretHeatmapData'],
  },

  'rop-polyhedron': {
    fields: {
      dimension:        { suffix: '-dimension',        type: 'int',   default: '2' },
      add_inner_points: { suffix: '-add-inner-points', type: 'bool',  default: true },
      npoints:          { suffix: '-npoints',          type: 'int',   default: '5000' },
      singular_extends: { suffix: '-singular-extends', type: 'float', default: '2' },
    },
    includeConfig: true,
    data: ['ropPlotData'],
    dataRaw: ['fitInnerPoints'],
    customSerialize(nodeId, result) {
      const dim = result.dimension || 2;
      const axisCount = dim === 3 ? 3 : 2;
      result.pairs = [];
      for (let i = 1; i <= axisCount; i++) {
        result.pairs.push({
          x_symbol:  document.getElementById(`${nodeId}-x${i}`)?.value || '',
          qk_symbol: document.getElementById(`${nodeId}-qk${i}`)?.value || '',
        });
      }
    },
    customRestore(nodeId, data) {
      (data.pairs || []).forEach((pair, idx) => {
        const axis = idx + 1;
        const xEl = document.getElementById(`${nodeId}-x${axis}`);
        const qkEl = document.getElementById(`${nodeId}-qk${axis}`);
        if (xEl && pair.x_symbol) xEl.value = pair.x_symbol;
        if (qkEl && pair.qk_symbol) qkEl.value = pair.qk_symbol;
      });
      _updateROPPolyDimension?.(nodeId);
    },
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
      result.built = !!result.built;
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
    data: ['scan1DResult'],
  },

  'rop-cloud-result': {
    data: ['ropCloudData', 'ropCloudRanges'],
    dataRaw: ['ropCloudPreset'],
  },

  'fret-result': {
    data: ['fretHeatmapData'],
  },

  'scan-2d-result': {
    data: ['scan2DResult'],
  },

  'rop-poly-result': {
    data: ['ropPlotData'],
    dataRaw: ['fitInnerPoints'],
  },

  'atlas-builder': {
    data: ['atlasData', 'lastSpec'],
    dataRaw: ['sqlitePath'],
  },

  'atlas-query-result': {
    data: ['queryData', 'lastQuery'],
  },

  'atlas-inverse-result': {
    data: ['inverseDesignData', 'lastInverseRequest'],
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

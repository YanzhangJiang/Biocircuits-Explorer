import { NOTE_TYPES, switchNoteTab } from './note.js';
import { INPUT_TYPES } from './input.js';
import { PROCESS_TYPES } from './process.js';
import { SISO_TYPES } from './siso.js';
import { SCAN_TYPES } from './scan.js';
import { ROP_CLOUD_TYPES } from './rop-cloud.js';
import { ROP_POLY_TYPES } from './rop-poly.js';
import { ATLAS_TYPES } from './atlas.js';
import { RESULT_TYPES } from './result.js';

export const NODE_TYPES = {
  ...NOTE_TYPES,
  ...INPUT_TYPES,
  ...PROCESS_TYPES,
  ...SISO_TYPES,
  ...SCAN_TYPES,
  ...ROP_CLOUD_TYPES,
  ...ROP_POLY_TYPES,
  ...ATLAS_TYPES,
  ...RESULT_TYPES,
};

// Required predecessor chain for each node type
export const PREREQ_CHAIN = {
  'model-builder': ['reaction-network'],
  'model-summary': ['reaction-network', 'model-builder'],
  'vertices-table': ['reaction-network', 'model-builder'],
  'regime-graph': ['reaction-network', 'model-builder'],
  'siso-analysis': ['reaction-network', 'model-builder'],
  'siso-params': ['reaction-network', 'model-builder'],
  'siso-result': ['siso-params'],
  'qk-poly-result': ['siso-result'],
  'rop-cloud': ['reaction-network'],
  'fret-heatmap': ['reaction-network', 'model-builder'],
  'parameter-scan-1d': ['reaction-network', 'model-builder'],
  'parameter-scan-2d': ['reaction-network', 'model-builder'],
  'rop-polyhedron': ['model-builder'],
  'scan-1d-params': ['reaction-network', 'model-builder'],
  'scan-1d-result': ['scan-1d-params'],
  'rop-cloud-params': ['reaction-network'],
  'rop-cloud-result': ['rop-cloud-params'],
  'fret-params': ['reaction-network', 'model-builder'],
  'fret-result': ['fret-params'],
  'scan-2d-params': ['reaction-network', 'model-builder'],
  'scan-2d-result': ['scan-2d-params'],
  'rop-poly-params': ['model-builder'],
  'rop-poly-result': ['rop-poly-params'],
  'atlas-spec': [],
  'atlas-builder': ['atlas-spec'],
  'atlas-query-config': [],
  'atlas-query-result': ['atlas-builder', 'atlas-query-config'],
  'atlas-inverse-result': ['atlas-spec', 'atlas-query-config'],
};

export { switchNoteTab };

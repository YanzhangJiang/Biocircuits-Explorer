import { NOTE_TYPES, switchNoteTab } from './note.js';
import { INPUT_TYPES } from './input.js';
import { PROCESS_TYPES } from './process.js';
import { SISO_TYPES } from './siso.js';
import { SCAN_TYPES } from './scan.js';
import { ROP_CLOUD_TYPES } from './rop-cloud.js';
import { ROP_POLY_TYPES } from './rop-poly.js';
import { ATLAS_TYPES } from './atlas.js';
import { RESULT_TYPES } from './result.js';
import { SBML_TYPES } from './sbml.js';

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
  ...SBML_TYPES,
};

// Prerequisite/connection validity is derived from the typed port graph
// (see port-types.js); the former hand-maintained PREREQ_CHAIN table that
// shadowed it by node type was dead code and has been removed.

export { switchNoteTab };

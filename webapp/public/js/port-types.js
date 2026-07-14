// Biocircuits Explorer — Typed Port Graph
//
// Every socket carries a *type* drawn from a small vocabulary aligned with the
// backend artifact families (bne-ir / bne-design / atlas / result). Connections
// are validated by type compatibility rather than by raw port-id string match,
// so the canvas behaves like a dataflow editor whose wires have a declared
// contract — and downstream auto-execution keys off the same types.
//
// Historically the port id *was* the type (one id ⇒ one meaning), so the map
// below is a bijection and preserves existing connection semantics exactly. The
// indirection is what lets types later be merged, aliased, or sub-typed in one
// place instead of at every call site, and it is the single source of truth
// (replacing the dead PREREQ_CHAIN table that used to shadow it by node type).

export const PORT_TYPES = Object.freeze({
  NetworkIR:     'NetworkIR',      // a reaction network (feeds build_model)
  ModelArtifact: 'ModelArtifact',  // a compiled model / session
  ParamsConfig:  'ParamsConfig',   // an analysis config (scan / siso / fret / rop)
  PathResult:    'PathResult',     // a selected SISO path handed downstream
  AtlasSpec:     'AtlasSpec',      // an atlas build spec
  AtlasArtifact: 'AtlasArtifact',  // a built atlas / preview library
  AtlasQuery:    'AtlasQuery',     // an atlas query / inverse-design config
  AtlasNetwork:  'AtlasNetwork',   // a network feeding an atlas spec
  DesignabilitySpec: 'DesignabilitySpec', // an explicit behavior/constraint target
  ROPShapeReferenceArtifact: 'ROPShapeReferenceArtifact', // pinned fixed-topology reference
  ROPShapeRequestArtifact:   'ROPShapeRequestArtifact',   // canonical typed edit request
  ROPShapeResultArtifact:    'ROPShapeResultArtifact',    // optimizer result + replay evidence
});

// port id → type: the single source of truth for what flows on each socket.
export const PORT_TYPE_OF = Object.freeze({
  reactions:       PORT_TYPES.NetworkIR,
  model:           PORT_TYPES.ModelArtifact,
  params:          PORT_TYPES.ParamsConfig,
  result:          PORT_TYPES.PathResult,
  'atlas-spec':    PORT_TYPES.AtlasSpec,
  atlas:           PORT_TYPES.AtlasArtifact,
  'atlas-query':   PORT_TYPES.AtlasQuery,
  'atlas-network': PORT_TYPES.AtlasNetwork,
  'designability-spec': PORT_TYPES.DesignabilitySpec,
  'rop-shape-reference': PORT_TYPES.ROPShapeReferenceArtifact,
  'rop-shape-request':   PORT_TYPES.ROPShapeRequestArtifact,
  'rop-shape-result':    PORT_TYPES.ROPShapeResultArtifact,
});

// Resolve a socket's type. A node may override per-port with a `type` field on
// its input/output declaration; otherwise the central map applies, falling back
// to the raw id so an unmapped port still behaves (connects only to its own id).
export function portTypeOf(portId, declaredType = null) {
  return declaredType || PORT_TYPE_OF[portId] || portId;
}

// Compatibility aliases beyond strict equality: a directed map of
// output-type → Set(accepted input-types). Empty today (every wire connects
// like-to-like); this is the seam for widening (e.g. a supertype accepting
// subtypes) without changing call sites.
const COMPAT = Object.freeze({});

export function portTypesCompatible(outputType, inputType) {
  if (outputType === inputType) return true;
  const accepted = COMPAT[outputType];
  return !!accepted && accepted.has(inputType);
}

// Convenience for connection code that holds raw port ids.
export function portsCompatible(fromPortId, toPortId) {
  return portTypesCompatible(portTypeOf(fromPortId), portTypeOf(toPortId));
}

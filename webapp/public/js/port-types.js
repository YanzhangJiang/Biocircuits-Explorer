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
  SISOConfig:    'SISOConfig',
  Scan1DConfig:  'Scan1DConfig',
  Scan2DConfig:  'Scan2DConfig',
  ROPCloudConfig: 'ROPCloudConfig',
  FRETConfig:    'FRETConfig',
  ROPPolyhedronConfig: 'ROPPolyhedronConfig',
  ParameterPlacerConfig: 'ParameterPlacerConfig',
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

const KNOWN_PORT_TYPES = new Set(Object.values(PORT_TYPES));

// Resolve a socket's type. Shared ids such as `params` must declare their exact
// family on the node definition. Unknown ids and misspelled declarations are
// rejected instead of becoming accidental self-compatible types.
export function portTypeOf(portId, declaredType = null) {
  const resolved = declaredType || PORT_TYPE_OF[portId] || null;
  return KNOWN_PORT_TYPES.has(resolved) ? resolved : null;
}

// The one resolver used by interactive wiring, restore, paste, Undo/Redo, and
// graph transactions. Its complete identity is node type + direction + port;
// a raw port id is not a sufficient artifact contract.
export function resolveNodePort(nodeTypes, nodeType, direction, port) {
  const declarationKey = direction === 'input'
    ? 'inputs'
    : direction === 'output'
      ? 'outputs'
      : null;
  if (!declarationKey || typeof nodeType !== 'string' || typeof port !== 'string') return null;
  const typeDef = nodeTypes?.[nodeType];
  const declarations = typeDef?.[declarationKey];
  if (!Array.isArray(declarations)) return null;
  const declaration = declarations.find(entry => entry?.port === port);
  if (!declaration) return null;
  if (typeof declaration.type !== 'string' || !declaration.type) return null;
  const type = portTypeOf(port, declaration.type);
  if (!type) return null;
  return { nodeType, direction, port, type };
}

// Compatibility aliases beyond strict equality: a directed map of
// output-type → Set(accepted input-types). Empty today (every wire connects
// like-to-like); this is the seam for widening (e.g. a supertype accepting
// subtypes) without changing call sites.
const COMPAT = Object.freeze({});

export function portTypesCompatible(outputType, inputType) {
  if (!KNOWN_PORT_TYPES.has(outputType) || !KNOWN_PORT_TYPES.has(inputType)) return false;
  if (outputType === inputType) return true;
  const accepted = COMPAT[outputType];
  return !!accepted && accepted.has(inputType);
}

// Convenience for connection code that holds raw port ids.
export function portsCompatible(fromPortId, toPortId, fromDeclaredType = null, toDeclaredType = null) {
  return portTypesCompatible(
    portTypeOf(fromPortId, fromDeclaredType),
    portTypeOf(toPortId, toDeclaredType),
  );
}

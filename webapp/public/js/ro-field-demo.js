import {
  renderROFieldArtifact,
  RO_FIELD_SCHEMA_VERSION,
} from './ro-field-render.js';

export const RO_FIELD_REQUEST_VERSION = 'bne-ro-field-request/v1.0.0';
export const RO_FIELD_ENDPOINT = '/api/v1/ro_field';

// Kept in sync with tests/fixtures/ro_field_request/sampled-inline-network.json.
// The browser intentionally sends an inline, nine-point demonstration request;
// it does not ask the server for persistence or an exhaustive Atlas build.
export const DEFAULT_RO_FIELD_REQUEST = {
  schema_version: RO_FIELD_REQUEST_VERSION,
  network: {
    ir_schema_version: 'bne-ir/v1.0.0',
    label: 'heterodimer-request-fixture',
    species: [
      { name: 'A', role: 'free' },
      { name: 'B', role: 'free' },
      { name: 'AB', role: 'complex' },
    ],
    reactions: [
      { formula: 'A + B <-> AB', kd: 1, kind: 'binding', reversible: true },
    ],
    observables: [],
    parameter_distributions: [],
    compartments: [],
    provenance: { source: 'contract-fixture' },
    extensions: {},
  },
  representation: 'sampled_grid',
  domain: {
    domain_kind: 'axis_aligned_log_box',
    coordinate_space: 'dimensionless_log_ratio',
    log_basis: 'log10',
    axis_order: ['input_a', 'input_b'],
    axes: [
      {
        axis_id: 'input_a',
        symbol: 'tA',
        coordinate_kind: 'conserved_total',
        orientation: 'increasing_physical_value',
        reference: { value: 1, unit: 'uM' },
        bounds: { lower: -1, upper: 1 },
      },
      {
        axis_id: 'input_b',
        symbol: 'tB',
        coordinate_kind: 'conserved_total',
        orientation: 'increasing_physical_value',
        reference: { value: 1, unit: 'uM' },
        bounds: { lower: -1, upper: 1 },
      },
    ],
    fixed_background: [
      {
        parameter_id: 'kd_ab',
        symbol: 'Kd1',
        coordinate_kind: 'binding_constant',
        reference: { value: 1, unit: 'uM' },
        log_value: 0,
      },
    ],
  },
  outputs: {
    output_order: ['output_ab'],
    items: [
      {
        output_id: 'output_ab',
        symbol: 'AB',
        observable_kind: 'species_concentration',
        reference: { value: 1, unit: 'uM' },
      },
    ],
  },
  sampling: {
    scheme: 'cartesian_product',
    axis_coordinates: [[-1, 0, 1], [-1, 0, 1]],
  },
  work_budget: {
    work_unit_kind: 'solver_samples',
    max_evaluated_items: 9,
    max_stored_items: 9,
    max_payload_bytes: 1_048_576,
    deadline_seconds: 10,
  },
  storage: { mode: 'inline' },
};

function objectValue(value, label) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
  return value;
}

export function extractROFieldResponse(payload) {
  const response = objectValue(payload, 'response');
  const field = objectValue(response.ro_field, 'response.ro_field');
  if (field.schema_version !== RO_FIELD_SCHEMA_VERSION) {
    throw new Error(
      `response.ro_field.schema_version must be exactly ${RO_FIELD_SCHEMA_VERSION}`,
    );
  }
  return field;
}

export async function fetchROField(request, options = {}) {
  const fetchImpl = options.fetchImpl ?? globalThis.fetch;
  if (typeof fetchImpl !== 'function') throw new Error('fetch is unavailable');
  objectValue(request, 'request');
  if (request.schema_version !== RO_FIELD_REQUEST_VERSION) {
    throw new Error(`request.schema_version must be exactly ${RO_FIELD_REQUEST_VERSION}`);
  }
  if (request.storage?.mode !== 'inline') {
    throw new Error('this demo only sends storage.mode="inline"');
  }
  const response = await fetchImpl(RO_FIELD_ENDPOINT, {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(request),
  });
  let payload;
  try {
    payload = await response.json();
  } catch {
    throw new Error(`server returned non-JSON data (HTTP ${response.status})`);
  }
  if (!response.ok) {
    const detail = payload?.error ?? payload?.message ?? `HTTP ${response.status}`;
    throw new Error(String(detail));
  }
  return { payload, roField: extractROFieldResponse(payload) };
}

function initializeDemo(documentRef) {
  const form = documentRef.getElementById('ro-field-form');
  const requestInput = documentRef.getElementById('ro-field-request');
  const submitButton = documentRef.getElementById('ro-field-submit');
  const status = documentRef.getElementById('ro-field-status');
  const result = documentRef.getElementById('ro-field-result');
  if (!form || !requestInput || !submitButton || !status || !result) return;

  requestInput.value = JSON.stringify(DEFAULT_RO_FIELD_REQUEST, null, 2);
  form.addEventListener('submit', async event => {
    event.preventDefault();
    submitButton.disabled = true;
    status.dataset.state = 'running';
    status.textContent = 'Computing the bounded inline demonstration…';
    result.replaceChildren();
    try {
      const request = JSON.parse(requestInput.value);
      const { roField } = await fetchROField(request);
      renderROFieldArtifact(result, roField);
      status.dataset.state = roField.partial ? 'partial' : 'complete';
      status.textContent = roField.partial
        ? 'Partial result rendered. Gaps and unknown regions remain explicit.'
        : 'Bounded demonstration result rendered.';
    } catch (error) {
      status.dataset.state = 'error';
      status.textContent = `Unable to render: ${String(error?.message ?? error)}`;
    } finally {
      submitButton.disabled = false;
    }
  });
}

if (typeof document !== 'undefined') {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => initializeDemo(document), { once: true });
  } else {
    initializeDemo(document);
  }
}

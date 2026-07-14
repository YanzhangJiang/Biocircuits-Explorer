import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { portsCompatible, portTypeOf, PORT_TYPES } from '../public/js/port-types.js';

const testDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(testDir, '..', '..');

globalThis.window = {
  matchMedia: () => null,
  addEventListener() {},
  location: { protocol: 'http:', hostname: '127.0.0.1', port: '8000' },
};
globalThis.document = {
  readyState: 'loading',
  documentElement: { dataset: {}, style: { setProperty() {} } },
  getElementById() { return null; },
  addEventListener() {},
  querySelectorAll() { return []; },
};

const { NODE_TYPES } = await import('../public/js/node-types/index.js');
const { NODE_SCHEMAS, serializeNodeBySchema, restoreNodeBySchema } = await import('../public/js/node-schema.js');
const { readDesignSpecConfig, validateDesignSpecConfig } = await import('../public/js/node-types/design-target.js');
const { cardsWithAgentSpec, normalizeAgentDesignabilitySpec } = await import('../public/js/agent-view.js');

let passed = 0;
const pendingTests = [];
function test(name, fn) {
  const done = () => {
    passed += 1;
    console.log(`  ok - ${name}`);
  };
  const result = fn();
  if (result && typeof result.then === 'function') {
    pendingTests.push(result.then(done));
  } else {
    done();
  }
}

test('Design Spec Config emits the DesignabilitySpec artifact consumed by Design Target', () => {
  const specNode = NODE_TYPES['design-spec-config'];
  const targetNode = NODE_TYPES['design-target'];
  assert.ok(specNode, 'design-spec-config node must be registered');
  assert.ok(targetNode, 'design-target node must be registered');
  assert.deepEqual(specNode.outputs.map(p => p.port), ['designability-spec']);
  assert.deepEqual(targetNode.inputs.map(p => p.port), ['designability-spec']);
  assert.equal(portTypeOf('designability-spec'), PORT_TYPES.DesignabilitySpec);
  assert.ok(portsCompatible('designability-spec', 'designability-spec'));
});

test('Design Target stays a reaction source and emits a separate pinned ROP shape reference', () => {
  const targetNode = NODE_TYPES['design-target'];
  assert.deepEqual(targetNode.outputs.map(p => p.port), ['reactions', 'rop-shape-reference']);
  assert.equal(targetNode.inputs.some(p => p.port === 'model'), false);
  assert.equal(targetNode.outputs.some(p => p.port === 'params'), false);
});

test('web and native add-node menus expose Design Spec Config', () => {
  const webMenu = fs.readFileSync(path.join(repoRoot, 'webapp/public/index-node.html'), 'utf8');
  const nativeMenu = fs.readFileSync(path.join(repoRoot, 'frontend-swift/BiocircuitsExplorerMac/ContentView.swift'), 'utf8');
  assert.match(webMenu, /data-type="design-spec-config"/);
  assert.match(nativeMenu, /NodeMenuItem\(id: "design-spec-config"/);
});

test('web and native add-node menus expose the ROP shape config/result pair', () => {
  const webMenu = fs.readFileSync(path.join(repoRoot, 'webapp/public/index-node.html'), 'utf8');
  const nativeMenu = fs.readFileSync(path.join(repoRoot, 'frontend-swift/BiocircuitsExplorerMac/ContentView.swift'), 'utf8');
  for (const type of ['rop-shape-edit-config', 'rop-shape-result']) {
    assert.match(webMenu, new RegExp(`data-type="${type}"`));
    assert.match(nativeMenu, new RegExp(`NodeMenuItem\\(id: "${type}"`));
  }
});

test('Design Target UI no longer uses wizard or proxy-recommendation wording', () => {
  const html = NODE_TYPES['design-target'].createBody('node-test');
  assert.doesNotMatch(html.toLowerCase(), /wizard/);
  assert.doesNotMatch(html, /Recommended for tuning/);
  assert.match(html, /Verified recommendations require/);
});

test('Design Spec Config can hand-author a structured behavior spec', () => {
  const html = NODE_TYPES['design-spec-config'].createBody('node-test');
  assert.match(html, /value="behavior_spec"/);
  assert.match(html, /id="node-test-spec-input"/);
  assert.match(html, /id="node-test-spec-output"/);
  assert.match(html, /id="node-test-spec-window-lo"/);
  assert.match(html, /id="node-test-spec-output-feature"/);

  const priorGetElementById = globalThis.document.getElementById;
  const field = (value = '', checked = false) => ({ value, checked });
  const fields = {
    'node-test-spec-kind': field('behavior_spec'),
    'node-test-spec-target': field('1, 0'),
    'node-test-spec-input': field('tA'),
    'node-test-spec-output': field('C_A_A'),
    'node-test-spec-kd-lo': field('-3'),
    'node-test-spec-kd-hi': field('3'),
    'node-test-spec-total-lo': field('-2'),
    'node-test-spec-total-hi': field('2'),
    'node-test-spec-radius': field('0.25'),
    'node-test-spec-volume': field('0.5'),
    'node-test-spec-window-lo': field('-1'),
    'node-test-spec-window-hi': field('1'),
    'node-test-spec-output-feature': field('threshold'),
    'node-test-spec-output-value': field('0.5'),
    'node-test-spec-output-samples': field('151'),
    'node-test-spec-output-tolerance': field('0.02'),
    'node-test-spec-shape': field('bell_shaped'),
    'node-test-spec-shape-monotonicity': field('any'),
    'node-test-spec-shape-prominence': field('0.4'),
    'node-test-spec-shape-samples': field('201'),
    'node-test-spec-shape-tolerance': field('0.03'),
    'node-test-spec-dynamic-range': field('4'),
    'node-test-spec-dynamic-samples': field('101'),
    'node-test-spec-transition-spacing': field('0.3'),
    'node-test-spec-max-species': field('3'),
    'node-test-spec-max-reactions': field('4'),
    'node-test-spec-max-mu': field('5'),
    'node-test-spec-allow-near-minimal': field('', false),
    'node-test-spec-max-exact': field('5'),
    'node-test-spec-extra-species': field('1'),
    'node-test-spec-extra-reactions': field('1'),
    'node-test-spec-extra-mu': field('0'),
    'node-test-spec-block-hard': field('', true),
    'node-test-spec-json': field(''),
  };
  globalThis.document.getElementById = id => fields[id] || null;
  try {
    const spec = readDesignSpecConfig('node-test');
    assert.equal(spec.source.kind, 'manual_config');
    assert.equal(spec.target.behavior_spec.input, 'tA');
    assert.equal(spec.target.behavior_spec.output, 'C_A_A');
    assert.deepEqual(spec.target.behavior_spec.program.map(step => step.value), [1, 0]);
    assert.deepEqual(spec.constraints.parameter_bounds.kd_log10, [-3, 3]);
    assert.deepEqual(spec.constraints.parameter_bounds.total_log10, [-2, 2]);
    assert.equal(spec.constraints.robustness.min_chebyshev_radius, 0.25);
    assert.equal(spec.constraints.robustness.min_tunable_volume_lower_bound, 0.5);
    assert.equal(spec.constraints.robustness.min_tunable_volume, undefined);
    assert.deepEqual(spec.target.behavior_spec.input_window.input_log10, [-1, 1]);
    assert.equal(spec.target.behavior_spec.input_window.min_spacing_decades, undefined);
    assert.equal(spec.target.input_window, undefined);
    assert.equal(spec.target.output_feature.feature, 'threshold');
    assert.equal(spec.target.output_feature.value, 0.5);
    assert.equal(spec.target.output_feature.sample_points, 151);
    assert.equal(spec.target.output_feature.tolerance_log10, 0.02);
    assert.equal(spec.target.shape.class, 'bell_shaped');
    assert.equal(spec.target.shape.min_prominence_log10, 0.4);
    assert.equal(spec.target.shape.sample_points, 201);
    assert.equal(spec.target.shape.tolerance_log10, 0.03);
    assert.equal(spec.constraints.dynamic_range.min_fold_change, 4);
    assert.equal(spec.constraints.dynamic_range.sample_points, 101);
    assert.equal(spec.constraints.transitions.min_spacing_decades, 0.3);
    assert.deepEqual(spec.constraints.network, {
      max_species: 3,
      max_reactions: 4,
      max_mu: 5,
      allow_near_minimal: false,
    });
    assert.equal(spec.candidate_budget.max_exact_placements, 5);
    assert.equal(spec.audit_policy.unsupported, 'block_if_hard');
  } finally {
    globalThis.document.getElementById = priorGetElementById;
  }
});

test('Design Spec Config requires explicit sampled verifier controls', () => {
  const priorGetElementById = globalThis.document.getElementById;
  const field = (value = '', checked = false) => ({ value, checked });
  const baseFields = () => ({
    'node-test-spec-kind': field('behavior_spec'),
    'node-test-spec-target': field('1'),
    'node-test-spec-input': field('tA'),
    'node-test-spec-output': field('C_A_A'),
    'node-test-spec-kd-lo': field('-3'),
    'node-test-spec-kd-hi': field('3'),
    'node-test-spec-total-lo': field('-3'),
    'node-test-spec-total-hi': field('3'),
    'node-test-spec-radius': field('0'),
    'node-test-spec-volume': field(''),
    'node-test-spec-window-lo': field('-2'),
    'node-test-spec-window-hi': field('2'),
    'node-test-spec-operating-points': field(''),
    'node-test-spec-output-feature': field(''),
    'node-test-spec-output-value': field(''),
    'node-test-spec-output-samples': field(''),
    'node-test-spec-output-tolerance': field(''),
    'node-test-spec-shape': field(''),
    'node-test-spec-shape-monotonicity': field('any'),
    'node-test-spec-shape-prominence': field(''),
    'node-test-spec-shape-samples': field(''),
    'node-test-spec-shape-tolerance': field(''),
    'node-test-spec-dynamic-range': field(''),
    'node-test-spec-dynamic-samples': field(''),
    'node-test-spec-transition-spacing': field(''),
    'node-test-spec-transition-order': field(''),
    'node-test-spec-max-species': field(''),
    'node-test-spec-max-reactions': field(''),
    'node-test-spec-max-mu': field(''),
    'node-test-spec-allow-near-minimal': field('', true),
    'node-test-spec-max-exact': field('3'),
    'node-test-spec-extra-species': field('1'),
    'node-test-spec-extra-reactions': field('1'),
    'node-test-spec-extra-mu': field('1'),
    'node-test-spec-block-hard': field('', true),
    'node-test-spec-json': field(''),
  });
  const cases = [
    {
      patch: {
        'node-test-spec-output-feature': field('threshold'),
        'node-test-spec-output-value': field('0.5'),
        'node-test-spec-output-tolerance': field('0.01'),
      },
      message: /output feature.*samples/i,
    },
    {
      patch: {
        'node-test-spec-output-feature': field('threshold'),
        'node-test-spec-output-value': field('0.5'),
        'node-test-spec-output-samples': field('81'),
      },
      message: /output feature.*tolerance/i,
    },
    {
      patch: {
        'node-test-spec-shape': field('monotonic'),
        'node-test-spec-shape-monotonicity': field('decreasing'),
        'node-test-spec-shape-tolerance': field('0.01'),
      },
      message: /shape.*samples/i,
    },
    {
      patch: {
        'node-test-spec-shape': field('monotonic'),
        'node-test-spec-shape-monotonicity': field('decreasing'),
        'node-test-spec-shape-samples': field('81'),
      },
      message: /shape.*tolerance/i,
    },
    {
      patch: {
        'node-test-spec-dynamic-range': field('10'),
      },
      message: /dynamic range.*samples/i,
    },
  ];

  try {
    for (const { patch, message } of cases) {
      const fields = { ...baseFields(), ...patch };
      globalThis.document.getElementById = id => fields[id] || null;
      assert.throws(() => readDesignSpecConfig('node-test'), message);
    }
  } finally {
    globalThis.document.getElementById = priorGetElementById;
  }
});

test('Design Spec Config partial JSON override normalizes tunable volume alias without dropping robustness controls', () => {
  const priorGetElementById = globalThis.document.getElementById;
  const field = (value = '', checked = false) => ({ value, checked });
  const fields = {
    'node-test-spec-kind': field('behavior_spec'),
    'node-test-spec-target': field('1, 0'),
    'node-test-spec-input': field('tA'),
    'node-test-spec-output': field('C_A_A'),
    'node-test-spec-kd-lo': field('-3'),
    'node-test-spec-kd-hi': field('3'),
    'node-test-spec-total-lo': field('-2'),
    'node-test-spec-total-hi': field('2'),
    'node-test-spec-radius': field('0.25'),
    'node-test-spec-volume': field('0.5'),
    'node-test-spec-window-lo': field(''),
    'node-test-spec-window-hi': field(''),
    'node-test-spec-operating-points': field(''),
    'node-test-spec-output-feature': field(''),
    'node-test-spec-output-value': field(''),
    'node-test-spec-output-samples': field(''),
    'node-test-spec-output-tolerance': field(''),
    'node-test-spec-shape': field(''),
    'node-test-spec-shape-monotonicity': field('any'),
    'node-test-spec-shape-prominence': field(''),
    'node-test-spec-shape-samples': field(''),
    'node-test-spec-shape-tolerance': field(''),
    'node-test-spec-dynamic-range': field(''),
    'node-test-spec-dynamic-samples': field(''),
    'node-test-spec-transition-spacing': field(''),
    'node-test-spec-transition-order': field(''),
    'node-test-spec-max-species': field(''),
    'node-test-spec-max-reactions': field(''),
    'node-test-spec-max-mu': field(''),
    'node-test-spec-allow-near-minimal': field('', true),
    'node-test-spec-max-exact': field('3'),
    'node-test-spec-extra-species': field('1'),
    'node-test-spec-extra-reactions': field('1'),
    'node-test-spec-extra-mu': field('1'),
    'node-test-spec-block-hard': field('', true),
    'node-test-spec-json': field(JSON.stringify({
      constraints: {
        robustness: {
          min_chebyshev_radius: 0.75,
          min_tunable_volume: 0.9,
        },
      },
    })),
  };
  globalThis.document.getElementById = id => fields[id] || null;
  try {
    const spec = readDesignSpecConfig('node-test');
    assert.equal(spec.constraints.robustness.min_chebyshev_radius, 0.75);
    assert.equal(spec.constraints.robustness.min_tunable_volume_lower_bound, 0.9);
    assert.equal(spec.constraints.robustness.min_tunable_volume, undefined);
    assert.deepEqual(spec.constraints.parameter_bounds.kd_log10, [-3, 3]);
    assert.deepEqual(spec.constraints.parameter_bounds.total_log10, [-2, 2]);
  } finally {
    globalThis.document.getElementById = priorGetElementById;
  }
});

test('Design Spec Config serialization preserves dynamic sample count', () => {
  const schema = NODE_SCHEMAS['design-spec-config'];
  assert.ok(schema.fields.dynamic_samples);
  assert.equal(schema.fields.dynamic_samples.suffix, '-spec-dynamic-samples');
  assert.equal(schema.fields.dynamic_samples.type, 'string');
  assert.equal(schema.fields.output_samples.type, 'string');
  assert.equal(schema.fields.shape_samples.type, 'string');
  assert.equal(schema.fields.min_volume.type, 'string');
  assert.equal(schema.fields.operating_points.type, 'string');
  assert.equal(schema.fields.transition_order.type, 'string');
  assert.equal(schema.fields.rank_primary.type, 'string');
  assert.equal(schema.fields.rank_secondary.type, 'string');
  assert.equal(schema.fields.max_species.type, 'string');
  assert.equal(schema.fields.max_reactions.type, 'string');
  assert.equal(schema.fields.max_mu.type, 'string');
});

test('Design Spec Config schema round-trips advanced spec controls', () => {
  class FakeSelect {
    constructor(value = '', options = []) {
      this.value = value;
      this.checked = false;
      this.dataset = {};
      this.options = options.map(option => ({ value: option }));
    }
  }
  const priorGetElementById = globalThis.document.getElementById;
  const priorHTMLSelectElement = globalThis.HTMLSelectElement;
  const field = value => ({ value, checked: false, dataset: {} });
  const selectOptions = ['', 'transition_spacing', 'dynamic_range', 'complexity'];
  const sourceElements = {
    'node-test-spec-operating-points': field('-2, 2'),
    'node-test-spec-transition-order': field('0, 1'),
    'node-test-spec-rank-primary': new FakeSelect('transition_spacing', selectOptions),
    'node-test-spec-rank-secondary': new FakeSelect('dynamic_range', selectOptions),
  };
  const restoredElements = {
    'node-restored-spec-operating-points': field(''),
    'node-restored-spec-transition-order': field(''),
    'node-restored-spec-rank-primary': new FakeSelect('', selectOptions),
    'node-restored-spec-rank-secondary': new FakeSelect('', selectOptions),
  };
  try {
    globalThis.HTMLSelectElement = FakeSelect;
    globalThis.document.getElementById = id => sourceElements[id] || null;
    const snapshot = serializeNodeBySchema('node-test', 'design-spec-config');
    assert.equal(snapshot.operating_points, '-2, 2');
    assert.equal(snapshot.transition_order, '0, 1');
    assert.equal(snapshot.rank_primary, 'transition_spacing');
    assert.equal(snapshot.rank_secondary, 'dynamic_range');

    globalThis.document.getElementById = id => restoredElements[id] || null;
    assert.equal(restoreNodeBySchema('node-restored', 'design-spec-config', snapshot), true);
    assert.equal(restoredElements['node-restored-spec-operating-points'].value, '-2, 2');
    assert.equal(restoredElements['node-restored-spec-transition-order'].value, '0, 1');
    assert.equal(restoredElements['node-restored-spec-rank-primary'].value, 'transition_spacing');
    assert.equal(restoredElements['node-restored-spec-rank-secondary'].value, 'dynamic_range');
  } finally {
    globalThis.document.getElementById = priorGetElementById;
    globalThis.HTMLSelectElement = priorHTMLSelectElement;
  }
});

test('Design Spec Config can hand-author monotonic shape parameters', () => {
  const html = NODE_TYPES['design-spec-config'].createBody('node-test');
  assert.match(html, /id="node-test-spec-shape-monotonicity"/);
  assert.match(html, /id="node-test-spec-shape-prominence"/);
  assert.match(html, /id="node-test-spec-shape-samples"/);

  const priorGetElementById = globalThis.document.getElementById;
  const field = (value = '', checked = false) => ({ value, checked });
  const fields = {
    'node-test-spec-kind': field('behavior_spec'),
    'node-test-spec-target': field('1, 0'),
    'node-test-spec-input': field('tA'),
    'node-test-spec-output': field('B'),
    'node-test-spec-kd-lo': field('-3'),
    'node-test-spec-kd-hi': field('3'),
    'node-test-spec-total-lo': field('-3'),
    'node-test-spec-total-hi': field('3'),
    'node-test-spec-radius': field('0'),
    'node-test-spec-volume': field(''),
    'node-test-spec-window-lo': field('-2'),
    'node-test-spec-window-hi': field('2'),
    'node-test-spec-output-feature': field(''),
    'node-test-spec-output-value': field(''),
    'node-test-spec-output-samples': field(''),
    'node-test-spec-output-tolerance': field(''),
    'node-test-spec-shape': field('monotonic'),
    'node-test-spec-shape-monotonicity': field('decreasing'),
    'node-test-spec-shape-prominence': field(''),
    'node-test-spec-shape-samples': field('301'),
    'node-test-spec-shape-tolerance': field('0.05'),
    'node-test-spec-dynamic-range': field(''),
    'node-test-spec-dynamic-samples': field(''),
    'node-test-spec-transition-spacing': field(''),
    'node-test-spec-max-species': field(''),
    'node-test-spec-max-reactions': field(''),
    'node-test-spec-max-mu': field(''),
    'node-test-spec-allow-near-minimal': field('', true),
    'node-test-spec-max-exact': field('2'),
    'node-test-spec-extra-species': field('1'),
    'node-test-spec-extra-reactions': field('1'),
    'node-test-spec-extra-mu': field('1'),
    'node-test-spec-block-hard': field('', true),
    'node-test-spec-json': field(''),
  };
  globalThis.document.getElementById = id => fields[id] || null;
  try {
    const spec = readDesignSpecConfig('node-test');
    assert.deepEqual(spec.target.shape, {
      class: 'monotonic',
      monotonicity: 'decreasing',
      sample_points: 301,
      tolerance_log10: 0.05,
      hard: true,
    });
  } finally {
    globalThis.document.getElementById = priorGetElementById;
  }
});

test('Design Spec Config can hand-author operating points, transition order, and ranking preference', () => {
  const html = NODE_TYPES['design-spec-config'].createBody('node-test');
  assert.match(html, /id="node-test-spec-operating-points"/);
  assert.match(html, /id="node-test-spec-transition-order"/);
  assert.match(html, /id="node-test-spec-rank-primary"/);
  assert.match(html, /id="node-test-spec-rank-secondary"/);

  const priorGetElementById = globalThis.document.getElementById;
  const field = (value = '', checked = false) => ({ value, checked });
  const fields = {
    'node-test-spec-kind': field('behavior_spec'),
    'node-test-spec-target': field('0, -1'),
    'node-test-spec-input': field('tA'),
    'node-test-spec-output': field('B'),
    'node-test-spec-kd-lo': field('-3'),
    'node-test-spec-kd-hi': field('3'),
    'node-test-spec-total-lo': field('-3'),
    'node-test-spec-total-hi': field('3'),
    'node-test-spec-radius': field('0'),
    'node-test-spec-volume': field(''),
    'node-test-spec-window-lo': field('-6'),
    'node-test-spec-window-hi': field('6'),
    'node-test-spec-operating-points': field('-2, 2'),
    'node-test-spec-output-feature': field(''),
    'node-test-spec-output-value': field(''),
    'node-test-spec-output-samples': field(''),
    'node-test-spec-output-tolerance': field(''),
    'node-test-spec-shape': field(''),
    'node-test-spec-shape-monotonicity': field('any'),
    'node-test-spec-shape-prominence': field(''),
    'node-test-spec-shape-samples': field(''),
    'node-test-spec-shape-tolerance': field(''),
    'node-test-spec-dynamic-range': field('2'),
    'node-test-spec-dynamic-samples': field('51'),
    'node-test-spec-transition-spacing': field('0.75'),
    'node-test-spec-transition-order': field('0, 1'),
    'node-test-spec-rank-primary': field('transition_spacing'),
    'node-test-spec-rank-secondary': field('dynamic_range'),
    'node-test-spec-max-species': field(''),
    'node-test-spec-max-reactions': field(''),
    'node-test-spec-max-mu': field(''),
    'node-test-spec-allow-near-minimal': field('', true),
    'node-test-spec-max-exact': field('3'),
    'node-test-spec-extra-species': field('1'),
    'node-test-spec-extra-reactions': field('1'),
    'node-test-spec-extra-mu': field('1'),
    'node-test-spec-block-hard': field('', true),
    'node-test-spec-json': field(''),
  };
  globalThis.document.getElementById = id => fields[id] || null;
  try {
    const spec = readDesignSpecConfig('node-test');
    assert.deepEqual(spec.target.behavior_spec.input_window.operating_points_log10, [-2, 2]);
    assert.deepEqual(spec.constraints.transitions.order, [0, 1]);
    assert.equal(spec.constraints.transitions.min_spacing_decades, 0.75);
    assert.equal(spec.constraints.dynamic_range.min_fold_change, 2);
    assert.equal(spec.constraints.dynamic_range.sample_points, 51);
    assert.deepEqual(spec.ranking_policy.prefer, ['transition_spacing', 'dynamic_range']);
  } finally {
    globalThis.document.getElementById = priorGetElementById;
  }
});

test('Design Spec Config soft toggle uses clause hard flags, not unsupported audit modes', () => {
  const priorGetElementById = globalThis.document.getElementById;
  const field = (value = '', checked = false) => ({ value, checked });
  const fields = {
    'node-test-spec-kind': field('behavior_spec'),
    'node-test-spec-target': field('1, 0'),
    'node-test-spec-input': field('tA'),
    'node-test-spec-output': field('B'),
    'node-test-spec-kd-lo': field('-3'),
    'node-test-spec-kd-hi': field('3'),
    'node-test-spec-total-lo': field('-3'),
    'node-test-spec-total-hi': field('3'),
    'node-test-spec-radius': field('0'),
    'node-test-spec-volume': field(''),
    'node-test-spec-window-lo': field('-2'),
    'node-test-spec-window-hi': field('2'),
    'node-test-spec-output-feature': field('threshold'),
    'node-test-spec-output-value': field('0.5'),
    'node-test-spec-output-samples': field('81'),
    'node-test-spec-output-tolerance': field('0.01'),
    'node-test-spec-shape': field(''),
    'node-test-spec-shape-monotonicity': field('any'),
    'node-test-spec-shape-prominence': field(''),
    'node-test-spec-shape-samples': field(''),
    'node-test-spec-shape-tolerance': field(''),
    'node-test-spec-dynamic-range': field(''),
    'node-test-spec-dynamic-samples': field(''),
    'node-test-spec-transition-spacing': field(''),
    'node-test-spec-max-species': field(''),
    'node-test-spec-max-reactions': field(''),
    'node-test-spec-max-mu': field(''),
    'node-test-spec-allow-near-minimal': field('', true),
    'node-test-spec-max-exact': field('2'),
    'node-test-spec-extra-species': field('1'),
    'node-test-spec-extra-reactions': field('1'),
    'node-test-spec-extra-mu': field('1'),
    'node-test-spec-block-hard': field('', false),
    'node-test-spec-json': field(''),
  };
  globalThis.document.getElementById = id => fields[id] || null;
  try {
    const spec = readDesignSpecConfig('node-test');
    assert.equal(spec.audit_policy.unsupported, 'block_if_hard');
    assert.equal(spec.target.behavior_spec.input_window.hard, false);
    assert.equal(spec.target.output_feature.hard, false);
  } finally {
    globalThis.document.getElementById = priorGetElementById;
  }
});

test('Design Spec Config UI only exposes solver-backed output feature choices', () => {
  const html = NODE_TYPES['design-spec-config'].createBody('node-test');
  const outputSelect = html.match(/<select id="node-test-spec-output-feature"[\s\S]*?<\/select>/)?.[0] || '';
  const values = [...outputSelect.matchAll(/<option value="([^"]*)"/g)].map(match => match[1]);
  assert.deepEqual(values, ['', 'threshold', 'fold_change', 'level']);
});

test('DesignabilitySpec authored schema exposes only solver-backed sampled target enums', () => {
  const schema = JSON.parse(fs.readFileSync(path.join(repoRoot, 'schemas', 'designability-spec.schema.json'), 'utf8'));
  const targetProps = schema.properties.target.properties;

  assert.deepEqual(
    new Set(targetProps.output_feature.properties.feature.enum),
    new Set(['threshold', 'level', 'fold_change']),
  );
  assert.deepEqual(
    new Set(targetProps.output_feature.properties.operator.enum),
    new Set(['=', '>=', '<=']),
  );
  assert.deepEqual(
    new Set(targetProps.shape.properties.class.enum),
    new Set(['monotonic', 'bell_shaped']),
  );
});

test('Design Spec Config JSON override cannot reintroduce unsupported output feature choices', () => {
  const priorGetElementById = globalThis.document.getElementById;
  const field = (value = '', checked = false) => ({ value, checked });
  const fields = {
    'node-test-spec-kind': field('behavior_spec'),
    'node-test-spec-target': field('1, 0'),
    'node-test-spec-input': field('tA'),
    'node-test-spec-output': field('B'),
    'node-test-spec-kd-lo': field('-3'),
    'node-test-spec-kd-hi': field('3'),
    'node-test-spec-total-lo': field('-3'),
    'node-test-spec-total-hi': field('3'),
    'node-test-spec-radius': field('0'),
    'node-test-spec-volume': field(''),
    'node-test-spec-window-lo': field('-2'),
    'node-test-spec-window-hi': field('2'),
    'node-test-spec-operating-points': field(''),
    'node-test-spec-output-feature': field(''),
    'node-test-spec-output-value': field(''),
    'node-test-spec-output-samples': field(''),
    'node-test-spec-output-tolerance': field(''),
    'node-test-spec-shape': field(''),
    'node-test-spec-shape-monotonicity': field('any'),
    'node-test-spec-shape-prominence': field(''),
    'node-test-spec-shape-samples': field(''),
    'node-test-spec-shape-tolerance': field(''),
    'node-test-spec-dynamic-range': field(''),
    'node-test-spec-dynamic-samples': field(''),
    'node-test-spec-transition-spacing': field(''),
    'node-test-spec-transition-order': field(''),
    'node-test-spec-rank-primary': field(''),
    'node-test-spec-rank-secondary': field(''),
    'node-test-spec-max-species': field(''),
    'node-test-spec-max-reactions': field(''),
    'node-test-spec-max-mu': field(''),
    'node-test-spec-allow-near-minimal': field('', true),
    'node-test-spec-max-exact': field('3'),
    'node-test-spec-extra-species': field('1'),
    'node-test-spec-extra-reactions': field('1'),
    'node-test-spec-extra-mu': field('1'),
    'node-test-spec-block-hard': field('', true),
    'node-test-spec-json': field(JSON.stringify({
      target: {
        output_feature: {
          feature: 'dynamic_range',
          operator: '>=',
          value: 4,
          hard: true,
        },
      },
    })),
  };
  globalThis.document.getElementById = id => fields[id] || null;
  try {
    assert.throws(
      () => readDesignSpecConfig('node-test'),
      /dynamic range belongs in constraints.dynamic_range/i,
    );
  } finally {
    globalThis.document.getElementById = priorGetElementById;
  }
});

test('Design Spec Config full JSON override bypasses blank manual behavior fields', () => {
  const priorGetElementById = globalThis.document.getElementById;
  const field = (value = '', checked = false) => ({ value, checked });
  const fullSpec = {
    schema_version: 'bne-designability/v1.0.0',
    source: { kind: 'hand_authored' },
    target: {
      behavior_spec: {
        input: 'tA',
        output: 'B',
        program: [{ kind: 'reaction_order', value: 1 }],
      },
    },
    constraints: {
      parameter_bounds: { kd_log10: [-3, 3] },
    },
  };
  const fields = {
    'node-test-spec-kind': field('behavior_spec'),
    'node-test-spec-target': field(''),
    'node-test-spec-input': field(''),
    'node-test-spec-output': field(''),
    'node-test-spec-json': field(JSON.stringify(fullSpec)),
  };
  globalThis.document.getElementById = id => fields[id] || field('');
  try {
    const spec = readDesignSpecConfig('node-test');
    assert.equal(spec.source.kind, 'hand_authored');
    assert.equal(spec.source.node_id, 'node-test');
    assert.deepEqual(spec.target.behavior_spec.program.map(step => step.value), [1]);
    assert.deepEqual(spec.constraints.parameter_bounds.kd_log10, [-3, 3]);
  } finally {
    globalThis.document.getElementById = priorGetElementById;
  }
});

test('Design Spec Config full JSON override validates wrapper contracts before backend', () => {
  const priorGetElementById = globalThis.document.getElementById;
  const field = (value = '', checked = false) => ({ value, checked });
  const validSpec = {
    schema_version: 'bne-designability/v1.0.0',
    source: { kind: 'hand_authored' },
    target: {
      behavior_spec: {
        input: 'tA',
        output: 'B',
        program: [{ kind: 'reaction_order', value: 1 }],
      },
    },
    constraints: {
      parameter_bounds: { kd_log10: [-3, 3] },
    },
  };
  const cases = [
    {
      patch: { extra_root: true },
      message: /unknown DesignabilitySpec key/i,
    },
    {
      patch: { source: undefined },
      message: /source/i,
    },
    {
      patch: { source: null },
      message: /source/i,
    },
    {
      patch: { source: {} },
      message: /source.kind/i,
    },
    {
      patch: { source: { kind: 'wizard' } },
      message: /source.kind/i,
    },
    {
      patch: { source: { kind: 'hand_authored', node_id: 12 } },
      message: /source.node_id/i,
    },
    {
      patch: { ranking_policy: { verified_only: false } },
      message: /verified_only.*true/i,
    },
    {
      patch: {
        constraints: {},
        candidate_budget: { max_exact_placements: 3 },
      },
      message: /max_exact_placements.*constraints\.parameter_bounds/i,
    },
    {
      patch: {
        constraints: {},
        ranking_policy: { prefer: ['chebyshev_radius'] },
      },
      message: /ranking_policy\.prefer\/0.*chebyshev_radius.*constraints\.parameter_bounds/i,
    },
    {
      patch: { ranking_policy: { prefer: ['dynamic_range'] } },
      message: /ranking_policy\.prefer\/0.*dynamic_range.*constraints\.dynamic_range/i,
    },
    {
      patch: { ranking_policy: { prefer: ['transition_spacing'] } },
      message: /ranking_policy\.prefer\/0.*transition_spacing.*constraints\.transitions\.min_spacing_decades/i,
    },
    {
      patch: { ranking_policy: { prefer: ['condition_number'] } },
      message: /ranking_policy\.prefer\/0.*condition_number.*no solver-backed/i,
    },
    {
      patch: { audit_policy: { unsupported: 'warn' } },
      message: /audit_policy.unsupported.*block_if_hard/i,
    },
    {
      patch: { candidate_budget: { max_exact_placements: -1 } },
      message: /candidate_budget.max_exact_placements/i,
    },
  ];

  try {
    for (const { patch, message } of cases) {
      const spec = { ...validSpec, ...patch };
      const fields = {
        'node-test-spec-kind': field('behavior_spec'),
        'node-test-spec-json': field(JSON.stringify(spec)),
      };
      globalThis.document.getElementById = id => fields[id] || field('');
      assert.throws(() => readDesignSpecConfig('node-test'), message);
    }
  } finally {
    globalThis.document.getElementById = priorGetElementById;
  }
});

test('Design Spec Config full JSON allows minimal-certificate specs without parameter bounds', () => {
  const priorGetElementById = globalThis.document.getElementById;
  const field = (value = '', checked = false) => ({ value, checked });
  const minimalOnlySpec = {
    schema_version: 'bne-designability/v1.0.0',
    source: { kind: 'hand_authored' },
    target: {
      behavior_spec: {
        input: 'tA',
        output: 'B',
        program: [{ kind: 'reaction_order', value: 1 }],
      },
    },
    constraints: {
      network: { max_reactions: 4 },
    },
    candidate_budget: {
      max_exact_placements: 0,
    },
  };
  const fields = {
    'node-test-spec-kind': field('behavior_spec'),
    'node-test-spec-json': field(JSON.stringify(minimalOnlySpec)),
  };
  globalThis.document.getElementById = id => fields[id] || field('');
  try {
    const spec = readDesignSpecConfig('node-test');
    assert.equal(spec.source.kind, 'hand_authored');
    assert.equal(spec.source.node_id, 'node-test');
    assert.equal(spec.constraints.parameter_bounds, undefined);
    assert.equal(spec.candidate_budget.max_exact_placements, 0);
  } finally {
    globalThis.document.getElementById = priorGetElementById;
  }
});

test('Design Spec Config full JSON override validates nested target and constraint clauses before backend', () => {
  const priorGetElementById = globalThis.document.getElementById;
  const field = (value = '', checked = false) => ({ value, checked });
  const validSpec = {
    schema_version: 'bne-designability/v1.0.0',
    source: { kind: 'hand_authored' },
    target: {
      behavior_spec: {
        input: 'tA',
        output: 'B',
        program: [{ kind: 'reaction_order', value: 1 }],
      },
    },
    constraints: {
      parameter_bounds: { kd_log10: [-3, 3] },
    },
  };
  const cases = [
    {
      patch: {
        target: {
          legacy_target: { target_kind: 'exact', target: [1] },
          behavior_spec: validSpec.target.behavior_spec,
        },
      },
      message: /legacy_target.*behavior_spec/i,
    },
    {
      patch: {
        target: {
          legacy_target: {
            target_kind: 'exact',
            target: [1],
            shape: {
              class: 'bell_shaped',
              min_prominence_log10: 0.2,
              sample_points: 51,
              tolerance_log10: 0.01,
              hard: true,
            },
          },
        },
      },
      message: /legacy_target\.shape/i,
    },
    {
      patch: {
        target: {
          legacy_target: {
            target_kind: 'exact',
            target: [true],
          },
        },
      },
      message: /legacy_target\.target.*finite non-Bool/i,
    },
    {
      patch: {
        target: {
          legacy_target: {
            target_kind: 'exact',
            target: [],
          },
        },
      },
      message: /legacy_target\.target.*at least one/i,
    },
    {
      patch: {
        target: {
          legacy_target: {
            target_kind: 'exact',
            target: '1, 0',
          },
        },
      },
      message: /legacy_target\.target.*array/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: true,
            output: 'B',
            program: [{ kind: 'reaction_order', value: 1 }],
          },
        },
      },
      message: /behavior_spec.input/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [{ kind: 'reaction_order', value: 1 }],
            input_window: { input_log10: [-2, 2] },
          },
          input_window: {
            input_log10: [-10, -9],
            hard: false,
          },
        },
      },
      message: /target\.input_window.*behavior_spec/i,
    },
    {
      patch: { constraints: { parameter_bounds: {} } },
      message: /parameter_bounds.*kd_log10.*total_log10.*by_class/i,
    },
    {
      patch: { constraints: { parameter_bounds: { kd_log10: [-3] } } },
      message: /parameter_bounds.kd_log10/i,
    },
    {
      patch: {
        constraints: {
          parameter_bounds: {
            by_class: { kd: [-1, 1] },
            kd_log10: [-3, 3],
          },
        },
      },
      message: /parameter_bounds\.kd_log10.*by_class\.kd/i,
    },
    {
      patch: {
        constraints: {
          parameter_bounds: {
            by_class: { total: [-1, 1] },
            total_log10: [-3, 3],
          },
        },
      },
      message: /parameter_bounds\.total_log10.*by_class\.total/i,
    },
    {
      patch: {
        constraints: {
          parameter_bounds: { kd_log10: [-3, 3] },
          robustness: { condition_number_max: -1 },
        },
      },
      message: /robustness.condition_number_max/i,
    },
    {
      patch: {
        constraints: {
          parameter_bounds: { kd_log10: [-3, 3] },
          robustness: {
            min_tunable_volume_lower_bound: 0.1,
            min_tunable_volume: 0.1,
          },
        },
      },
      message: /min_tunable_volume_lower_bound.*min_tunable_volume/i,
    },
    {
      patch: {
        constraints: {
          parameter_bounds: { kd_log10: [-3, 3] },
          transitions: { order: [0, 0] },
        },
      },
      message: /transitions.order.*unique/i,
    },
    {
      patch: {
        constraints: {
          parameter_bounds: { kd_log10: [-3, 3] },
          transitions: { hard: true },
        },
      },
      message: /transitions.*min_spacing_decades.*order/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [{ kind: 'reaction_order', value: 1 }],
            input_window: { min_spacing_decades: 0.5 },
          },
        },
      },
      message: /input_window\.min_spacing_decades.*input_window\.input_log10/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [{ kind: 'reaction_order', value: 1 }],
            input_window: { input_log10: [-2, 2] },
          },
          output_feature: {
            feature: 'threshold',
            operator: 'between',
            value: 0.5,
            sample_points: 51,
            tolerance_log10: 0.01,
          },
        },
      },
      message: /output_feature.operator.*>=.*<=.*=/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [{ kind: 'reaction_order', value: 1 }],
            input_window: { input_log10: [-2, 2] },
          },
          output_feature: {
            feature: 'peak_time',
            operator: '=',
            value: 0.5,
            sample_points: 51,
            tolerance_log10: 0.01,
          },
        },
      },
      message: /Unsupported output_feature peak_time/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [{ kind: 'reaction_order', value: 1 }],
            input_window: { input_log10: [-2, 2] },
          },
          output_feature: {
            feature: 'fold_change',
            operator: '=',
            value: 0,
            sample_points: 51,
            tolerance_log10: 0.01,
          },
        },
      },
      message: /fold_change.*positive/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [{ kind: 'reaction_order', value: 1 }],
          },
          output_feature: {
            feature: 'threshold',
            operator: '>=',
            value: 0.5,
            sample_points: 51,
            tolerance_log10: 0.01,
          },
        },
      },
      message: /output_feature.*input_window/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [{ kind: 'reaction_order', value: 1 }],
          },
        },
        constraints: {
          parameter_bounds: { kd_log10: [-3, 3] },
          dynamic_range: {
            min_fold_change: 2,
            sample_points: 51,
          },
        },
      },
      message: /dynamic_range.*input_window/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [{ kind: 'reaction_order', value: 1 }],
          },
          shape: {
            class: 'monotonic',
            monotonicity: 'any',
            sample_points: 51,
            tolerance_log10: 0.01,
          },
        },
      },
      message: /shape.*input_window/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [{ kind: 'reaction_order', value: 1 }],
            input_window: { input_log10: [-2, 2] },
          },
          shape: {
            class: 'pulse',
            sample_points: 51,
            tolerance_log10: 0.01,
          },
        },
      },
      message: /Unsupported target\.shape pulse/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [{ kind: 'reaction_order', value: 1 }],
            input_window: { input_log10: [-2, 2] },
          },
          shape: {
            class: 'monotonic',
            monotonicity: 'any',
            min_prominence_log10: 0.5,
            sample_points: 51,
            tolerance_log10: 0.01,
          },
        },
      },
      message: /shape\.min_prominence_log10.*monotonic/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [{ kind: 'reaction_order', value: 1 }],
            input_window: { input_log10: [-2, 2] },
          },
          shape: {
            class: 'bell_shaped',
            min_prominence_log10: 0.5,
            min_prominence_decades: 1.0,
            sample_points: 51,
            tolerance_log10: 0.01,
          },
        },
      },
      message: /shape\.min_prominence_decades.*min_prominence_log10/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [
              { kind: 'reaction_order', value: 1 },
              { kind: 'reaction_order', value: 0 },
              { kind: 'reaction_order', value: -1 },
            ],
            input_window: {
              operating_points_log10: [-1, 0, 1],
            },
          },
        },
      },
      message: /operating_points_log10.*input_window/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [
              { kind: 'reaction_order', value: 1 },
              { kind: 'reaction_order', value: 0 },
              { kind: 'reaction_order', value: -1 },
            ],
            input_window: {
              input_log10: [-2, 2],
              operating_points_log10: [-1, 1],
            },
          },
        },
      },
      message: /operating_points_log10.*program length/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [
              { kind: 'reaction_order', value: 1 },
              { kind: 'reaction_order', value: 0 },
            ],
            input_window: {
              input_log10: [-2, 2],
              operating_points_log10: [-3, 0],
            },
          },
        },
      },
      message: /operating_points_log10.*input_window/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [
              { kind: 'reaction_order', value: 1 },
              { kind: 'reaction_order', value: 0 },
            ],
            input_window: {
              input_log10: [-2, 2],
              operating_points_log10: [-1, 0],
            },
          },
        },
        constraints: {
          parameter_bounds: { kd_log10: [-3, 3] },
          transitions: { min_spacing_decades: 1.5 },
        },
      },
      message: /operating_points_log10.*spacing/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [{ kind: 'reaction_order', value: 1 }],
            input_window: { input_log10: [-2, 2] },
          },
        },
        constraints: {
          parameter_bounds: { kd_log10: [-3, 3] },
          transitions: { min_spacing_decades: 0.5 },
        },
      },
      message: /transition.*spacing.*at least two/i,
    },
    {
      patch: {
        target: {
          behavior_spec: {
            input: 'tA',
            output: 'B',
            program: [
              { kind: 'reaction_order', value: 1 },
              { kind: 'reaction_order', value: 0 },
            ],
          },
        },
        constraints: {
          parameter_bounds: { kd_log10: [-3, 3] },
          transitions: { order: [0, 1] },
        },
      },
      message: /transitions\.order.*input_window/i,
    },
    {
      patch: {
        target: {
          temporal_dynamics: {
            peak_width_seconds: {},
            hard: false,
          },
        },
      },
      message: /peak_width_seconds.*min.*max/i,
    },
    {
      patch: {
        target: {
          temporal_dynamics: {
            peak_width_seconds: { min: 3, max: 1 },
            hard: false,
          },
        },
      },
      message: /peak_width_seconds.*min.*max/i,
    },
  ];

  try {
    for (const { patch, message } of cases) {
      const spec = { ...validSpec, ...patch };
      const fields = {
        'node-test-spec-kind': field('behavior_spec'),
        'node-test-spec-json': field(JSON.stringify(spec)),
      };
      globalThis.document.getElementById = id => fields[id] || field('');
      assert.throws(() => readDesignSpecConfig('node-test'), message);
    }
  } finally {
    globalThis.document.getElementById = priorGetElementById;
  }
});

test('Design Spec Config rejects transition orders that do not match the behavior program length', () => {
  const priorGetElementById = globalThis.document.getElementById;
  const field = (value = '', checked = false) => ({ value, checked });
  const validSpec = {
    schema_version: 'bne-designability/v1.0.0',
    source: { kind: 'hand_authored' },
    target: {
      behavior_spec: {
        input: 'tA',
        output: 'B',
        program: [
          { kind: 'reaction_order', value: 1 },
          { kind: 'reaction_order', value: 0 },
          { kind: 'reaction_order', value: -1 },
        ],
        input_window: { input_log10: [-2, 2] },
      },
    },
    constraints: {
      parameter_bounds: { kd_log10: [-3, 3] },
      transitions: { order: [0, 1] },
    },
  };

  try {
    globalThis.document.getElementById = id => ({
      'node-test-spec-kind': field('behavior_spec'),
      'node-test-spec-json': field(JSON.stringify(validSpec)),
    }[id] || field(''));
    assert.throws(
      () => readDesignSpecConfig('node-test'),
      /transitions\.order.*program length/i,
    );
  } finally {
    globalThis.document.getElementById = priorGetElementById;
  }
});

test('Design Spec Config validation preview reports hard unsupported clauses as blocked', async () => {
  const priorGetElementById = globalThis.document.getElementById;
  const priorCreateElement = globalThis.document.createElement;
  const priorFetch = globalThis.fetch;
  const priorRequestAnimationFrame = globalThis.requestAnimationFrame;
  const field = (value = '', checked = false) => ({ value, checked });
  const preview = { style: { display: 'none' }, innerHTML: '' };
  const toastContainer = { appendChild() {} };
  const spec = {
    schema_version: 'bne-designability/v1.0.0',
    source: { kind: 'hand_authored' },
    target: {
      temporal_dynamics: {
        peak_width_seconds: { min: 1 },
        hard: true,
      },
    },
    constraints: {
      robustness: {
        min_sampled_pass_fraction: 0.8,
      },
    },
    audit_policy: {
      unsupported: 'block_if_hard',
      path_format: 'json_pointer',
      include_supported: true,
    },
  };

  try {
    globalThis.requestAnimationFrame = fn => fn();
    globalThis.document.createElement = () => ({
      className: '',
      textContent: '',
      classList: { add() {}, remove() {} },
      remove() {},
    });
    globalThis.document.getElementById = id => ({
      'node-test-spec-kind': field('behavior_spec'),
      'node-test-spec-json': field(JSON.stringify(spec)),
      'node-test-spec-preview': preview,
      'toast-container': toastContainer,
      'status-badge': { className: '', textContent: '' },
    }[id] || field(''));
    globalThis.fetch = async (url, options) => {
      assert.equal(url, '/api/v1/validate_designability_spec');
      const body = JSON.parse(options.body);
      assert.deepEqual(body.target.temporal_dynamics.peak_width_seconds, { min: 1 });
      assert.equal(body.constraints.robustness.min_sampled_pass_fraction, 0.8);
      return {
        headers: { get: () => 'application/json' },
        json: async () => ({
          ok: true,
          constraint_audit: [
            {
              path: '/target/temporal_dynamics/peak_width_seconds',
              kind: 'temporal_dynamics',
              support_level: 'unsupported',
              hard: true,
              reason: 'Temporal dynamics are schema-recognized but not solver-backed.',
            },
            {
              path: '/constraints/robustness/min_sampled_pass_fraction',
              kind: 'min_sampled_pass_fraction',
              support_level: 'unsupported',
              hard: true,
              reason: 'This robustness constraint is not yet backed by an exact or sampled designability solver.',
            },
          ],
          blocked_by_unsupported_hard_clause: true,
        }),
      };
    };

    const data = await validateDesignSpecConfig('node-test');

    assert.equal(data.blocked_by_unsupported_hard_clause, true);
    assert.match(preview.innerHTML, /blocked/i);
    assert.match(preview.innerHTML, /\/target\/temporal_dynamics\/peak_width_seconds/);
    assert.match(preview.innerHTML, /\/constraints\/robustness\/min_sampled_pass_fraction/);
    assert.match(preview.innerHTML, /unsupported/i);
    assert.doesNotMatch(preview.innerHTML, />valid</i);
  } finally {
    globalThis.document.getElementById = priorGetElementById;
    globalThis.document.createElement = priorCreateElement;
    globalThis.fetch = priorFetch;
    globalThis.requestAnimationFrame = priorRequestAnimationFrame;
  }
});

test('Design Spec Config rejects fractional integer fields instead of truncating', () => {
  const priorGetElementById = globalThis.document.getElementById;
  const field = (value = '', checked = false) => ({ value, checked });
  const fields = {
    'node-test-spec-kind': field('behavior_spec'),
    'node-test-spec-target': field('1, 0'),
    'node-test-spec-input': field('tA'),
    'node-test-spec-output': field('B'),
    'node-test-spec-kd-lo': field('-3'),
    'node-test-spec-kd-hi': field('3'),
    'node-test-spec-total-lo': field('-3'),
    'node-test-spec-total-hi': field('3'),
    'node-test-spec-radius': field('0'),
    'node-test-spec-volume': field(''),
    'node-test-spec-window-lo': field('-2'),
    'node-test-spec-window-hi': field('2'),
    'node-test-spec-operating-points': field(''),
    'node-test-spec-output-feature': field(''),
    'node-test-spec-output-value': field(''),
    'node-test-spec-output-samples': field(''),
    'node-test-spec-output-tolerance': field(''),
    'node-test-spec-shape': field(''),
    'node-test-spec-shape-monotonicity': field('any'),
    'node-test-spec-shape-prominence': field(''),
    'node-test-spec-shape-samples': field(''),
    'node-test-spec-shape-tolerance': field(''),
    'node-test-spec-dynamic-range': field(''),
    'node-test-spec-dynamic-samples': field(''),
    'node-test-spec-transition-spacing': field(''),
    'node-test-spec-transition-order': field(''),
    'node-test-spec-rank-primary': field(''),
    'node-test-spec-rank-secondary': field(''),
    'node-test-spec-max-species': field(''),
    'node-test-spec-max-reactions': field(''),
    'node-test-spec-max-mu': field(''),
    'node-test-spec-allow-near-minimal': field('', true),
    'node-test-spec-max-exact': field('3.9'),
    'node-test-spec-extra-species': field('1'),
    'node-test-spec-extra-reactions': field('1'),
    'node-test-spec-extra-mu': field('1'),
    'node-test-spec-block-hard': field('', true),
    'node-test-spec-json': field(''),
  };
  globalThis.document.getElementById = id => fields[id] || null;
  try {
    assert.throws(
      () => readDesignSpecConfig('node-test'),
      /Invalid integer in spec-max-exact/,
    );
  } finally {
    globalThis.document.getElementById = priorGetElementById;
  }
});

test('Design Spec Config blank required numeric fields use configured defaults', () => {
  const priorGetElementById = globalThis.document.getElementById;
  const field = (value = '', checked = false) => ({ value, checked });
  const fields = {
    'node-test-spec-kind': field('behavior_spec'),
    'node-test-spec-target': field('1, 0'),
    'node-test-spec-input': field('tA'),
    'node-test-spec-output': field('B'),
    'node-test-spec-kd-lo': field(''),
    'node-test-spec-kd-hi': field(''),
    'node-test-spec-total-lo': field(''),
    'node-test-spec-total-hi': field(''),
    'node-test-spec-radius': field(''),
    'node-test-spec-volume': field(''),
    'node-test-spec-window-lo': field(''),
    'node-test-spec-window-hi': field(''),
    'node-test-spec-operating-points': field(''),
    'node-test-spec-output-feature': field(''),
    'node-test-spec-output-value': field(''),
    'node-test-spec-output-samples': field(''),
    'node-test-spec-output-tolerance': field(''),
    'node-test-spec-shape': field(''),
    'node-test-spec-shape-monotonicity': field('any'),
    'node-test-spec-shape-prominence': field(''),
    'node-test-spec-shape-samples': field(''),
    'node-test-spec-shape-tolerance': field(''),
    'node-test-spec-dynamic-range': field(''),
    'node-test-spec-dynamic-samples': field(''),
    'node-test-spec-transition-spacing': field(''),
    'node-test-spec-transition-order': field(''),
    'node-test-spec-rank-primary': field(''),
    'node-test-spec-rank-secondary': field(''),
    'node-test-spec-max-species': field(''),
    'node-test-spec-max-reactions': field(''),
    'node-test-spec-max-mu': field(''),
    'node-test-spec-allow-near-minimal': field('', true),
    'node-test-spec-max-exact': field('3'),
    'node-test-spec-extra-species': field('1'),
    'node-test-spec-extra-reactions': field('1'),
    'node-test-spec-extra-mu': field('1'),
    'node-test-spec-block-hard': field('', true),
    'node-test-spec-json': field(''),
  };
  globalThis.document.getElementById = id => fields[id] || null;
  try {
    const spec = readDesignSpecConfig('node-test');
    assert.deepEqual(spec.constraints.parameter_bounds.kd_log10, [-3, 3]);
    assert.deepEqual(spec.constraints.parameter_bounds.total_log10, [-3, 3]);
    assert.equal(spec.constraints.robustness.min_chebyshev_radius, 0);
  } finally {
    globalThis.document.getElementById = priorGetElementById;
  }
});

test('Design Spec Config UI only exposes solver-backed shape choices', () => {
  const html = NODE_TYPES['design-spec-config'].createBody('node-test');
  const shapeSelect = html.match(/<select id="node-test-spec-shape"[\s\S]*?<\/select>/)?.[0] || '';
  const values = [...shapeSelect.matchAll(/<option value="([^"]*)"/g)].map(match => match[1]);
  assert.deepEqual(values, ['', 'monotonic', 'bell_shaped']);
});

test('Design Spec Config JSON override cannot reintroduce unsupported shape choices', () => {
  const priorGetElementById = globalThis.document.getElementById;
  const field = (value = '', checked = false) => ({ value, checked });
  const fields = {
    'node-test-spec-kind': field('behavior_spec'),
    'node-test-spec-target': field('1, 0'),
    'node-test-spec-input': field('tA'),
    'node-test-spec-output': field('B'),
    'node-test-spec-kd-lo': field('-3'),
    'node-test-spec-kd-hi': field('3'),
    'node-test-spec-total-lo': field('-3'),
    'node-test-spec-total-hi': field('3'),
    'node-test-spec-radius': field('0'),
    'node-test-spec-volume': field(''),
    'node-test-spec-window-lo': field('-2'),
    'node-test-spec-window-hi': field('2'),
    'node-test-spec-operating-points': field(''),
    'node-test-spec-output-feature': field(''),
    'node-test-spec-output-value': field(''),
    'node-test-spec-output-samples': field(''),
    'node-test-spec-output-tolerance': field(''),
    'node-test-spec-shape': field(''),
    'node-test-spec-shape-monotonicity': field('any'),
    'node-test-spec-shape-prominence': field(''),
    'node-test-spec-shape-samples': field(''),
    'node-test-spec-shape-tolerance': field(''),
    'node-test-spec-dynamic-range': field(''),
    'node-test-spec-dynamic-samples': field(''),
    'node-test-spec-transition-spacing': field(''),
    'node-test-spec-transition-order': field(''),
    'node-test-spec-rank-primary': field(''),
    'node-test-spec-rank-secondary': field(''),
    'node-test-spec-max-species': field(''),
    'node-test-spec-max-reactions': field(''),
    'node-test-spec-max-mu': field(''),
    'node-test-spec-allow-near-minimal': field('', true),
    'node-test-spec-max-exact': field('3'),
    'node-test-spec-extra-species': field('1'),
    'node-test-spec-extra-reactions': field('1'),
    'node-test-spec-extra-mu': field('1'),
    'node-test-spec-block-hard': field('', true),
    'node-test-spec-json': field(JSON.stringify({
      target: {
        shape: {
          class: 'threshold',
          hard: true,
        },
      },
    })),
  };
  try {
    for (const shapeClass of ['threshold', 'level']) {
      fields['node-test-spec-json'] = field(JSON.stringify({
        target: {
          shape: {
            class: shapeClass,
            hard: true,
          },
        },
      }));
      globalThis.document.getElementById = id => fields[id] || null;
      assert.throws(
        () => readDesignSpecConfig('node-test'),
        new RegExp(`${shapeClass} belongs in target\\.output_feature`, 'i'),
      );
    }
  } finally {
    globalThis.document.getElementById = priorGetElementById;
  }
});

test('Agent nested BehaviorSpec lowers to a backend-rerunnable DesignabilitySpec wrapper', () => {
  const nested = {
    schema_version: 'bne-behavior/v0.1.0',
    goal: {
      behavior_family: 'dose_shape',
      behavior_class: 'monotone_activation',
      output: { kind: 'species', symbol: 'B' },
    },
    behavior_spec: {
      feature_space: 'reaction_order',
      program: [
        { kind: 'reaction_order', operator: '=', value: 1 },
        { kind: 'reaction_order', operator: '=', value: 0 },
      ],
      input_window: { input_log10: [-2, 2], hard: true },
    },
    shape_preferences: {
      dynamic_range_log10: { min: 1.2, sample_points: 81 },
    },
    network_constraints: {
      max_reactions: 4,
      kd_profile: {
        log10_kd_min: -2,
        log10_kd_max: 2,
      },
    },
    verification_policy: {
      min_robustness_score: 0.8,
    },
  };
  const spec = normalizeAgentDesignabilitySpec(nested, {
    input_symbol: 'tA',
    observe_species: 'B',
  });

  assert.equal(spec.schema_version, 'bne-designability/v1.0.0');
  assert.equal(spec.source.kind, 'agent_design');
  assert.deepEqual(spec.source.provenance.agent_behavior_spec, nested);
  assert.equal(spec.target.behavior_spec.input, 'tA');
  assert.equal(spec.target.behavior_spec.output, 'B');
  assert.deepEqual(spec.target.behavior_spec.program.map(step => step.value), [1, 0]);
  assert.deepEqual(spec.target.behavior_spec.input_window.input_log10, [-2, 2]);
  assert.equal(spec.target.shape, undefined);
  assert.equal(spec.target.output_feature, undefined);
  assert.ok(spec.constraints.dynamic_range.min_fold_change > 15);
  assert.equal(spec.constraints.dynamic_range.sample_points, 81);
  assert.equal(spec.constraints.robustness, undefined);
  assert.deepEqual(spec.constraints.parameter_bounds.kd_log10, [-2, 2]);
  assert.equal(spec.constraints.parameter_bounds.total_log10, undefined);
  assert.equal(spec.constraints.network.max_reactions, 4);
  assert.ok(spec.candidate_budget.max_exact_placements > 0);
});

test('Agent fallback lowering refuses non-rerunnable natural-language-only specs', () => {
  const nested = {
    schema_version: 'bne-behavior/v0.1.0',
    goal: {
      behavior_family: 'dose_shape',
      behavior_class: 'monotone_activation',
    },
    shape_preferences: {
      dynamic_range_log10: { min: 1.2 },
    },
    verification_policy: {
      min_robustness_score: 0.8,
    },
  };
  const spec = normalizeAgentDesignabilitySpec(nested);

  assert.equal(spec, null);
});

test('Agent BehaviorSpec export refuses sampled dynamic range without explicit samples', () => {
  const nested = {
    schema_version: 'bne-behavior/v0.1.0',
    behavior_spec: {
      feature_space: 'reaction_order',
      input: 'tA',
      output: 'C_A_A',
      program: [
        { kind: 'reaction_order', operator: '=', value: 1 },
      ],
      input_window: { input_log10: [-2, 2], hard: true },
    },
    shape_preferences: {
      dynamic_range_log10: { min: 1.2 },
    },
  };
  const spec = normalizeAgentDesignabilitySpec(nested);

  assert.equal(spec, null);
});

test('Agent BehaviorSpec export refuses sampled dynamic range without a behavior input window', () => {
  const nested = {
    schema_version: 'bne-behavior/v0.1.0',
    behavior_spec: {
      feature_space: 'reaction_order',
      input: 'tA',
      output: 'C_A_A',
      program: [
        { kind: 'reaction_order', operator: '=', value: 1 },
      ],
    },
    shape_preferences: {
      dynamic_range_log10: { min: 1.2, sample_points: 81 },
    },
  };
  const spec = normalizeAgentDesignabilitySpec(nested);

  assert.equal(spec, null);
});

test('Agent response cards with invalid sampled specs are not exportable candidates', () => {
  const invalidCompiledSpec = {
    schema_version: 'bne-behavior/v0.1.0',
    behavior_spec: {
      feature_space: 'reaction_order',
      input: 'tA',
      output: 'C_A_A',
      program: [
        { kind: 'reaction_order', operator: '=', value: 1 },
      ],
      input_window: { input_log10: [-2, 2], hard: true },
    },
    shape_preferences: {
      dynamic_range_log10: { min: 1.2 },
    },
  };
  const cards = cardsWithAgentSpec({
    cards: [
      {
        rules: ['A + A <-> C_A_A'],
        input_symbol: 'tA',
        output_symbol: 'C_A_A',
        compiled_spec: invalidCompiledSpec,
      },
    ],
  });

  assert.deepEqual(cards, []);
});

test('Agent response cards with invalid wrapped DesignabilitySpec are not exportable candidates', () => {
  const invalidWrappedSpec = {
    schema_version: 'bne-designability/v1.0.0',
    source: { kind: 'agent_design' },
    target: {
      behavior_spec: {
        feature_space: 'reaction_order',
        input: 'tA',
        output: 'C_A_A',
        program: [
          { kind: 'reaction_order', value: 1 },
        ],
        input_window: { input_log10: [-2, 2] },
      },
    },
    constraints: {
      dynamic_range: { min_fold_change: 10 },
    },
  };
  const cards = cardsWithAgentSpec({
    cards: [
      {
        rules: ['A + A <-> C_A_A'],
        designability_spec: invalidWrappedSpec,
      },
    ],
  });

  assert.deepEqual(cards, []);
});

test('Agent wrapped DesignabilitySpec export refuses sampled clauses without solver prerequisites', () => {
  const validWrappedSpec = {
    schema_version: 'bne-designability/v1.0.0',
    source: { kind: 'agent_design' },
    target: {
      behavior_spec: {
        feature_space: 'reaction_order',
        input: 'tA',
        output: 'C_A_A',
        program: [
          { kind: 'reaction_order', value: 1 },
          { kind: 'reaction_order', value: 0 },
        ],
        input_window: { input_log10: [-2, 2] },
      },
    },
    constraints: {
      parameter_bounds: { kd_log10: [-2, 2] },
    },
  };
  const invalidSpecs = [
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          input: 'tA',
          output: 'C_A_A',
          program: ['bad'],
          input_window: { input_log10: [-2, 2] },
        },
      },
      constraints: {
        ...validWrappedSpec.constraints,
        dynamic_range: { min_fold_change: 2, sample_points: 51 },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          output: 'C_A_A',
          program: [{ kind: 'reaction_order', value: 1 }],
          input_window: { input_log10: [-2, 2] },
        },
      },
      constraints: {
        ...validWrappedSpec.constraints,
        dynamic_range: { min_fold_change: 2, sample_points: 51 },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          feature_space: 'concentration',
          input: 'tA',
          output: 'C_A_A',
          program: [{ kind: 'reaction_order', value: 1 }],
          input_window: { input_log10: [-2, 2] },
        },
      },
      constraints: {
        ...validWrappedSpec.constraints,
        dynamic_range: { min_fold_change: 2, sample_points: 51 },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        ...validWrappedSpec.target,
        output_feature: {
          feature: 'threshold',
          operator: 'between',
          value: 0.5,
          sample_points: 51,
          tolerance_log10: 0.01,
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          input: 'tA',
          output: 'C_A_A',
          program: [
            { kind: 'reaction_order', value: 1 },
            { kind: 'reaction_order', value: 0 },
          ],
          input_window: {
            input_log10: [-2, 2],
            operating_points_log10: [-3, 0],
          },
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          input: 'tA',
          output: 'C_A_A',
          program: [
            { kind: 'reaction_order', value: 1 },
            { kind: 'reaction_order', value: 0 },
          ],
          input_window: {
            input_log10: [-2, 2],
            operating_points_log10: [-1, 0],
          },
        },
      },
      constraints: {
        ...validWrappedSpec.constraints,
        transitions: { min_spacing_decades: 1.5 },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          input: 'tA',
          output: 'C_A_A',
          program: [{ kind: 'reaction_order', value: 1 }],
          input_window: null,
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          input: 'tA',
          output: 'C_A_A',
          program: [{ kind: 'reaction_order', value: 1 }],
          input_window: 'bad',
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        ...validWrappedSpec.target,
        output_feature: {
          feature: 'threshold',
          operator: null,
          value: 0.5,
          sample_points: 51,
          tolerance_log10: 0.01,
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        ...validWrappedSpec.target,
        output_feature: {
          feature: 'threshold',
          operator: '',
          value: 0.5,
          sample_points: 51,
          tolerance_log10: 0.01,
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        ...validWrappedSpec.target,
        output_feature: {
          feature: 'fold_change',
          operator: '=',
          value: 0,
          sample_points: 51,
          tolerance_log10: 0.01,
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          input: 'tA',
          output: 'C_A_A',
          program: [{ kind: 'reaction_order', value: 1 }],
        },
        output_feature: {
          feature: 'threshold',
          operator: '>=',
          value: 0.5,
          sample_points: 51,
          tolerance_log10: 0.01,
        },
      },
    },
    {
      ...validWrappedSpec,
      constraints: {
        ...validWrappedSpec.constraints,
        dynamic_range: {
          min_fold_change: 2,
          sample_points: 51,
        },
      },
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          input: 'tA',
          output: 'C_A_A',
          program: [{ kind: 'reaction_order', value: 1 }],
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          input: 'tA',
          output: 'C_A_A',
          program: [{ kind: 'reaction_order', value: 1 }],
          input_window: {
            input_log10: [-2, 2],
            operating_points_log10: ['bad'],
          },
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          input: 'tA',
          output: 'C_A_A',
          program: [{ kind: 'reaction_order', value: 1 }],
          input_window: {
            operating_points_log10: [0],
          },
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          input: 'tA',
          output: 'C_A_A',
          program: [{ kind: 'reaction_order', value: 1 }],
          input_window: {
            input_log10: [-2, 2],
            min_spacing_decades: 'bad',
          },
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          input: 'tA',
          output: 'C_A_A',
          program: [{ kind: 'reaction_order', value: 1 }],
          input_window: {
            input_log10: [-2, 2],
            operating_points_log10: [-1, 1],
          },
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          input: 'tA',
          output: 'C_A_A',
          program: [{ kind: 'reaction_order', value: 1 }],
          input_window: { input_log10: [-2, 2] },
        },
      },
      constraints: {
        ...validWrappedSpec.constraints,
        transitions: { order: [0, 1] },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          input: 'tA',
          output: 'C_A_A',
          program: [{ kind: 'reaction_order', value: 1 }],
          input_window: { input_log10: [-2, 2] },
        },
      },
      constraints: {
        ...validWrappedSpec.constraints,
        transitions: { min_spacing_decades: 0.5 },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          input: 'tA',
          output: 'C_A_A',
          program: [
            { kind: 'reaction_order', value: 1 },
            { kind: 'reaction_order', value: 0 },
          ],
        },
      },
      constraints: {
        ...validWrappedSpec.constraints,
        transitions: { order: [0, 1] },
      },
    },
  ];

  for (const spec of invalidSpecs) {
    assert.equal(normalizeAgentDesignabilitySpec(spec), null);
    assert.deepEqual(cardsWithAgentSpec({
      cards: [{ rules: ['A + A <-> C_A_A'], designability_spec: spec }],
    }), []);
  }
});

test('Agent wrapped DesignabilitySpec export enforces wrapper schema clauses', () => {
  const validWrappedSpec = {
    schema_version: 'bne-designability/v1.0.0',
    source: { kind: 'agent_design', node_id: 'agent-node' },
    target: {
      behavior_spec: {
        feature_space: 'reaction_order',
        input: 'tA',
        output: 'C_A_A',
        program: [{ kind: 'reaction_order', value: 1 }],
        input_window: { input_log10: [-2, 2] },
      },
    },
    constraints: {
      parameter_bounds: { kd_log10: [-2, 2] },
    },
    candidate_budget: { max_exact_placements: 3 },
    ranking_policy: { verified_only: true },
    audit_policy: { unsupported: 'block_if_hard', path_format: 'json_pointer' },
  };
  const invalidSpecs = [
    { ...validWrappedSpec, unexpected_root: true },
    { ...validWrappedSpec, source: undefined },
    { ...validWrappedSpec, source: null },
    { ...validWrappedSpec, source: {} },
    { ...validWrappedSpec, source: { kind: 'agent_design', unknown: true } },
    { ...validWrappedSpec, source: { kind: 'agent_design', node_id: 12 } },
    {
      ...validWrappedSpec,
      target: {
        legacy_target: {
          target_kind: 'exact',
          target: [true],
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        legacy_target: {
          target_kind: 'exact',
          target: [],
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        legacy_target: {
          target_kind: 'exact',
          target: '1, 0',
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        legacy_target: {
          target_kind: 'exact',
          target: [1],
          shape: {
            class: 'bell_shaped',
            min_prominence_log10: 0.2,
            sample_points: 51,
            tolerance_log10: 0.01,
            hard: true,
          },
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        legacy_target: {
          target_kind: 'exact',
          target: [1],
        },
        input_window: { input_log10: [-2, 2] },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        legacy_target: {
          target_kind: 'exact',
          target: [1],
        },
        temporal_dynamics: {
          peak_width_seconds: { min: 1 },
        },
      },
    },
    {
      ...validWrappedSpec,
      constraints: {
        ...validWrappedSpec.constraints,
        parameter_bounds: { kd_log10: [-2] },
      },
    },
    {
      ...validWrappedSpec,
      constraints: {
        ...validWrappedSpec.constraints,
        parameter_bounds: {
          by_class: { kd: [-1, 1] },
          kd_log10: [-2, 2],
        },
      },
    },
    {
      ...validWrappedSpec,
      constraints: {
        ...validWrappedSpec.constraints,
        parameter_bounds: {
          by_class: { total: [-1, 1] },
          total_log10: [-2, 2],
        },
      },
    },
    {
      ...validWrappedSpec,
      constraints: {
        ...validWrappedSpec.constraints,
        robustness: { min_chebyshev_radius: -0.1 },
      },
    },
    {
      ...validWrappedSpec,
      constraints: {
        ...validWrappedSpec.constraints,
        robustness: {
          min_tunable_volume_lower_bound: 0.1,
          min_tunable_volume: 0.1,
        },
      },
    },
    {
      ...validWrappedSpec,
      constraints: {
        ...validWrappedSpec.constraints,
        robustness: { condition_number_max: 100 },
      },
    },
    {
      ...validWrappedSpec,
      constraints: {
        ...validWrappedSpec.constraints,
        robustness: { min_sampled_pass_fraction: 0.8 },
      },
    },
    {
      ...validWrappedSpec,
      constraints: {
        ...validWrappedSpec.constraints,
        network: { max_species: 1.5 },
      },
    },
    { ...validWrappedSpec, candidate_budget: { max_exact_placements: -1 } },
    {
      ...validWrappedSpec,
      constraints: {},
      candidate_budget: { max_exact_placements: 3 },
    },
    {
      ...validWrappedSpec,
      constraints: {},
      candidate_budget: { max_exact_placements: 0 },
      ranking_policy: { prefer: ['chebyshev_radius'] },
    },
    { ...validWrappedSpec, ranking_policy: { verified_only: false } },
    { ...validWrappedSpec, ranking_policy: { prefer: ['unknown_metric'] } },
    { ...validWrappedSpec, ranking_policy: { prefer: ['dynamic_range'] } },
    { ...validWrappedSpec, ranking_policy: { prefer: ['transition_spacing'] } },
    { ...validWrappedSpec, ranking_policy: { prefer: ['condition_number'] } },
    { ...validWrappedSpec, ranking_policy: { prefer: ['sampled_robustness'] } },
    { ...validWrappedSpec, audit_policy: { unsupported: 'warn' } },
    {
      ...validWrappedSpec,
      constraints: {
        ...validWrappedSpec.constraints,
        transitions: { hard: true },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        behavior_spec: {
          ...validWrappedSpec.target.behavior_spec,
          input_window: { min_spacing_decades: 0.5 },
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        ...validWrappedSpec.target,
        shape: {
          class: 'monotonic',
          monotonicity: 'any',
          min_prominence_log10: 0.5,
          sample_points: 51,
          tolerance_log10: 0.01,
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        ...validWrappedSpec.target,
        shape: {
          class: 'bell_shaped',
          min_prominence_log10: 0.5,
          min_prominence_decades: 1.0,
          sample_points: 51,
          tolerance_log10: 0.01,
        },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        ...validWrappedSpec.target,
        input_window: { input_log10: [-10, -9], hard: false },
      },
    },
    {
      ...validWrappedSpec,
      target: {
        ...validWrappedSpec.target,
        temporal_dynamics: {
          peak_width_seconds: { min: 3, max: 1 },
          hard: false,
        },
      },
    },
  ];

  for (const spec of invalidSpecs) {
    assert.equal(normalizeAgentDesignabilitySpec(spec), null);
    assert.deepEqual(cardsWithAgentSpec({
      cards: [{ rules: ['A + A <-> C_A_A'], designability_spec: spec }],
    }), []);
  }

  const softUnsupportedRobustnessSpec = {
    ...validWrappedSpec,
    constraints: {
      ...validWrappedSpec.constraints,
      robustness: {
        condition_number_max: 100,
        min_sampled_pass_fraction: 0.8,
        hard: false,
      },
    },
  };
  assert.deepEqual(
    normalizeAgentDesignabilitySpec(softUnsupportedRobustnessSpec)?.constraints.robustness,
    {
      condition_number_max: 100,
      min_sampled_pass_fraction: 0.8,
      hard: false,
    },
  );

  const softUnsupportedTargetSpec = {
    ...validWrappedSpec,
    target: {
      legacy_target: {
        target_kind: 'exact',
        target: [1],
      },
      input_window: {
        input_log10: [-2, 2],
        hard: false,
      },
      temporal_dynamics: {
        peak_width_seconds: { min: 1 },
        hard: false,
      },
    },
  };
  assert.deepEqual(
    normalizeAgentDesignabilitySpec(softUnsupportedTargetSpec)?.target,
    softUnsupportedTargetSpec.target,
  );

  const minimalOnlySpec = {
    ...validWrappedSpec,
    constraints: { network: { max_reactions: 4 } },
    candidate_budget: { max_exact_placements: 0 },
    ranking_policy: { prefer: ['complexity'] },
  };
  assert.deepEqual(
    normalizeAgentDesignabilitySpec(minimalOnlySpec)?.constraints,
    { network: { max_reactions: 4 } },
  );
});

test('Agent nested BehaviorSpec lowering rejects explicit empty kd_profile objects', () => {
  const nested = {
    schema_version: 'bne-behavior/v0.1.0',
    behavior_spec: {
      feature_space: 'reaction_order',
      input: 'tA',
      output: 'C_A_A',
      program: [{ kind: 'reaction_order', operator: '=', value: 1 }],
    },
    network_constraints: {
      kd_profile: {},
    },
  };

  assert.equal(normalizeAgentDesignabilitySpec(nested), null);
});

test('Agent response cards with explicit empty spec payloads are not exportable candidates', () => {
  for (const payload of [{}, null]) {
    const cards = cardsWithAgentSpec({
      cards: [
        {
          rules: ['A + A <-> C_A_A'],
          compiled_spec: payload,
        },
      ],
    });

    assert.deepEqual(cards, []);
  }
});

test('Agent BehaviorSpec export refuses invalid I/O instead of string coercion', () => {
  const nested = {
    schema_version: 'bne-behavior/v0.1.0',
    behavior_spec: {
      feature_space: 'reaction_order',
      input: true,
      output: ['C_A_A'],
      program: [
        { kind: 'reaction_order', operator: '=', value: 1 },
      ],
    },
  };
  const spec = normalizeAgentDesignabilitySpec(nested);

  assert.equal(spec, null);
});

test('Agent BehaviorSpec export refuses loose numeric program values', () => {
  for (const value of [true, false, null, '', [], '1']) {
    const nested = {
      schema_version: 'bne-behavior/v0.1.0',
      behavior_spec: {
        feature_space: 'reaction_order',
        input: 'tA',
        output: 'C_A_A',
        program: [
          { kind: 'reaction_order', operator: '=', value },
        ],
      },
    };

    assert.equal(normalizeAgentDesignabilitySpec(nested), null);
  }
});

test('Agent BehaviorSpec export refuses unsupported operators instead of rewriting to equality', () => {
  for (const operator of ['>=', null, '', false, 0]) {
    const nested = {
      schema_version: 'bne-behavior/v0.1.0',
      behavior_spec: {
        feature_space: 'reaction_order',
        input: 'tA',
        output: 'C_A_A',
        program: [
          { kind: 'reaction_order', operator, value: 1 },
        ],
      },
    };

    assert.equal(normalizeAgentDesignabilitySpec(nested), null);
  }
});

test('Agent BehaviorSpec export refuses lossy nested clause lowering', () => {
  const cases = [
    {
      behavior_spec: {
        at: 0.5,
      },
    },
    {
      behavior_spec: {
        program: [
          { kind: 'reaction_order', operator: '=', value: 1, at: 0.5 },
        ],
      },
    },
    { behavior_spec: { input_window: null } },
    { behavior_spec: { input_window: { input_log10: [-2] } } },
    { shape_preferences: { dynamic_range_log10: { max: 2 } } },
    { shape_preferences: { dynamic_range_log10: { range: [0, 2] } } },
    { shape_preferences: { rise_slope: { min: 0.5 } } },
  ];
  for (const patch of cases) {
    const nested = {
      schema_version: 'bne-behavior/v0.1.0',
      behavior_spec: {
        feature_space: 'reaction_order',
        input: 'tA',
        output: 'C_A_A',
        program: [
          { kind: 'reaction_order', operator: '=', value: 1 },
        ],
        ...(patch.behavior_spec || {}),
      },
      ...(patch.shape_preferences ? { shape_preferences: patch.shape_preferences } : {}),
    };

    assert.equal(normalizeAgentDesignabilitySpec(nested), null);
  }
});

test('Agent BehaviorSpec export refuses loose numeric constraint fields', () => {
  const cases = [
    { network_constraints: { max_reactions: '4.9' } },
    { network_constraints: { max_base_species: true } },
    { network_constraints: { kd_profile: { log10_kd_min: '-2', log10_kd_max: 2 } } },
    { shape_preferences: { dynamic_range_log10: { min: false } } },
  ];
  for (const extra of cases) {
    const nested = {
      schema_version: 'bne-behavior/v0.1.0',
      behavior_spec: {
        feature_space: 'reaction_order',
        input: 'tA',
        output: 'C_A_A',
        program: [
          { kind: 'reaction_order', operator: '=', value: 1 },
        ],
      },
      ...extra,
    };

    assert.equal(normalizeAgentDesignabilitySpec(nested), null);
  }
});

test('Agent DesignabilitySpec normalization forces agent_design source kind', () => {
  const raw = {
    schema_version: 'bne-designability/v1.0.0',
    source: { kind: 'hand_authored', node_id: 'manual-config' },
    target: {
      behavior_spec: {
        input: 'tA',
        output: 'C_A_A',
        program: [{ kind: 'reaction_order', operator: '=', value: 1 }],
      },
    },
  };
  const spec = normalizeAgentDesignabilitySpec(raw);

  assert.equal(spec.source.kind, 'agent_design');
  assert.equal(spec.source.node_id, 'manual-config');
});

test('Agent response cards cannot inherit stale DesignabilitySpec from chat state', () => {
  const source = fs.readFileSync(path.join(repoRoot, 'webapp/public/js/agent-view.js'), 'utf8');

  assert.doesNotMatch(source, /chatState\?\.spec/);
});

await Promise.all(pendingTests);

console.log(`\nAll ${passed} design spec node contract tests passed.`);

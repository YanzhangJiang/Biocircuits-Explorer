import assert from 'node:assert/strict';
import { isRestoredConnectionValid } from '../public/js/connection-validation.js';

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

const nodeTypes = {
  source: { outputs: [{ port: 'reactions', type: 'NetworkIR', label: 'Reactions' }], inputs: [] },
  builder: { inputs: [{ port: 'reactions', type: 'NetworkIR', label: 'Reactions' }], outputs: [{ port: 'model', type: 'ModelArtifact', label: 'Model' }] },
  viewer: { inputs: [{ port: 'model', type: 'ModelArtifact', label: 'Model' }], outputs: [] },
  ropReference: {
    inputs: [],
    outputs: [{ port: 'rop-shape-reference', type: 'ROPShapeReferenceArtifact', label: 'ROP Shape Reference' }],
  },
  ropEdit: {
    inputs: [{ port: 'rop-shape-reference', type: 'ROPShapeReferenceArtifact', label: 'ROP Shape Reference' }],
    outputs: [{ port: 'rop-shape-request', type: 'ROPShapeRequestArtifact', label: 'ROP Shape Request' }],
  },
  ropOptimizer: {
    inputs: [{ port: 'rop-shape-request', type: 'ROPShapeRequestArtifact', label: 'ROP Shape Request' }],
    outputs: [{ port: 'rop-shape-result', type: 'ROPShapeResultArtifact', label: 'ROP Shape Result' }],
  },
  ropResult: {
    inputs: [{ port: 'rop-shape-result', type: 'ROPShapeResultArtifact', label: 'ROP Shape Result' }],
    outputs: [],
  },
  legacyPathSource: {
    inputs: [],
    outputs: [{ port: 'result', type: 'PathResult', label: 'Path' }],
  },
  legacyPathResult: {
    inputs: [{ port: 'result', type: 'PathResult', label: 'Path' }],
    outputs: [],
  },
};

test('restored connections must use declared output and input ports', () => {
  assert.equal(isRestoredConnectionValid(
    { fromPort: 'reactions', toPort: 'reactions' },
    'source',
    'builder',
    nodeTypes,
  ), true);
  assert.equal(isRestoredConnectionValid(
    { fromPort: 'model', toPort: 'reactions' },
    'source',
    'builder',
    nodeTypes,
  ), false);
  assert.equal(isRestoredConnectionValid(
    { fromPort: 'reactions', toPort: 'params' },
    'source',
    'builder',
    nodeTypes,
  ), false);
});

test('restored connections must keep compatible port artifact types', () => {
  assert.equal(isRestoredConnectionValid(
    { fromPort: 'reactions', toPort: 'model' },
    'source',
    'viewer',
    nodeTypes,
  ), false);
});

test('ROP shape restored connections use exact reference/request/result labels and types', () => {
  assert.deepEqual(nodeTypes.ropReference.outputs[0], {
    port: 'rop-shape-reference', type: 'ROPShapeReferenceArtifact', label: 'ROP Shape Reference',
  });
  assert.deepEqual(nodeTypes.ropEdit.outputs[0], {
    port: 'rop-shape-request', type: 'ROPShapeRequestArtifact', label: 'ROP Shape Request',
  });
  assert.deepEqual(nodeTypes.ropOptimizer.outputs[0], {
    port: 'rop-shape-result', type: 'ROPShapeResultArtifact', label: 'ROP Shape Result',
  });
  assert.equal(isRestoredConnectionValid(
    { fromPort: 'rop-shape-reference', toPort: 'rop-shape-reference' },
    'ropReference', 'ropEdit', nodeTypes,
  ), true);
  assert.equal(isRestoredConnectionValid(
    { fromPort: 'rop-shape-request', toPort: 'rop-shape-request' },
    'ropEdit', 'ropOptimizer', nodeTypes,
  ), true);
  assert.equal(isRestoredConnectionValid(
    { fromPort: 'rop-shape-result', toPort: 'rop-shape-result' },
    'ropOptimizer', 'ropResult', nodeTypes,
  ), true);
});

test('ROP shape artifact stages reject every implicit cross-stage or legacy connection', () => {
  const producers = {
    'rop-shape-reference': 'ropReference',
    'rop-shape-request': 'ropEdit',
    'rop-shape-result': 'ropOptimizer',
    result: 'legacyPathSource',
  };
  const consumers = {
    'rop-shape-reference': 'ropEdit',
    'rop-shape-request': 'ropOptimizer',
    'rop-shape-result': 'ropResult',
    result: 'legacyPathResult',
  };
  for (const [fromPort, fromType] of Object.entries(producers)) {
    for (const [toPort, toType] of Object.entries(consumers)) {
      if (fromPort === toPort) continue;
      assert.equal(
        isRestoredConnectionValid({ fromPort, toPort }, fromType, toType, nodeTypes),
        false,
        `${fromPort} must not connect to ${toPort}`,
      );
    }
  }
});

console.log(`\nAll ${passed} connection validation tests passed.`);

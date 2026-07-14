import assert from 'node:assert/strict';

import {
  PORT_TYPES,
  portTypeOf,
  portsCompatible,
  resolveNodePort,
} from '../public/js/port-types.js';
import {
  validateNodeConnection,
} from '../public/js/connection-validation.js';

globalThis.window = {
  matchMedia: () => null,
  addEventListener() {},
  location: { protocol: 'http:', hostname: '127.0.0.1', port: '8000' },
  sessionStorage: { getItem() { return null; }, setItem() {} },
};
globalThis.document = {
  readyState: 'loading',
  documentElement: { dataset: {}, style: { setProperty() {} } },
  getElementById() { return null; },
  addEventListener() {},
  querySelectorAll() { return []; },
};
globalThis.HTMLSelectElement = class HTMLSelectElement {};

const { NODE_TYPES } = await import('../public/js/node-types/index.js');

const CONFIG_FAMILIES = Object.freeze([
  ['siso-params', 'siso-result', PORT_TYPES.SISOConfig],
  ['scan-1d-params', 'scan-1d-result', PORT_TYPES.Scan1DConfig],
  ['scan-2d-params', 'scan-2d-result', PORT_TYPES.Scan2DConfig],
  ['rop-cloud-params', 'rop-cloud-result', PORT_TYPES.ROPCloudConfig],
  ['fret-params', 'fret-result', PORT_TYPES.FRETConfig],
  ['rop-poly-params', 'rop-poly-result', PORT_TYPES.ROPPolyhedronConfig],
  ['placer-params', 'placer-result', PORT_TYPES.ParameterPlacerConfig],
]);

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

test('ParamsConfig is removed instead of becoming a universal compatibility alias', () => {
  assert.equal(PORT_TYPES.ParamsConfig, undefined);
  assert.equal(portTypeOf('params'), null);
  assert.equal(portsCompatible('params', 'params'), false);
});

test('all seven config families declare an exact type on their params sockets', () => {
  for (const [producerType, consumerType, expectedType] of CONFIG_FAMILIES) {
    assert.ok(expectedType, `${producerType} must use a registered strict type`);
    assert.deepEqual(
      resolveNodePort(NODE_TYPES, producerType, 'output', 'params'),
      { nodeType: producerType, direction: 'output', port: 'params', type: expectedType },
    );
    assert.deepEqual(
      resolveNodePort(NODE_TYPES, consumerType, 'input', 'params'),
      { nodeType: consumerType, direction: 'input', port: 'params', type: expectedType },
    );
  }
});

test('every declared node socket has an explicit registered artifact type', () => {
  for (const [nodeType, definition] of Object.entries(NODE_TYPES)) {
    for (const [key, direction] of [['inputs', 'input'], ['outputs', 'output']]) {
      for (const declaration of definition[key] || []) {
        assert.equal(typeof declaration.type, 'string', `${nodeType}.${direction}.${declaration.port}`);
        assert.ok(
          Object.values(PORT_TYPES).includes(declaration.type),
          `${nodeType}.${direction}.${declaration.port} has unknown type ${String(declaration.type)}`,
        );
        assert.ok(resolveNodePort(NODE_TYPES, nodeType, direction, declaration.port));
      }
    }
  }
});

test('the 7x7 compatibility matrix accepts only the seven same-family pairs', () => {
  const registry = {};
  CONFIG_FAMILIES.forEach(([producerType, consumerType], index) => {
    registry[`producer-${index}`] = { type: producerType };
    registry[`consumer-${index}`] = { type: consumerType };
  });

  let accepted = 0;
  let rejected = 0;
  for (let output = 0; output < CONFIG_FAMILIES.length; output += 1) {
    for (let input = 0; input < CONFIG_FAMILIES.length; input += 1) {
      const result = validateNodeConnection({
        fromNode: `producer-${output}`,
        fromPort: 'params',
        toNode: `consumer-${input}`,
        toPort: 'params',
      }, registry, NODE_TYPES);
      assert.equal(result.ok, output === input, result.message);
      if (result.ok) accepted += 1;
      else rejected += 1;
    }
  }
  assert.equal(accepted, 7);
  assert.equal(rejected, 42);
});

test('unknown node, direction, port and misspelled declaration all fail closed', () => {
  assert.equal(resolveNodePort(NODE_TYPES, 'not-a-node', 'output', 'params'), null);
  assert.equal(resolveNodePort(NODE_TYPES, 'siso-params', 'sideways', 'params'), null);
  assert.equal(resolveNodePort(NODE_TYPES, 'siso-params', 'output', 'paramz'), null);

  const misspelled = {
    producer: { inputs: [], outputs: [{ port: 'params', type: 'SIS0Config' }] },
  };
  assert.equal(resolveNodePort(misspelled, 'producer', 'output', 'params'), null);
  assert.equal(portTypeOf('mystery'), null);
  assert.equal(portsCompatible('mystery', 'mystery'), false);
});

console.log(`\nAll ${passed} strict config-port contract tests passed.`);

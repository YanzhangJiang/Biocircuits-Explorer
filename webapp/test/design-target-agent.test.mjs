import assert from 'node:assert/strict';

const persisted = new Map();
globalThis.localStorage = {
  getItem: key => persisted.get(key) ?? null,
  setItem: (key, value) => persisted.set(key, String(value)),
  removeItem: key => persisted.delete(key),
};
Object.defineProperty(globalThis, 'document', {
  configurable: true,
  get() { throw new Error('Target transport must not import or mount a DOM surface.'); },
});
const { compileDesignTarget } = await import('../public/js/design-target-agent.js');
delete globalThis.document;
globalThis.window = { matchMedia: () => null, addEventListener() {}, location: { protocol: 'http:', hostname: '127.0.0.1', port: '18088' } };
globalThis.document = { readyState: 'loading', documentElement: { dataset: {}, style: { setProperty() {} } }, getElementById() { return null; }, addEventListener() {}, querySelectorAll: () => [] };

const { setDesignChatEndpoint } = await import('../public/js/agent-view.js');
const { setLLMConfig } = await import('../public/js/llm-settings.js');
const target = {
  schema_version: 'bne-design-target/v1.0.0', source: 'agent', description: 'monotone increasing',
  inputs: [{ name: 'X', min: .05, max: 10, scale: 'log' }],
  outputs: [{ name: 'response', species: 'A', transform: 'linear', offset: 0, optimize_offset: false }],
  samples: [{ inputs: [.05], outputs: [.1], weight: 1 }],
};
const result = { target, interpretation: 'An increasing response.', warnings: ['Edit numerical assumptions.'] };
let observed;
const fetchImpl = async (url, options) => {
  observed = { url, options };
  return { ok: true, json: async () => result };
};
setDesignChatEndpoint('http://127.0.0.1:19876/design-chat');
setLLMConfig({ provider: 'anthropic', apiKey: 'transient-llm-key', model: 'test' });
const controller = new AbortController();
assert.deepEqual(await compileDesignTarget('  monotone increasing ', { target, signal: controller.signal, fetchImpl }), result);
assert.equal(observed.url, 'http://127.0.0.1:19876/compile-target');
assert.equal(observed.options.headers.Authorization, undefined);
assert.equal(observed.options.signal, controller.signal);
const request = JSON.parse(observed.options.body);
assert.equal(request.message, 'monotone increasing');
assert.deepEqual(request.target, target);
assert.equal(request.llm.apiKey, 'transient-llm-key');
assert.ok([...persisted.values()].every(value => !value.includes('transient-llm-key')));

setDesignChatEndpoint('http://127.0.0.1:19877/design-chat');
await compileDesignTarget('bandpass', { fetchImpl });
assert.equal(observed.url, 'http://127.0.0.1:19877/compile-target');
assert.equal('target' in JSON.parse(observed.options.body), false);

await assert.rejects(compileDesignTarget('', { fetchImpl }), /Describe the target/);
controller.abort();
await assert.rejects(compileDesignTarget('increasing', { fetchImpl, signal: controller.signal }), { name: 'AbortError' });
const late = new AbortController();
await assert.rejects(compileDesignTarget('increasing', {
  signal: late.signal,
  fetchImpl: async () => { late.abort(); return { ok: true, json: async () => result }; },
}), { name: 'AbortError' });
await assert.rejects(compileDesignTarget('unknown', {
  fetchImpl: async () => ({ ok: false, json: async () => ({ error: 'Specify the input and output roles.', code: 'cannot_compile_target' }) }),
}), { message: 'Specify the input and output roles.', code: 'cannot_compile_target' });
await assert.rejects(compileDesignTarget('unknown', {
  fetchImpl: async () => ({ ok: true, json: async () => ({ target: {} }) }),
}), /invalid target/);

console.log('Design target Agent transport tests passed.');

import assert from 'node:assert/strict';

const persisted = new Map();
globalThis.localStorage = {
  getItem(key) { return persisted.get(key) ?? null; },
  setItem(key, value) { persisted.set(key, String(value)); },
};
globalThis.window = {
  matchMedia: () => null,
  addEventListener() {},
  location: { protocol: 'http:', hostname: '127.0.0.1', port: '18088' },
};
globalThis.document = {
  readyState: 'loading',
  documentElement: { dataset: {}, style: { setProperty() {} } },
  getElementById() { return null; },
  addEventListener() {},
  querySelectorAll() { return []; },
};

const {
  designChatRequestHeaders,
  setDesignChatEndpoint,
} = await import('../public/js/agent-view.js');

assert.deepEqual(designChatRequestHeaders(), {});
assert.deepEqual(designChatRequestHeaders({ json: true }), {
  'Content-Type': 'application/json',
});

const endpoint = 'http://127.0.0.1:8765/design-chat';
const firstToken = 'a'.repeat(64);
setDesignChatEndpoint(endpoint, firstToken);
assert.deepEqual(designChatRequestHeaders(), {
  Authorization: `Bearer ${firstToken}`,
});
assert.deepEqual(designChatRequestHeaders({ json: true }), {
  'Content-Type': 'application/json',
  Authorization: `Bearer ${firstToken}`,
});
assert.equal(persisted.get('bcx-chat-api'), endpoint);
assert.equal([...persisted.values()].includes(firstToken), false, 'bearer token must stay memory-only');

const rotatedToken = 'b'.repeat(64);
setDesignChatEndpoint(endpoint, rotatedToken);
assert.equal(designChatRequestHeaders().Authorization, `Bearer ${rotatedToken}`);

setDesignChatEndpoint(endpoint);
assert.deepEqual(designChatRequestHeaders(), {}, 'local development remains token-free');

console.log('Design Chat auth contract tests passed.');

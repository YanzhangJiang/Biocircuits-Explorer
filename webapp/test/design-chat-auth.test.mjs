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

// The loopback helper is Origin-checked; the browser client sends no credentials.
assert.deepEqual(designChatRequestHeaders(), {});
assert.deepEqual(designChatRequestHeaders({ json: true }), {
  'Content-Type': 'application/json',
});

const endpoint = 'http://127.0.0.1:8765/design-chat';
setDesignChatEndpoint(endpoint);
assert.equal(persisted.get('bcx-chat-api'), endpoint);
assert.equal(window.__BCX_CHAT_API__, endpoint);
assert.deepEqual(designChatRequestHeaders(), {});

console.log('Design Chat endpoint contract tests passed.');

import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { webcrypto } from 'node:crypto';

import {
  markdownToSafeTree,
  renderSafeMarkdown,
  resolveMarkdownImageSrc,
  resolveMarkdownLinkHref,
} from '../public/js/safe-markdown.js';

class MemoryStorage {
  constructor(entries = {}) {
    this.values = new Map(Object.entries(entries).map(([key, value]) => [key, String(value)]));
  }
  getItem(key) { return this.values.get(String(key)) ?? null; }
  setItem(key, value) { this.values.set(String(key), String(value)); }
  removeItem(key) { this.values.delete(String(key)); }
  clear() { this.values.clear(); }
}

class FakeNode {
  constructor(ownerDocument) {
    this.ownerDocument = ownerDocument;
    this.children = [];
  }
  appendChild(child) {
    this.children.push(child);
    return child;
  }
  replaceChildren(...children) {
    this.children = children;
  }
}

class FakeText extends FakeNode {
  constructor(value, ownerDocument) {
    super(ownerDocument);
    this.nodeType = 3;
    this.value = String(value);
  }
  get textContent() { return this.value; }
}

class FakeElement extends FakeNode {
  constructor(tagName, ownerDocument) {
    super(ownerDocument);
    this.nodeType = 1;
    this.tagName = String(tagName).toLowerCase();
    this.attributes = new Map();
    this.className = '';
    this.dataset = {};
    this.style = {};
  }
  setAttribute(name, value) { this.attributes.set(String(name), String(value)); }
  getAttribute(name) { return this.attributes.get(String(name)) ?? null; }
  set textContent(value) { this.children = [new FakeText(value, this.ownerDocument)]; }
  get textContent() { return this.children.map(child => child.textContent).join(''); }
}

const fakeDocument = {
  createElement(tagName) { return new FakeElement(tagName, fakeDocument); },
  createTextNode(value) { return new FakeText(value, fakeDocument); },
  getElementById() { return null; },
  querySelector() { return null; },
  querySelectorAll() { return []; },
  documentElement: { dataset: {}, style: { setProperty() {} } },
};

function descendants(node) {
  return [node, ...(node.children || []).flatMap(descendants)];
}

// Markdown is rendered as an allow-listed DOM tree. Quotes remain attribute
// values, raw HTML remains text, and executable URL schemes never become links.
const markdown = [
  '<script>globalThis.markdownPwned = true</script>',
  '',
  '![" onerror="globalThis.imagePwned=true](https://example.com/image.png" onerror="boom)',
  '[run](javascript:globalThis.linkPwned=true)',
  '![svg](data:image/svg+xml,<svg onload=alert(1)>)',
  '[safe](https://example.com/path?q=1)',
  '![local](/tmp/a b.png)',
].join('\n');
const container = new FakeElement('div', fakeDocument);
renderSafeMarkdown(container, markdown);
const rendered = descendants(container);
const elements = rendered.filter(node => node.nodeType === 1);
const images = elements.filter(node => node.tagName === 'img');
const links = elements.filter(node => node.tagName === 'a');

assert.equal(elements.some(node => node.tagName === 'script'), false);
assert.match(container.textContent, /<script>globalThis\.markdownPwned/);
assert.equal(images.length, 2, 'only HTTPS and local raster/image paths should render');
assert.equal(images[0].getAttribute('alt'), '" onerror="globalThis.imagePwned=true');
assert.equal(images[0].getAttribute('onerror'), null, 'quoted alt text must not create an event attribute');
assert.match(images[0].getAttribute('src'), /^https:\/\/example\.com\/image\.png/);
assert.equal(images[1].getAttribute('src'), '/api/v1/local-image?path=%2Ftmp%2Fa%20b.png');
assert.equal(links.length, 1, 'javascript: links must degrade to plain text');
assert.equal(links[0].getAttribute('rel'), 'noopener noreferrer');
assert.equal(links[0].getAttribute('target'), '_blank');
assert.equal(resolveMarkdownLinkHref('javascript:alert(1)'), null);
assert.equal(resolveMarkdownLinkHref('java\tscript:alert(1)'), null);
assert.equal(resolveMarkdownLinkHref('//attacker.example/path'), null);
assert.equal(resolveMarkdownImageSrc('data:image/svg+xml,<svg onload=alert(1)>'), null);
assert.equal(resolveMarkdownImageSrc('data:text/html,<script>alert(1)</script>'), null);
assert.equal(resolveMarkdownImageSrc('data:image/png;base64,iVBORw0KGgo='), 'data:image/png;base64,iVBORw0KGgo=');
assert.ok(markdownToSafeTree('# Heading\n\n- **safe**').every(node => node.type === 'element'));

const noteSource = await readFile(new URL('../public/js/node-types/note.js', import.meta.url), 'utf8');
assert.doesNotMatch(noteSource, /preview\.innerHTML/, 'note preview must not parse user Markdown as HTML');

// Migrate an old persistent LLM key into this module's memory, immediately
// rewrite localStorage without the key, and prove a module reload forgets it.
const localStorage = new MemoryStorage({
  'bcx-llm-cfg': JSON.stringify({
    provider: 'anthropic',
    apiKey: 'legacy-llm-secret',
    baseUrl: 'https://api.anthropic.com',
    model: 'claude-test',
    effort: 'high',
  }),
});
const sessionStorage = new MemoryStorage();
globalThis.localStorage = localStorage;
globalThis.sessionStorage = sessionStorage;
globalThis.document = {
  readyState: 'loading',
  addEventListener() {},
  getElementById() { return null; },
};

const llmModule = await import(`../public/js/llm-settings.js?security=${Date.now()}`);
assert.equal(llmModule.getLLMConfig().apiKey, 'legacy-llm-secret');
assert.deepEqual(JSON.parse(localStorage.getItem('bcx-llm-cfg')), {
  provider: 'anthropic',
  baseUrl: 'https://api.anthropic.com',
  model: 'claude-test',
  effort: 'high',
});

llmModule.setLLMConfig({
  provider: 'openai',
  apiKey: 'runtime-only-secret',
  baseUrl: 'https://api.openai.com/v1',
  model: 'model-test',
  effort: 'medium',
});
assert.equal(llmModule.getLLMConfig().apiKey, 'runtime-only-secret');
assert.equal(localStorage.getItem('bcx-llm-cfg').includes('runtime-only-secret'), false);
assert.equal([...sessionStorage.values.values()].includes('runtime-only-secret'), false);

const reloadedLlmModule = await import(`../public/js/llm-settings.js?reload=${Date.now()}`);
assert.equal(reloadedLlmModule.getLLMConfig().apiKey, '', 'reload must retire the in-memory LLM key');
assert.equal(reloadedLlmModule.getLLMConfig().model, 'model-test');

const writeFailingStorage = new MemoryStorage({
  'bcx-llm-cfg': JSON.stringify({ provider: 'openai', apiKey: 'quota-secret' }),
});
writeFailingStorage.setItem = () => { throw new Error('quota exceeded'); };
globalThis.localStorage = writeFailingStorage;
const writeFailingLlmModule = await import(`../public/js/llm-settings.js?quota=${Date.now()}`);
assert.equal(writeFailingLlmModule.getLLMConfig().apiKey, 'quota-secret');
assert.equal(writeFailingStorage.getItem('bcx-llm-cfg'), null,
  'failed preference rewrite must still delete the legacy persistent key');
globalThis.localStorage = localStorage;

// Cognito migration and callback tokens are session-scoped. The unused access
// token is discarded, and an external post-login return target fails closed.
const authPrefix = 'biocircuits-explorer.auth.';
const encodedPayload = Buffer.from(JSON.stringify({
  sub: 'user-1',
  email: 'user@example.test',
  email_verified: true,
  exp: Math.floor(Date.now() / 1000) + 3600,
})).toString('base64url');
const legacyIdToken = `header.${encodedPayload}.signature`;
localStorage.setItem(`${authPrefix}id_token`, legacyIdToken);
localStorage.setItem(`${authPrefix}access_token`, 'legacy-access-secret');
localStorage.setItem(`${authPrefix}refresh_token`, 'legacy-refresh-secret');
localStorage.setItem(`${authPrefix}expires_at`, String(Date.now() + 3600_000));

const assignedLocations = [];
const location = {
  origin: 'http://127.0.0.1:18088',
  protocol: 'http:',
  hostname: '127.0.0.1',
  port: '18088',
  pathname: '/index-node.html',
  search: '?project=test',
  hash: '#agent',
  assign(value) { assignedLocations.push(String(value)); },
};
if (!globalThis.crypto) {
  Object.defineProperty(globalThis, 'crypto', { value: webcrypto, configurable: true });
}
globalThis.window = {
  localStorage,
  sessionStorage,
  location,
  matchMedia: () => null,
};

const callbackIdToken = `header.${encodedPayload}.callback`;
const fetchCalls = [];
globalThis.fetch = async (url, options = {}) => {
  fetchCalls.push({ url: String(url), options });
  if (String(url).endsWith('/api/v1/auth/config')) {
    return {
      ok: true,
      status: 200,
      json: async () => ({
        enabled: true,
        cognito_domain: 'login.example.test',
        cognito_app_client_id: 'public-client',
        scopes: ['openid', 'email'],
      }),
    };
  }
  if (String(url) === 'https://login.example.test/oauth2/token') {
    return {
      ok: true,
      status: 200,
      json: async () => ({
        id_token: callbackIdToken,
        access_token: 'callback-access-secret',
        refresh_token: 'callback-refresh-secret',
        expires_in: 3600,
      }),
    };
  }
  throw new Error(`unexpected fetch: ${url}`);
};

const authModule = await import(`../public/js/auth.js?security=${Date.now()}`);
for (const suffix of ['id_token', 'access_token', 'refresh_token', 'expires_at']) {
  assert.equal(localStorage.getItem(`${authPrefix}${suffix}`), null, `${suffix} must leave localStorage`);
}
assert.equal(sessionStorage.getItem(`${authPrefix}id_token`), legacyIdToken);
assert.equal(sessionStorage.getItem(`${authPrefix}refresh_token`), 'legacy-refresh-secret');
assert.equal(sessionStorage.getItem(`${authPrefix}access_token`), null);
assert.equal(authModule.isAuthenticated(), true);
assert.equal(authModule.getCurrentUser()?.email, 'user@example.test');

sessionStorage.setItem(`${authPrefix}pkce_state`, 'expected-state');
sessionStorage.setItem(`${authPrefix}pkce_verifier`, 'expected-verifier');
sessionStorage.setItem(`${authPrefix}post_login_return`, 'https://attacker.example/steal');
const returnPath = await authModule.handleCallback(new URLSearchParams({
  code: 'authorization-code',
  state: 'expected-state',
}));
assert.equal(returnPath, '/index-node.html', 'OAuth callback must not become an open redirect');
assert.equal(sessionStorage.getItem(`${authPrefix}id_token`), callbackIdToken);
assert.equal(sessionStorage.getItem(`${authPrefix}refresh_token`), 'callback-refresh-secret');
assert.equal(sessionStorage.getItem(`${authPrefix}access_token`), null);
assert.equal(sessionStorage.getItem(`${authPrefix}pkce_state`), null);
assert.equal(sessionStorage.getItem(`${authPrefix}pkce_verifier`), null);
assert.equal(sessionStorage.getItem(`${authPrefix}post_login_return`), null);
assert.equal(await authModule.getIdToken(), callbackIdToken);
assert.equal([...localStorage.values.values()].some(value => value.includes('callback-')), false);

await authModule.signIn({ returnTo: 'https://attacker.example/after-login' });
assert.equal(sessionStorage.getItem(`${authPrefix}post_login_return`), '/index-node.html');
assert.match(assignedLocations.at(-1), /^https:\/\/login\.example\.test\/oauth2\/authorize\?/);
assert.equal(fetchCalls.filter(call => call.url.endsWith('/api/v1/auth/config')).length, 1);

// Server error bodies flow into Error.message. Rendering them must use a text
// node, because the same code runs inside the privileged local WKWebView.
globalThis.document = fakeDocument;
globalThis.ResizeObserver = class ResizeObserver {
  observe() {}
  disconnect() {}
};
globalThis.CustomEvent = class CustomEvent {
  constructor(type, options = {}) { this.type = type; this.detail = options.detail; }
};
const { renderNodeError } = await import(`../public/js/api.js?security=${Date.now()}`);
const errorHost = new FakeElement('div', fakeDocument);
const hostileError = '<img src=x onerror="globalThis.backendErrorPwned=true">';
renderNodeError(errorHost, new Error(hostileError));
assert.equal(errorHost.textContent, hostileError);
assert.equal(descendants(errorHost).some(node => node.tagName === 'img'), false);

// Saved workspaces can carry historical SISO and Atlas response payloads.
// The restore path renders those payloads without contacting the backend, so
// every persisted scalar used in generated markup is treated as plain text.
const sisoModule = await import(`../public/js/siso.js?security=${Date.now()}`);
const sisoPayload = '<img src=x onerror="globalThis.savedWorkspacePwned=true">';
const sisoHtml = sisoModule.renderBehaviorFamiliesResult('node-1', `q" autofocus onfocus="pwn`, {
  change_qK: sisoPayload,
  observe_x: sisoPayload,
  included_paths: sisoPayload,
  exact_families: [{
    family_idx: sisoPayload,
    exact_label: sisoPayload,
    path_indices: [`1" autofocus onfocus="pwn`],
  }],
  paths: [{
    path_idx: `1" autofocus onfocus="pwn`,
    feasible: true,
    included: false,
    exclusion_reason: sisoPayload,
    perms: [[`</div><script>globalThis.savedWorkspacePwned=true</script>`]],
    exact_label: sisoPayload,
  }],
});
assert.doesNotMatch(sisoHtml, /<script>|<img\b|\sonfocus="pwn"/i);
assert.match(sisoHtml, /&lt;script&gt;|&lt;img/);

const qkHtml = sisoModule.renderQKPolyhedronResult('node-1', {
  path_idx: sisoPayload,
  exact_family_idx: sisoPayload,
  observe_x: sisoPayload,
}, {
  change_qK: sisoPayload,
  qk_symbols: [sisoPayload],
  polyhedra: [{
    dimension: sisoPayload,
    is_bounded: false,
    A: [[1]],
    b: [sisoPayload],
    vertices: [[sisoPayload]],
    rays: [],
  }],
}).html;
assert.doesNotMatch(qkHtml, /<img\b|<script>/i);
assert.match(qkHtml, /&lt;img/);

const atlasModule = await import(`../public/js/atlas.js?security=${Date.now()}`);
const atlasBuilderHtml = atlasModule.renderAtlasBuilderResult({
  generated_at: sisoPayload,
  input_network_count: sisoPayload,
  unique_network_count: 0,
  successful_network_count: 0,
  deduplicated_network_count: 0,
  skipped_existing_slice_count: sisoPayload,
  skipped_existing_network_count: 0,
  network_entries: [],
});
assert.doesNotMatch(atlasBuilderHtml, /<img\b|<script>/i);
assert.match(atlasBuilderHtml, /&lt;img/);

const atlasQueryHtml = atlasModule.renderAtlasQueryResult({
  result_count: sisoPayload,
  result_unit: 'slice',
  query: { limit: sisoPayload },
  results: [{
    rank: sisoPayload,
    source_label: 'candidate',
    base_species_count: sisoPayload,
    reaction_count: sisoPayload,
    max_support: sisoPayload,
    support_mass: sisoPayload,
  }],
});
assert.doesNotMatch(atlasQueryHtml, /<img\b|<script>/i);
assert.match(atlasQueryHtml, /&lt;img/);

// Incremental debug-log rendering uses insertAdjacentHTML; both attribute
// fields and the visible log line must therefore be escaped.
const debugModule = await import(`../public/js/debug-console.js?security=${Date.now()}`);
const debugHtml = debugModule.renderDebugEntry({
  level: `INFO" onmouseover="pwn`,
  seq: `1" autofocus onfocus="pwn`,
  message: `</pre><img src=x onerror="pwn">`,
});
assert.doesNotMatch(debugHtml, /"\s+(?:onmouseover|autofocus|onfocus)=|<img\b/i);
assert.match(debugHtml, /&quot;|&lt;img/);

const auditedSources = await Promise.all([
  'scan.js', 'regime-graph.js', 'rop-cloud.js', 'siso.js',
  'node-types/rop-cloud.js', 'node-types/result.js', 'node-types/siso.js',
].map(path => readFile(new URL(`../public/js/${path}`, import.meta.url), 'utf8')));
for (const source of auditedSources) {
  assert.doesNotMatch(source, /innerHTML\s*=\s*`[^`]*\$\{(?:e|err|error)\??\.message\}/s,
    'backend/provider Error.message must not be interpolated into innerHTML');
}

console.log('Browser security contract tests passed.');

import assert from 'node:assert/strict';

const elementsById = new Map();

class FakeElement {
  constructor(tagName = 'div') {
    this.tagName = String(tagName).toUpperCase();
    this.style = {};
    this.dataset = {};
    this.attributes = new Map();
    this.children = [];
    this.parentNode = null;
    this.parentElement = null;
    this.listeners = new Map();
    this.disabled = false;
    this.value = '';
    this.placeholder = '';
    this.scrollTop = 0;
    this.clientWidth = 1200;
    this.isConnected = false;
    this._textContent = '';
    this._innerHTML = '';
    this._classes = new Set();
    this._id = '';
    this.classList = {
      add: (...names) => names.forEach(name => this._classes.add(String(name))),
      remove: (...names) => names.forEach(name => this._classes.delete(String(name))),
      toggle: (name, force) => {
        const text = String(name);
        const enabled = force === undefined ? !this._classes.has(text) : Boolean(force);
        if (enabled) this._classes.add(text);
        else this._classes.delete(text);
        return enabled;
      },
      contains: name => this._classes.has(String(name)),
    };
  }

  set id(value) {
    if (this._id) elementsById.delete(this._id);
    this._id = String(value);
    if (this._id) elementsById.set(this._id, this);
  }

  get id() { return this._id; }

  set className(value) {
    this._classes = new Set(String(value).split(/\s+/).filter(Boolean));
  }

  get className() { return [...this._classes].join(' '); }

  set textContent(value) {
    for (const child of [...this.children]) this._detachChild(child);
    this._textContent = String(value ?? '');
    this._innerHTML = '';
  }

  get textContent() {
    return this._textContent + this.children.map(child => child.textContent).join('');
  }

  set innerHTML(value) {
    for (const child of [...this.children]) this._detachChild(child);
    this._innerHTML = String(value ?? '');
    this._textContent = '';
  }

  get innerHTML() { return this._innerHTML; }

  get scrollHeight() { return Math.max(1, this.children.length) * 20; }

  _setConnected(connected) {
    this.isConnected = connected;
    this.children.forEach(child => child._setConnected(connected));
  }

  appendChild(child) {
    if (child.parentNode) child.parentNode._detachChild(child);
    child.parentNode = this;
    child.parentElement = this;
    this.children.push(child);
    child._setConnected(this.isConnected);
    return child;
  }

  _detachChild(child) {
    const index = this.children.indexOf(child);
    if (index >= 0) this.children.splice(index, 1);
    child.parentNode = null;
    child.parentElement = null;
    child._setConnected(false);
  }

  insertBefore(child, reference) {
    if (reference == null) return this.appendChild(child);
    const index = this.children.indexOf(reference);
    if (index < 0) throw new Error('insertBefore reference is not a child');
    if (child.parentNode) child.parentNode._detachChild(child);
    child.parentNode = this;
    child.parentElement = this;
    this.children.splice(index, 0, child);
    child._setConnected(this.isConnected);
    return child;
  }

  replaceChildren(...children) {
    for (const child of [...this.children]) this._detachChild(child);
    this._textContent = '';
    this._innerHTML = '';
    children.forEach(child => this.appendChild(child));
  }

  replaceChild(replacement, previous) {
    const index = this.children.indexOf(previous);
    if (index < 0) throw new Error('replaceChild target is not a child');
    if (replacement.parentNode) replacement.parentNode._detachChild(replacement);
    previous.parentNode = null;
    previous.parentElement = null;
    previous._setConnected(false);
    replacement.parentNode = this;
    replacement.parentElement = this;
    this.children[index] = replacement;
    replacement._setConnected(this.isConnected);
    return previous;
  }

  setAttribute(name, value) {
    const key = String(name);
    this.attributes.set(key, String(value));
    if (key === 'id') this.id = value;
    if (key === 'disabled') this.disabled = true;
    if (key === 'placeholder') this.placeholder = String(value);
  }

  getAttribute(name) { return this.attributes.get(String(name)) ?? null; }

  removeAttribute(name) {
    const key = String(name);
    this.attributes.delete(key);
    if (key === 'disabled') this.disabled = false;
  }

  addEventListener(type, listener) {
    const key = String(type);
    const listeners = this.listeners.get(key) || [];
    listeners.push(listener);
    this.listeners.set(key, listeners);
  }

  dispatchEvent(event) {
    event.target ??= this;
    event.currentTarget = this;
    for (const listener of this.listeners.get(event.type, [])) listener(event);
    return true;
  }

  matches(selector) {
    if (selector.startsWith('#')) return this.id === selector.slice(1);
    if (selector.startsWith('.')) {
      return selector.slice(1).split('.').every(name => this.classList.contains(name));
    }
    return this.tagName === selector.toUpperCase();
  }

  querySelectorAll(selector) {
    const matches = [];
    const visit = node => {
      for (const child of node.children) {
        if (child.matches(selector)) matches.push(child);
        visit(child);
      }
    };
    visit(this);
    return matches;
  }

  querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }

  closest(selector) {
    let cursor = this;
    while (cursor) {
      if (cursor.matches(selector)) return cursor;
      cursor = cursor.parentNode;
    }
    return null;
  }

  contains(candidate) {
    if (candidate === this) return true;
    return this.children.some(child => child.contains(candidate));
  }

  getBoundingClientRect() { return { left: 0, top: 0, width: this.clientWidth, height: 600 }; }
}

class FakeTextNode extends FakeElement {
  constructor(text) {
    super('#text');
    this._textContent = String(text);
  }
}

const body = new FakeElement('body');
const head = new FakeElement('head');
const documentElement = new FakeElement('html');
body._setConnected(true);
head._setConnected(true);
documentElement._setConnected(true);

const documentListeners = new Map();
globalThis.document = {
  readyState: 'loading',
  body,
  head,
  documentElement,
  createElement: tagName => new FakeElement(tagName),
  createElementNS: (_namespace, tagName) => new FakeElement(tagName),
  createTextNode: text => new FakeTextNode(text),
  getElementById: id => elementsById.get(String(id)) || null,
  querySelector: selector => body.querySelector(selector),
  querySelectorAll: selector => body.querySelectorAll(selector),
  addEventListener(type, listener) {
    const listeners = documentListeners.get(type) || [];
    listeners.push(listener);
    documentListeners.set(type, listeners);
  },
};

const persisted = new Map();
globalThis.localStorage = {
  getItem: key => persisted.get(key) ?? null,
  setItem: (key, value) => persisted.set(key, String(value)),
  removeItem: key => persisted.delete(key),
};

globalThis.window = {
  matchMedia: () => null,
  location: { protocol: 'http:', hostname: '127.0.0.1', port: '18088' },
  addEventListener() {},
  dispatchEvent() {},
};

const postCalls = [];
globalThis.fetch = (_url, options = {}) => {
  if ((options.method || 'GET') !== 'POST') {
    return Promise.resolve({
      ok: true,
      status: 200,
      json: async () => ({ engine: { ready: true } }),
      text: async () => '',
    });
  }
  let resolve;
  let reject;
  const promise = new Promise((accept, decline) => {
    resolve = bodyValue => accept({
      ok: true,
      status: 200,
      json: async () => bodyValue,
      text: async () => '',
    });
    reject = decline;
  });
  // Deliberately ignore AbortSignal here. The production owner check must also
  // reject transports that deliver a late response after abort.
  postCalls.push({ options, resolve, reject });
  return promise;
};

const unhandled = [];
const onUnhandled = reason => unhandled.push(reason);
process.on('unhandledRejection', onUnhandled);

const {
  getDesignAgentConversation,
  setDesignAgentConversation,
  setNodeView,
} = await import('../public/js/agent-view.js');

setNodeView('agent');

const textarea = body.querySelector('textarea');
const sendButton = body.querySelectorAll('button').find(button => button.textContent === 'Send');
const rulesPanel = () => body.querySelector('.rules-list');
const chartPanel = () => body.querySelector('.chart-wrap');
const exportButton = () => body.querySelectorAll('button')
  .find(button => button.textContent.startsWith('Export to Workspace'));

assert.ok(textarea, 'agent composer textarea should be mounted');
assert.ok(sendButton, 'agent composer send button should be mounted');

function fire(target, type, fields = {}) {
  target.dispatchEvent({
    type,
    preventDefault() {},
    ...fields,
  });
}

function typeMessage(text) {
  textarea.value = text;
  fire(textarea, 'input');
}

async function settle() {
  await new Promise(resolve => setImmediate(resolve));
  await new Promise(resolve => setImmediate(resolve));
}

async function waitForPostCount(count) {
  for (let attempt = 0; attempt < 50 && postCalls.length < count; attempt += 1) {
    await new Promise(resolve => setImmediate(resolve));
  }
  assert.equal(postCalls.length, count, `expected ${count} Design Agent POST requests`);
}

function card(label) {
  return {
    family: 'dose_shape',
    network_id: `${label}-network`,
    dominant_shape: label,
    output_symbol: 'Y',
    n_reactions: 1,
    rules: [`${label} -> Y`],
    kd: [1],
    computed_series: [{ x: 0, y: 0 }, { x: 1, y: 1 }],
  };
}

function reply(label, state, cards = []) {
  return {
    kind: 'agent',
    reply: `${label} reply`,
    family: 'dose_shape',
    state,
    cards,
    info: {},
  };
}

function restoredConversation(label, state, cards = []) {
  const response = reply(label, state, cards);
  return {
    chatState: state,
    convo: [
      { role: 'user', text: `${label} request` },
      { role: 'agent', res: response },
    ],
  };
}

let passed = 0;
async function test(name, fn) {
  await fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

await test('composer is single-flight and commits chat state in sequence', async () => {
  setDesignAgentConversation({ convo: [], chatState: { workspace: 'alpha', turn: 0 } });
  typeMessage('first turn');
  fire(textarea, 'keydown', { key: 'Enter', shiftKey: false });
  await waitForPostCount(1);

  assert.equal(textarea.disabled, true);
  assert.equal(sendButton.disabled, true);
  assert.deepEqual(JSON.parse(postCalls[0].options.body).state, { workspace: 'alpha', turn: 0 });

  // Exercise a programmatic Enter+click re-entry even though the real disabled
  // controls suppress it. It must not send the same predecessor state twice.
  typeMessage('must not run concurrently');
  fire(sendButton, 'click');
  fire(textarea, 'keydown', { key: 'Enter', shiftKey: false });
  await settle();
  assert.equal(postCalls.length, 1);

  postCalls[0].resolve(reply('alpha', { workspace: 'alpha', turn: 1 }));
  await settle();
  assert.equal(textarea.disabled, false);
  assert.deepEqual(getDesignAgentConversation().chatState, { workspace: 'alpha', turn: 1 });
  assert.deepEqual(getDesignAgentConversation().convo.map(entry => entry.role), ['user', 'agent']);
});

await test('restore aborts a pending turn and a late success has no side effects', async () => {
  setDesignAgentConversation(restoredConversation('old-owner', { workspace: 'old' }, [card('old-owner')]));
  typeMessage('slow old turn');
  fire(sendButton, 'click');
  await waitForPostCount(2);
  const oldCall = postCalls[1];

  const replacement = restoredConversation('new-owner', { workspace: 'new', turn: 4 }, [card('new-owner')]);
  setDesignAgentConversation(replacement);
  assert.equal(oldCall.options.signal.aborted, true);
  assert.match(rulesPanel().textContent, /new-owner/);
  assert.doesNotMatch(rulesPanel().textContent, /old-owner/);

  oldCall.resolve(reply('late-old', { workspace: 'old', turn: 99 }, [card('late-old')]));
  await settle();

  assert.deepEqual(getDesignAgentConversation().chatState, { workspace: 'new', turn: 4 });
  assert.equal(getDesignAgentConversation().convo.length, replacement.convo.length);
  assert.match(rulesPanel().textContent, /new-owner/);
  assert.doesNotMatch(rulesPanel().textContent, /late-old/);
  assert.equal(unhandled.length, 0, 'late success must not replace a detached pending node');
});

await test('late failure cannot overwrite a restored workspace', async () => {
  setDesignAgentConversation(restoredConversation('failure-old', { workspace: 'failure-old' }, [card('failure-old')]));
  typeMessage('turn that will fail late');
  fire(sendButton, 'click');
  await waitForPostCount(3);
  const oldCall = postCalls[2];

  setDesignAgentConversation({ convo: [], chatState: { workspace: 'failure-new', turn: 0 } });
  assert.equal(oldCall.options.signal.aborted, true);
  oldCall.reject(new Error('obsolete failure'));
  await settle();

  assert.deepEqual(getDesignAgentConversation().chatState, { workspace: 'failure-new', turn: 0 });
  assert.deepEqual(getDesignAgentConversation().convo, []);
  assert.doesNotMatch(body.textContent + body.innerHTML, /obsolete failure|Backend unreachable/);
  assert.match(rulesPanel().textContent, /Reaction rules appear here/);
  assert.match(chartPanel().textContent, /top candidate’s response curve appears here/);
  assert.equal(exportButton().disabled, true);
  assert.equal(unhandled.length, 0, 'late failure must not touch detached DOM');
});

await test('restore gives the latest agent response sole ownership of results', async () => {
  const first = reply('first-card', { workspace: 'cards' }, [card('first-card')]);
  const noCard = reply('no-card', { workspace: 'cards' });
  const last = reply('last-card', { workspace: 'cards' }, [card('last-card')]);
  setDesignAgentConversation({
    chatState: { workspace: 'cards' },
    convo: [
      { role: 'agent', res: first },
      { role: 'agent', res: noCard },
      { role: 'agent', res: last },
    ],
  });
  assert.match(rulesPanel().textContent, /last-card/);
  assert.doesNotMatch(rulesPanel().textContent, /first-card/);
  assert.equal(exportButton().disabled, false);

  setDesignAgentConversation({
    chatState: { workspace: 'later-no-card' },
    convo: [
      { role: 'agent', res: first },
      { role: 'agent', res: noCard },
    ],
  });
  assert.match(rulesPanel().textContent, /Reaction rules appear here/);
  assert.match(chartPanel().textContent, /top candidate’s response curve appears here/);
  assert.equal(exportButton().disabled, true);

  setDesignAgentConversation({
    chatState: { workspace: 'empty-card-workspace' },
    convo: [{ role: 'agent', res: noCard }],
  });
  assert.match(rulesPanel().textContent, /Reaction rules appear here/);
  assert.match(chartPanel().textContent, /top candidate’s response curve appears here/);
  assert.equal(exportButton().disabled, true);
});

await test('the next turn uses the restored workspace chat state', async () => {
  const restoredState = { workspace: 'fresh-owner', turn: 7, constraints: { max: 3 } };
  setDesignAgentConversation({ convo: [], chatState: restoredState });
  typeMessage('fresh workspace turn');
  fire(sendButton, 'click');
  await waitForPostCount(4);
  assert.deepEqual(JSON.parse(postCalls[3].options.body).state, restoredState);

  postCalls[3].resolve(reply('fresh-owner', { ...restoredState, turn: 8 }));
  await settle();
  assert.deepEqual(getDesignAgentConversation().chatState, { ...restoredState, turn: 8 });
});

process.off('unhandledRejection', onUnhandled);
assert.equal(unhandled.length, 0);
console.log(`Design Agent conversation owner contract tests passed (${passed}).`);

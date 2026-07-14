// Biocircuits Explorer — TEMPORARY LLM key panel.
// ---------------------------------------------------------------------------
// A self-contained, self-mounting floating panel to hold the LLM API key used by the
// NL->behavior_spec compiler (webapp/scripts/llm_compile.py). Deliberately minimal and
// isolated (touches none of agent-view.js / editor-ui.js) so it is easy to replace with a
// proper settings surface later. Supports BOTH an OpenAI-compatible endpoint (base URL +
// Bearer key — e.g. a local reverse proxy) and Anthropic (x-api-key).
//
// Storage: localStorage 'bcx-llm-cfg' contains only non-secret preferences
// ({provider, baseUrl, model, effort}). The API key is held in this module's memory and
// disappears on navigation/reload. A one-time migration removes keys written by older builds.
// getLLMConfig() returns the merged runtime config for agent-view.js to send in a turn.
//
// The key is OPTIONAL: without it the backend's rule-based (keyword) compiler handles
// requests; a key upgrades the NL→spec step to free-form phrasing via llm_compile.py.

const KEY = 'bcx-llm-cfg';
const DEFAULTS = { provider: 'openai', apiKey: '', baseUrl: '', model: '', effort: '' };
const PROVIDERS = new Set(['openai', 'anthropic']);
let memoryApiKey = '';
const PLACEHOLDER = {
  openai:    { base: 'http://localhost:8317/v1  (or https://api.openai.com/v1)', model: 'gpt-5.4-mini' },
  anthropic: { base: 'https://api.anthropic.com (optional)',                     model: 'claude-sonnet-4-6' },
};
// Effort scales differ by provider. OpenAI-compatible (GPT-5.x / codex) takes a `reasoning_effort`
// enum whose top is `xhigh` (this proxy rejects `minimal`). Anthropic has no effort enum — it maps
// to an extended-thinking token budget, so we expose budget tiers up to `max` (ultrathink-style).
const EFFORTS = {
  openai: [['', 'model default'], ['low', 'low'], ['medium', 'medium'], ['high', 'high'], ['xhigh', 'xhigh (max)']],
  anthropic: [['', 'model default'], ['low', 'low (think)'], ['medium', 'medium'], ['high', 'high'], ['max', 'max (ultrathink)']],
};
const EFFORT_NOTE = {
  openai: 'OpenAI: reasoning_effort (low…xhigh).',
  anthropic: 'Anthropic: extended-thinking budget (low…max).',
};

function preferenceStorage() {
  try { return globalThis.localStorage || null; }
  catch { return null; }
}

function normalizedPreferences(value = {}) {
  const provider = PROVIDERS.has(value.provider) ? value.provider : DEFAULTS.provider;
  const allowedEfforts = new Set((EFFORTS[provider] || EFFORTS.openai).map(([effort]) => effort));
  return {
    provider,
    baseUrl: typeof value.baseUrl === 'string' ? value.baseUrl : '',
    model: typeof value.model === 'string' ? value.model : '',
    effort: allowedEfforts.has(value.effort) ? value.effort : '',
  };
}

function persistPreferences(preferences) {
  const storage = preferenceStorage();
  if (!storage) return;
  try { storage.setItem(KEY, JSON.stringify(normalizedPreferences(preferences))); }
  catch { /* unavailable or quota-limited storage leaves preferences in memory only */ }
}

function migrateLegacyConfig() {
  const storage = preferenceStorage();
  if (!storage) return { ...DEFAULTS };

  let parsed;
  try {
    const raw = storage.getItem(KEY);
    if (!raw) return { ...DEFAULTS };
    parsed = JSON.parse(raw);
  } catch {
    // A malformed legacy value may still contain a key. Fail closed by deleting it.
    try { storage.removeItem(KEY); } catch {}
    return { ...DEFAULTS };
  }

  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    try { storage.removeItem(KEY); } catch {}
    return { ...DEFAULTS };
  }

  if (typeof parsed.apiKey === 'string' && parsed.apiKey.trim()) {
    // Preserve the current page's usability while immediately removing the
    // legacy persistent copy. A reload intentionally forgets this value.
    memoryApiKey = parsed.apiKey.trim();
  }
  const preferences = normalizedPreferences(parsed);
  // Delete first so a quota/write failure cannot leave the legacy secret in
  // place. Rewriting the non-secret preferences is best effort.
  try { storage.removeItem(KEY); } catch {}
  persistPreferences(preferences);
  return { ...DEFAULTS, ...preferences, apiKey: memoryApiKey };
}

let currentPreferences = migrateLegacyConfig();

export function getLLMConfig() {
  return { ...DEFAULTS, ...currentPreferences, apiKey: memoryApiKey };
}

export function setLLMConfig(config = {}) {
  currentPreferences = normalizedPreferences(config);
  memoryApiKey = typeof config.apiKey === 'string' ? config.apiKey.trim() : '';
  persistPreferences(currentPreferences);
  return getLLMConfig();
}

export function clearLLMConfig() {
  memoryApiKey = '';
  currentPreferences = { ...DEFAULTS };
  const storage = preferenceStorage();
  try { storage?.removeItem(KEY); } catch {}
}

function mount() {
  if (document.getElementById('bcx-llm-fab')) return;
  const cfg = getLLMConfig();

  const css = document.createElement('style');
  css.textContent = `
    #bcx-llm-fab{position:fixed;right:14px;bottom:14px;z-index:9999;font:12px system-ui,sans-serif}
    #bcx-llm-fab button.fab{background:#222;color:#fff;border:1px solid #444;border-radius:18px;
      padding:7px 12px;cursor:pointer;opacity:.85}
    #bcx-llm-fab button.fab:hover{opacity:1}
    #bcx-llm-panel{position:absolute;right:0;bottom:38px;width:300px;background:#fff;color:#111;
      border:1px solid #ccc;border-radius:10px;box-shadow:0 6px 24px rgba(0,0,0,.18);padding:12px;display:none}
    #bcx-llm-panel.show{display:block}
    #bcx-llm-panel label{display:block;margin:7px 0 2px;font-weight:600}
    #bcx-llm-panel input,#bcx-llm-panel select{width:100%;box-sizing:border-box;padding:5px;
      border:1px solid #bbb;border-radius:5px;font:12px system-ui}
    #bcx-llm-panel .row{display:flex;gap:6px;margin-top:10px}
    #bcx-llm-panel .row button{flex:1;padding:6px;border-radius:6px;border:1px solid #bbb;cursor:pointer}
    #bcx-llm-panel .save{background:#1769e0;color:#fff;border-color:#1769e0}
    #bcx-llm-panel .note{margin-top:8px;color:#777;font-size:11px}
    #bcx-llm-status{margin-top:6px;font-size:11px;color:#2a8}`;
  document.head.appendChild(css);

  const wrap = document.createElement('div'); wrap.id = 'bcx-llm-fab';
  wrap.innerHTML = `
    <div id="bcx-llm-panel">
      <label>Provider</label>
      <select id="llm-prov">
        <option value="openai">OpenAI-compatible (key + base URL)</option>
        <option value="anthropic">Anthropic (x-api-key)</option>
      </select>
      <label>API key</label>
      <input id="llm-key" type="password" autocomplete="new-password" spellcheck="false" placeholder="sk-…">
      <label>Base URL <span style="font-weight:400;color:#999">(optional)</span></label>
      <input id="llm-base" type="text" placeholder="">
      <label>Model</label>
      <input id="llm-model" type="text" placeholder="">
      <label>Reasoning effort <span style="font-weight:400;color:#999">(if the model supports it)</span></label>
      <select id="llm-effort"></select>
      <div class="note" id="llm-effort-note"></div>
      <div class="row"><button class="save" id="llm-save">Apply</button><button id="llm-clear">Clear</button></div>
      <div id="bcx-llm-status"></div>
      <div class="note">The API key is kept only in this page's memory and is cleared on
        reload or close. Provider, URL, model, and effort preferences are saved locally.</div>
    </div>
    <button class="fab" id="bcx-llm-toggle">⚙ LLM key</button>`;
  document.body.appendChild(wrap);

  const $ = (id) => document.getElementById(id);
  const panel = $('bcx-llm-panel');
  const prov = $('llm-prov'), keyI = $('llm-key'), baseI = $('llm-base'), modelI = $('llm-model'), effortI = $('llm-effort'),
        effortNote = $('llm-effort-note'), status = $('bcx-llm-status');
  // Rebuild placeholders + the provider-specific effort scale, preserving the chosen effort if valid.
  function applyProviderUI() {
    const p = PLACEHOLDER[prov.value] || PLACEHOLDER.openai;
    baseI.placeholder = p.base; modelI.placeholder = p.model;
    const want = effortI.value;
    const opts = EFFORTS[prov.value] || EFFORTS.openai;
    effortI.innerHTML = opts.map(([v, lbl]) => `<option value="${v}">${lbl}</option>`).join('');
    effortI.value = opts.some(([v]) => v === want) ? want : '';
    effortNote.textContent = EFFORT_NOTE[prov.value] || EFFORT_NOTE.openai;
  }
  // Hydrate only non-secret controls. Never copy a migrated/runtime key back
  // into the DOM; the explicit Clear button retires the in-memory key.
  prov.value = cfg.provider; keyI.value = ''; baseI.value = cfg.baseUrl; modelI.value = cfg.model; effortI.value = cfg.effort || '';
  applyProviderUI();
  status.textContent = cfg.apiKey ? `key set (${cfg.provider}${cfg.model ? ', ' + cfg.model : ''})` : 'no key set';

  prov.addEventListener('change', applyProviderUI);
  $('bcx-llm-toggle').addEventListener('click', () => panel.classList.toggle('show'));
  // Click anywhere outside the panel (or press Esc) to dismiss it — not only the toggle button.
  document.addEventListener('mousedown', (e) => {
    if (panel.classList.contains('show') && !wrap.contains(e.target)) panel.classList.remove('show');
  });
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') panel.classList.remove('show'); });
  $('llm-save').addEventListener('click', () => {
    const enteredKey = keyI.value.trim();
    const activeKey = enteredKey || getLLMConfig().apiKey;
    const next = setLLMConfig({
      provider: prov.value,
      apiKey: activeKey,
      baseUrl: baseI.value.trim(),
      model: modelI.value.trim(),
      effort: effortI.value,
    });
    keyI.value = '';
    status.textContent = next.apiKey
      ? `key active for this page (${next.provider}${next.model ? ', ' + next.model : ''}${next.effort ? ', effort=' + next.effort : ''})`
      : 'no key set';
  });
  $('llm-clear').addEventListener('click', () => {
    clearLLMConfig();
    prov.value = DEFAULTS.provider;
    keyI.value = baseI.value = modelI.value = '';
    effortI.value = '';
    applyProviderUI();
    status.textContent = 'cleared';
  });
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount);
else mount();

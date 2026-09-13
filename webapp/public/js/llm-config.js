// Shared Design Agent preferences and memory-only API key. No DOM/UI imports.
const KEY = 'bcx-llm-cfg';
export const DEFAULTS = { provider: 'openai', apiKey: '', baseUrl: '', model: '', effort: '' };
const PROVIDERS = new Set(['openai', 'anthropic']);
let memoryApiKey = '';
// Effort scales differ by provider. OpenAI-compatible (GPT-5.x / codex) takes a `reasoning_effort`
// enum whose top is `xhigh` (this proxy rejects `minimal`). Anthropic has no effort enum — it maps
// to an extended-thinking token budget, so we expose budget tiers up to `max` (ultrathink-style).
export const EFFORTS = {
  openai: [['', 'model default'], ['low', 'low'], ['medium', 'medium'], ['high', 'high'], ['xhigh', 'xhigh (max)']],
  anthropic: [['', 'model default'], ['low', 'low (think)'], ['medium', 'medium'], ['high', 'high'], ['max', 'max (ultrathink)']],
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


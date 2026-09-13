// Lazy endpoint selection shared by the conversational and target-only clients.
// The native helper rotates its bearer for each launch; only its URL is persisted.
const CHAT_API_KEY = 'bcx-chat-api';
const DEFAULT_CHAT_API = 'http://127.0.0.1:8765/design-chat';
let chatBearerToken = '';

export function chatApiUrl() {
  if (typeof window !== 'undefined' && window.__BCX_CHAT_API__) return window.__BCX_CHAT_API__;
  try { return localStorage.getItem(CHAT_API_KEY) || DEFAULT_CHAT_API; }
  catch { return DEFAULT_CHAT_API; }
}

export function designTargetCompileUrl() {
  return new URL('/compile-target', chatApiUrl()).toString();
}

export function healthUrl() {
  try { return new URL('/health', chatApiUrl()).toString(); }
  catch { return DEFAULT_CHAT_API.replace('/design-chat', '/health'); }
}

export function setDesignChatEndpoint(url, bearerToken = '') {
  if (!url) return;
  if (typeof window !== 'undefined') window.__BCX_CHAT_API__ = String(url);
  chatBearerToken = bearerToken ? String(bearerToken) : '';
  try { localStorage.setItem(CHAT_API_KEY, String(url)); } catch { /* optional preferences */ }
}

export function designChatRequestHeaders({ json = false } = {}) {
  const headers = {};
  if (json) headers['Content-Type'] = 'application/json';
  if (chatBearerToken) headers.Authorization = `Bearer ${chatBearerToken}`;
  return headers;
}

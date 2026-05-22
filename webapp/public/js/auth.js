// Cognito Hosted UI OAuth (Authorization Code + PKCE) for the SPA.
// Works both in the browser and inside the Swift macOS WebView (same JS
// runtime, same localStorage). Tokens are kept in localStorage; this is the
// standard tradeoff for public SPA clients — XSS protection in this app is
// the layer that matters, and we never put long-lived AWS credentials here.

import { CLOUD_API } from './state.js';

const STORAGE_PREFIX = 'biocircuits-explorer.auth.';
const KEY_ID_TOKEN = STORAGE_PREFIX + 'id_token';
const KEY_ACCESS_TOKEN = STORAGE_PREFIX + 'access_token';
const KEY_REFRESH_TOKEN = STORAGE_PREFIX + 'refresh_token';
const KEY_EXPIRES_AT = STORAGE_PREFIX + 'expires_at';
const KEY_PKCE_VERIFIER = STORAGE_PREFIX + 'pkce_verifier';
const KEY_PKCE_STATE = STORAGE_PREFIX + 'pkce_state';
const KEY_POST_LOGIN_RETURN = STORAGE_PREFIX + 'post_login_return';

const REFRESH_LEEWAY_SECONDS = 300; // refresh 5 minutes before expiry

let cachedConfig = null;
let configPromise = null;
let refreshPromise = null;
const subscribers = new Set();

function notifyChange() {
  for (const cb of subscribers) {
    try { cb(); } catch (e) { console.warn('auth subscriber failed', e); }
  }
}

export function onAuthStateChanged(cb) {
  subscribers.add(cb);
  return () => subscribers.delete(cb);
}

export async function fetchAuthConfig() {
  if (cachedConfig) return cachedConfig;
  if (configPromise) return configPromise;
  configPromise = (async () => {
    const resp = await fetch(`${CLOUD_API}/api/auth/config`);
    if (!resp.ok) throw new Error(`Auth config fetch failed: ${resp.status}`);
    cachedConfig = await resp.json();
    return cachedConfig;
  })();
  try {
    return await configPromise;
  } finally {
    configPromise = null;
  }
}

export function isAuthEnabled() {
  return !!(cachedConfig && cachedConfig.enabled);
}

function b64urlEncode(arrayBuffer) {
  const bytes = new Uint8Array(arrayBuffer);
  let str = '';
  for (let i = 0; i < bytes.length; i++) str += String.fromCharCode(bytes[i]);
  return btoa(str).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

async function generatePkce() {
  const randomBytes = new Uint8Array(64);
  crypto.getRandomValues(randomBytes);
  const verifier = b64urlEncode(randomBytes);
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier));
  const challenge = b64urlEncode(digest);
  return { verifier, challenge };
}

function generateState() {
  const randomBytes = new Uint8Array(16);
  crypto.getRandomValues(randomBytes);
  return b64urlEncode(randomBytes);
}

function callbackUrl() {
  // Always use the canonical /auth-callback.html on the same origin as the
  // SPA. This must be one of the URLs registered on the Cognito app client.
  return `${window.location.origin}/auth-callback.html`;
}

function postLoginReturnUrl() {
  // Where to send the user after a successful login. Defaults to the app.
  try {
    const stored = window.localStorage.getItem(KEY_POST_LOGIN_RETURN);
    return stored || '/index-node.html';
  } catch (_) {
    return '/index-node.html';
  }
}

export async function signIn({ returnTo } = {}) {
  const config = await fetchAuthConfig();
  if (!config.enabled) throw new Error('Auth is not enabled in this deployment.');
  if (!config.cognito_domain) throw new Error('Cognito domain not configured.');
  if (!config.cognito_app_client_id) throw new Error('Cognito app client not configured.');

  const { verifier, challenge } = await generatePkce();
  const state = generateState();
  window.localStorage.setItem(KEY_PKCE_VERIFIER, verifier);
  window.localStorage.setItem(KEY_PKCE_STATE, state);
  if (returnTo) {
    window.localStorage.setItem(KEY_POST_LOGIN_RETURN, returnTo);
  } else if (!window.localStorage.getItem(KEY_POST_LOGIN_RETURN)) {
    window.localStorage.setItem(KEY_POST_LOGIN_RETURN, window.location.pathname + window.location.search);
  }

  const params = new URLSearchParams({
    response_type: config.response_type || 'code',
    client_id: config.cognito_app_client_id,
    redirect_uri: callbackUrl(),
    scope: (config.scopes || ['openid', 'email', 'profile']).join(' '),
    state,
    code_challenge: challenge,
    code_challenge_method: 'S256',
  });
  window.location.assign(`https://${config.cognito_domain}/oauth2/authorize?${params.toString()}`);
}

export async function handleCallback(searchParams) {
  const config = await fetchAuthConfig();
  if (!config.enabled) throw new Error('Auth is not enabled in this deployment.');

  const error = searchParams.get('error');
  if (error) {
    const description = searchParams.get('error_description') || error;
    throw new Error(`Cognito returned ${error}: ${description}`);
  }
  const code = searchParams.get('code');
  const state = searchParams.get('state');
  if (!code) throw new Error('Missing authorization code.');

  const storedState = window.localStorage.getItem(KEY_PKCE_STATE);
  if (!storedState || storedState !== state) {
    throw new Error('State mismatch — refusing to complete sign-in.');
  }
  const verifier = window.localStorage.getItem(KEY_PKCE_VERIFIER);
  if (!verifier) throw new Error('Missing PKCE verifier.');

  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    client_id: config.cognito_app_client_id,
    code,
    redirect_uri: callbackUrl(),
    code_verifier: verifier,
  });

  const resp = await fetch(`https://${config.cognito_domain}/oauth2/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Token exchange failed (${resp.status}): ${text}`);
  }
  const tokens = await resp.json();
  persistTokens(tokens);
  window.localStorage.removeItem(KEY_PKCE_VERIFIER);
  window.localStorage.removeItem(KEY_PKCE_STATE);
  notifyChange();
  return postLoginReturnUrl();
}

function persistTokens(tokens) {
  if (tokens.id_token) window.localStorage.setItem(KEY_ID_TOKEN, tokens.id_token);
  if (tokens.access_token) window.localStorage.setItem(KEY_ACCESS_TOKEN, tokens.access_token);
  if (tokens.refresh_token) window.localStorage.setItem(KEY_REFRESH_TOKEN, tokens.refresh_token);
  const expiresIn = Number(tokens.expires_in || 3600);
  const expiresAt = Date.now() + expiresIn * 1000;
  window.localStorage.setItem(KEY_EXPIRES_AT, String(expiresAt));
}

function clearTokens() {
  window.localStorage.removeItem(KEY_ID_TOKEN);
  window.localStorage.removeItem(KEY_ACCESS_TOKEN);
  window.localStorage.removeItem(KEY_REFRESH_TOKEN);
  window.localStorage.removeItem(KEY_EXPIRES_AT);
  window.localStorage.removeItem(KEY_POST_LOGIN_RETURN);
}

export function isAuthenticated() {
  const token = window.localStorage.getItem(KEY_ID_TOKEN);
  if (!token) return false;
  const expiresAt = Number(window.localStorage.getItem(KEY_EXPIRES_AT) || 0);
  return Date.now() < expiresAt;
}

export function getCurrentUser() {
  const token = window.localStorage.getItem(KEY_ID_TOKEN);
  if (!token) return null;
  const parts = token.split('.');
  if (parts.length !== 3) return null;
  try {
    const payload = JSON.parse(atob(parts[1].replace(/-/g, '+').replace(/_/g, '/')));
    return {
      sub: payload.sub,
      email: payload.email,
      email_verified: payload['email_verified'],
      exp: payload.exp,
    };
  } catch (_) {
    return null;
  }
}

async function refreshTokens() {
  if (refreshPromise) return refreshPromise;
  refreshPromise = (async () => {
    const config = await fetchAuthConfig();
    const refresh = window.localStorage.getItem(KEY_REFRESH_TOKEN);
    if (!refresh) throw new Error('No refresh token available.');
    const body = new URLSearchParams({
      grant_type: 'refresh_token',
      client_id: config.cognito_app_client_id,
      refresh_token: refresh,
    });
    const resp = await fetch(`https://${config.cognito_domain}/oauth2/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: body.toString(),
    });
    if (!resp.ok) {
      const text = await resp.text();
      clearTokens();
      notifyChange();
      throw new Error(`Token refresh failed (${resp.status}): ${text}`);
    }
    const tokens = await resp.json();
    // Cognito refresh does not return a new refresh_token; keep the existing one.
    if (!tokens.refresh_token) tokens.refresh_token = refresh;
    persistTokens(tokens);
    notifyChange();
    return tokens.id_token;
  })();
  try {
    return await refreshPromise;
  } finally {
    refreshPromise = null;
  }
}

export async function getIdToken({ requireFresh = false } = {}) {
  const config = await fetchAuthConfig();
  if (!config.enabled) return null;
  const token = window.localStorage.getItem(KEY_ID_TOKEN);
  const expiresAt = Number(window.localStorage.getItem(KEY_EXPIRES_AT) || 0);
  const needsRefresh = !token || requireFresh ||
    (Date.now() + REFRESH_LEEWAY_SECONDS * 1000 >= expiresAt);
  if (!needsRefresh) return token;
  try {
    return await refreshTokens();
  } catch (e) {
    return null;
  }
}

export async function signOut() {
  const config = await fetchAuthConfig().catch(() => null);
  clearTokens();
  notifyChange();
  if (config && config.enabled && config.cognito_domain && config.cognito_app_client_id) {
    const params = new URLSearchParams({
      client_id: config.cognito_app_client_id,
      logout_uri: window.location.origin,
    });
    window.location.assign(`https://${config.cognito_domain}/logout?${params.toString()}`);
  } else {
    window.location.assign('/');
  }
}

export async function ensureSignedIn() {
  const config = await fetchAuthConfig();
  if (!config.enabled) return null;
  const token = await getIdToken();
  if (token) return token;
  await signIn();
  // signIn redirects; control will not return.
  return null;
}

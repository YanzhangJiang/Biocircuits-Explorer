// Cognito Hosted UI OAuth (Authorization Code + PKCE) for the SPA.
// Works both in the browser and inside the Swift macOS WebView (same JS
// runtime). OAuth state and tokens live only for the current browsing context
// in sessionStorage. Older localStorage credentials are migrated once and the
// persistent copies are deleted immediately.

import { CLOUD_API } from './state.js';

const STORAGE_PREFIX = 'biocircuits-explorer.auth.';
const KEY_ID_TOKEN = STORAGE_PREFIX + 'id_token';
const KEY_ACCESS_TOKEN = STORAGE_PREFIX + 'access_token';
const KEY_REFRESH_TOKEN = STORAGE_PREFIX + 'refresh_token';
const KEY_EXPIRES_AT = STORAGE_PREFIX + 'expires_at';
const KEY_PKCE_VERIFIER = STORAGE_PREFIX + 'pkce_verifier';
const KEY_PKCE_STATE = STORAGE_PREFIX + 'pkce_state';
const KEY_POST_LOGIN_RETURN = STORAGE_PREFIX + 'post_login_return';

const LEGACY_AUTH_KEYS = [
  KEY_ID_TOKEN,
  KEY_ACCESS_TOKEN,
  KEY_REFRESH_TOKEN,
  KEY_EXPIRES_AT,
  KEY_PKCE_VERIFIER,
  KEY_PKCE_STATE,
  KEY_POST_LOGIN_RETURN,
];

const REFRESH_LEEWAY_SECONDS = 300; // refresh 5 minutes before expiry

let cachedConfig = null;
let configPromise = null;
let refreshPromise = null;
const subscribers = new Set();

function browserStorage(name) {
  try { return window?.[name] || null; }
  catch { return null; }
}

function sessionValue(key) {
  try { return browserStorage('sessionStorage')?.getItem(key) ?? null; }
  catch { return null; }
}

function setSessionValue(key, value) {
  try {
    const storage = browserStorage('sessionStorage');
    if (!storage) return false;
    storage.setItem(key, String(value));
    return true;
  } catch {
    return false;
  }
}

function removeSessionValue(key) {
  try { browserStorage('sessionStorage')?.removeItem(key); }
  catch {}
}

function migrateLegacyAuthStorage() {
  const legacy = browserStorage('localStorage');
  if (!legacy) return;

  for (const key of LEGACY_AUTH_KEYS) {
    let legacyValue = null;
    try { legacyValue = legacy.getItem(key); } catch {}

    // The access token has no browser consumer. Discard it instead of moving
    // another bearer credential into the new session-scoped store.
    if (key !== KEY_ACCESS_TOKEN && legacyValue !== null && sessionValue(key) === null) {
      setSessionValue(key, legacyValue);
    }

    // Removal is unconditional: if session storage is unavailable, signing
    // in again is safer than retaining a persistent plaintext credential.
    try { legacy.removeItem(key); } catch {}
  }
  removeSessionValue(KEY_ACCESS_TOKEN);
}

migrateLegacyAuthStorage();

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

function sameOriginReturnPath(value) {
  try {
    const base = new URL(window.location.origin);
    const target = new URL(String(value || '/index-node.html'), base);
    if (target.origin !== base.origin) return '/index-node.html';
    return `${target.pathname}${target.search}${target.hash}` || '/index-node.html';
  } catch {
    return '/index-node.html';
  }
}

function postLoginReturnUrl() {
  return sameOriginReturnPath(sessionValue(KEY_POST_LOGIN_RETURN));
}

function consumePostLoginReturnUrl() {
  const target = postLoginReturnUrl();
  removeSessionValue(KEY_POST_LOGIN_RETURN);
  return target;
}

function clearOauthFlow() {
  removeSessionValue(KEY_PKCE_VERIFIER);
  removeSessionValue(KEY_PKCE_STATE);
}

export async function signIn({ returnTo } = {}) {
  const config = await fetchAuthConfig();
  if (!config.enabled) throw new Error('Auth is not enabled in this deployment.');
  if (!config.cognito_domain) throw new Error('Cognito domain not configured.');
  if (!config.cognito_app_client_id) throw new Error('Cognito app client not configured.');

  const { verifier, challenge } = await generatePkce();
  const state = generateState();
  const currentPath = `${window.location.pathname || '/'}${window.location.search || ''}${window.location.hash || ''}`;
  const returnPath = sameOriginReturnPath(returnTo || currentPath);
  const stored = setSessionValue(KEY_PKCE_VERIFIER, verifier)
    && setSessionValue(KEY_PKCE_STATE, state)
    && setSessionValue(KEY_POST_LOGIN_RETURN, returnPath);
  if (!stored) {
    clearOauthFlow();
    removeSessionValue(KEY_POST_LOGIN_RETURN);
    throw new Error('Sign-in requires session storage so OAuth credentials are not persisted long-term.');
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
    clearOauthFlow();
    const description = searchParams.get('error_description') || error;
    throw new Error(`Cognito returned ${error}: ${description}`);
  }
  const code = searchParams.get('code');
  const state = searchParams.get('state');
  if (!code) throw new Error('Missing authorization code.');

  const storedState = sessionValue(KEY_PKCE_STATE);
  if (!storedState || storedState !== state) {
    throw new Error('State mismatch — refusing to complete sign-in.');
  }
  const verifier = sessionValue(KEY_PKCE_VERIFIER);
  if (!verifier) throw new Error('Missing PKCE verifier.');
  // The authorization code is single-use. Retire state/verifier before the
  // exchange so retries cannot accidentally reuse the OAuth transaction.
  clearOauthFlow();

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
  clearTokenValues({ clearReturn: false });
  persistTokens(tokens);
  notifyChange();
  return consumePostLoginReturnUrl();
}

function persistTokens(tokens) {
  if (!tokens || typeof tokens.id_token !== 'string' || !tokens.id_token) {
    throw new Error('Token response did not contain an ID token.');
  }
  const expiresValue = Number(tokens.expires_in);
  const expiresIn = Number.isFinite(expiresValue) && expiresValue > 0 ? expiresValue : 3600;
  const expiresAt = Date.now() + expiresIn * 1000;
  const stored = setSessionValue(KEY_ID_TOKEN, tokens.id_token)
    && setSessionValue(KEY_EXPIRES_AT, String(expiresAt))
    && (!tokens.refresh_token || setSessionValue(KEY_REFRESH_TOKEN, tokens.refresh_token));
  // The browser never consumes Cognito's access token, so intentionally do
  // not retain it. ID and refresh tokens remain scoped to this window.
  removeSessionValue(KEY_ACCESS_TOKEN);
  if (!stored) {
    clearTokenValues({ clearReturn: false });
    throw new Error('Unable to keep the sign-in session in session storage.');
  }
}

function clearTokenValues({ clearReturn = true } = {}) {
  removeSessionValue(KEY_ID_TOKEN);
  removeSessionValue(KEY_ACCESS_TOKEN);
  removeSessionValue(KEY_REFRESH_TOKEN);
  removeSessionValue(KEY_EXPIRES_AT);
  if (clearReturn) removeSessionValue(KEY_POST_LOGIN_RETURN);
}

function clearTokens() {
  clearTokenValues();
  clearOauthFlow();
}

export function isAuthenticated() {
  const token = sessionValue(KEY_ID_TOKEN);
  if (!token) return false;
  const expiresAt = Number(sessionValue(KEY_EXPIRES_AT) || 0);
  return Date.now() < expiresAt;
}

export function getCurrentUser() {
  const token = sessionValue(KEY_ID_TOKEN);
  if (!token) return null;
  const parts = token.split('.');
  if (parts.length !== 3) return null;
  try {
    const encoded = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    const payload = JSON.parse(atob(encoded.padEnd(Math.ceil(encoded.length / 4) * 4, '=')));
    return {
      sub: payload.sub,
      email: payload.email,
      email_verified: payload['email_verified'],
      exp: payload.exp,
    };
  } catch {
    return null;
  }
}

async function refreshTokens() {
  if (refreshPromise) return refreshPromise;
  refreshPromise = (async () => {
    const config = await fetchAuthConfig();
    const refresh = sessionValue(KEY_REFRESH_TOKEN);
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
  const token = sessionValue(KEY_ID_TOKEN);
  const expiresAt = Number(sessionValue(KEY_EXPIRES_AT) || 0);
  const needsRefresh = !token || requireFresh ||
    (Date.now() + REFRESH_LEEWAY_SECONDS * 1000 >= expiresAt);
  if (!needsRefresh) return token;
  try {
    return await refreshTokens();
  } catch {
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

// Sign in / sign out button binding. Stays out of view when Cognito is not
// configured (dev / on-prem deployments).

import { fetchAuthConfig, isAuthenticated, getCurrentUser, signIn, signOut, onAuthStateChanged } from './auth.js';

function renderButton(authEnabled) {
  const btn = document.getElementById('auth-btn');
  const label = document.getElementById('auth-btn-label');
  if (!btn || !label) return;
  if (!authEnabled) {
    btn.hidden = true;
    return;
  }
  btn.hidden = false;
  if (isAuthenticated()) {
    const user = getCurrentUser();
    label.textContent = user && user.email ? `Sign out (${user.email})` : 'Sign out';
    btn.dataset.signedIn = 'true';
    btn.title = 'Sign out of Biocircuits Explorer cloud';
  } else {
    label.textContent = 'Sign in';
    btn.dataset.signedIn = 'false';
    btn.title = 'Sign in to enable cloud compute';
  }
}

export async function initAuthUiEvents() {
  let config;
  try {
    config = await fetchAuthConfig();
  } catch (_) {
    config = { enabled: false };
  }
  const authEnabled = !!(config && config.enabled);
  renderButton(authEnabled);
  if (!authEnabled) return;

  const btn = document.getElementById('auth-btn');
  if (!btn) return;
  btn.addEventListener('click', async (event) => {
    event.stopPropagation();
    if (btn.dataset.signedIn === 'true') {
      await signOut();
    } else {
      await signIn();
    }
  });
  onAuthStateChanged(() => renderButton(true));
}

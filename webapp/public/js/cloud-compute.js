import { CLOUD_COMPUTE_STORAGE_KEY, cloudComputeState } from './state.js';

const cloudComputeBtn = document.getElementById('cloud-compute-btn');

function readStoredCloudComputeEnabled() {
  try {
    return window.localStorage.getItem(CLOUD_COMPUTE_STORAGE_KEY) === 'true';
  } catch (_) {
    return false;
  }
}

function persistCloudComputeEnabled(enabled) {
  try {
    window.localStorage.setItem(CLOUD_COMPUTE_STORAGE_KEY, enabled ? 'true' : 'false');
  } catch (_) {}
}

function renderCloudComputeToggle() {
  if (!cloudComputeBtn) return;
  cloudComputeBtn.classList.toggle('active', cloudComputeState.enabled);
  cloudComputeBtn.setAttribute('aria-pressed', cloudComputeState.enabled ? 'true' : 'false');
  cloudComputeBtn.title = cloudComputeState.enabled
    ? 'Cloud Compute on'
    : 'Cloud Compute off';
}

export function isCloudComputeEnabled() {
  return !!cloudComputeState.enabled;
}

export function setCloudComputeEnabled(enabled, { persist = true } = {}) {
  cloudComputeState.enabled = !!enabled;
  if (persist) persistCloudComputeEnabled(cloudComputeState.enabled);
  renderCloudComputeToggle();
  window.dispatchEvent(new CustomEvent('biocircuits-explorer:cloud-compute-changed', {
    detail: { enabled: cloudComputeState.enabled },
  }));
  return cloudComputeState.enabled;
}

export function toggleCloudComputeEnabled() {
  return setCloudComputeEnabled(!cloudComputeState.enabled);
}

export function initCloudComputeToggleEvents() {
  setCloudComputeEnabled(readStoredCloudComputeEnabled(), { persist: false });
  cloudComputeBtn?.addEventListener('click', (event) => {
    event.stopPropagation();
    toggleCloudComputeEnabled();
  });
}

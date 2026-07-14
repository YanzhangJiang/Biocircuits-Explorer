// Resolve the active surface before first paint without an inline script. The
// native shell may override this once its bridge is ready.
(() => {
  try {
    const hash = window.location.hash;
    const stored = window.localStorage.getItem('bcx-node-view');
    const surface = hash === '#agent'
      ? 'agent'
      : hash === '#workspace'
        ? 'workspace'
        : (stored || 'workspace');
    document.documentElement.dataset.nodeView = surface === 'agent' ? 'agent' : 'workspace';
  } catch {
    document.documentElement.dataset.nodeView = 'workspace';
  }
})();

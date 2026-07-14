import { connections } from './state.js';

export function setNodeLoading(nodeId, loading) {
  if (typeof document === 'undefined') return;
  const el = document.getElementById(nodeId);
  if (!el) return;
  if (loading) {
    el.classList.add('loading');
    for (const conn of connections.filter(candidate => candidate.toNode === nodeId)) {
      document.getElementById(`wire-${conn.fromNode}-${conn.toNode}`)?.classList.add('transmitting');
    }
    return;
  }

  el.classList.remove('loading');
  for (const conn of connections.filter(candidate => candidate.toNode === nodeId)) {
    document.getElementById(`wire-${conn.fromNode}-${conn.toNode}`)?.classList.remove('transmitting');
  }
}

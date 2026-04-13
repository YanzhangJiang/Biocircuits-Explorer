import { debugConsoleState } from './state.js';
import { apiSilent, escapeHtml } from './api.js';

const debugConsoleBtn = document.getElementById('debug-console-btn');
const debugConsolePanel = document.getElementById('debug-console');
const debugConsoleBody = document.getElementById('debug-console-body');
const debugConsoleCounter = document.getElementById('debug-console-counter');
const debugConsoleIndicator = document.getElementById('debug-console-indicator');
const debugConsoleCloseBtn = document.getElementById('debug-console-close');
const debugConsoleRefreshBtn = document.getElementById('debug-console-refresh');

export function debugConsoleShouldStickToBottom() {
  if (!debugConsoleBody) return true;
  const threshold = 32;
  return debugConsoleBody.scrollHeight - debugConsoleBody.scrollTop - debugConsoleBody.clientHeight <= threshold;
}

export function renderDebugEntry(entry) {
  const level = String(entry.level || 'INFO').toUpperCase();
  const scope = [entry.module, entry.file ? `${entry.file}${entry.line ? `:${entry.line}` : ''}` : null]
    .filter(Boolean)
    .join(' · ');
  const lines = [];
  const header = [entry.timestamp || '', level, scope].filter(Boolean).join('  ');
  if (header) lines.push(header);
  if (entry.message) lines.push(String(entry.message));
  if (entry.details) lines.push(String(entry.details));
  return `<pre class="debug-terminal-line level-${level.toLowerCase()}" data-seq="${entry.seq}">${escapeHtml(lines.join('\n'))}</pre>`;
}

export function updateDebugConsoleIndicator() {
  if (!debugConsoleIndicator) return;
  debugConsoleIndicator.style.display = debugConsoleState.unseenPriority ? '' : 'none';
}

export function renderDebugConsole(fullRender = true, newEntries = []) {
  if (!debugConsoleBody) return;
  const shouldStick = debugConsoleShouldStickToBottom();
  if (!debugConsoleState.entries.length) {
    debugConsoleBody.innerHTML = '<div class="debug-console-empty">No backend logs captured yet.</div>';
  } else if (fullRender) {
    debugConsoleBody.innerHTML = debugConsoleState.entries.map(renderDebugEntry).join('');
  } else if (newEntries.length) {
    const emptyEl = debugConsoleBody.querySelector('.debug-console-empty');
    if (emptyEl) emptyEl.remove();
    debugConsoleBody.insertAdjacentHTML('beforeend', newEntries.map(renderDebugEntry).join(''));
  }

  if (debugConsoleCounter) {
    const count = debugConsoleState.entries.length;
    debugConsoleCounter.textContent = `${count} line${count === 1 ? '' : 's'}`;
  }

  if (shouldStick || fullRender) {
    debugConsoleBody.scrollTop = debugConsoleBody.scrollHeight;
  }
}

export async function refreshDebugConsole(forceFull = false) {
  if (debugConsoleState.fetchInFlight) return;
  debugConsoleState.fetchInFlight = true;

  try {
    const data = await apiSilent('debug_logs', {
      after_seq: forceFull ? 0 : debugConsoleState.lastSeq,
      limit: forceFull ? 400 : 200,
    });

    const entries = Array.isArray(data.entries) ? data.entries : [];
    if (forceFull) {
      debugConsoleState.entries = entries.slice(-800);
      const lastEntry = debugConsoleState.entries.length ? debugConsoleState.entries[debugConsoleState.entries.length - 1] : null;
      debugConsoleState.lastSeq = data.next_seq || (lastEntry ? lastEntry.seq : 0);
      renderDebugConsole(true);
    } else if (entries.length) {
      debugConsoleState.entries.push(...entries);
      if (debugConsoleState.entries.length > 800) {
        debugConsoleState.entries = debugConsoleState.entries.slice(-800);
      }
      debugConsoleState.lastSeq = data.next_seq || debugConsoleState.lastSeq;
      renderDebugConsole(false, entries);
      if (!debugConsoleState.open) {
        debugConsoleState.unseenPriority ||= entries.some(entry => ['WARN', 'ERROR'].includes(String(entry.level || '').toUpperCase()));
        updateDebugConsoleIndicator();
      }
    }
  } catch (e) {
    if (forceFull && debugConsoleBody) {
      debugConsoleBody.innerHTML = `<div class="debug-console-empty">${escapeHtml(e.message)}</div>`;
    }
  } finally {
    debugConsoleState.fetchInFlight = false;
  }
}

export function startDebugConsolePolling() {
  if (debugConsoleState.pollTimer) return;
  debugConsoleState.pollTimer = setInterval(() => {
    refreshDebugConsole(false);
  }, 1500);
}

export function stopDebugConsolePolling() {
  if (!debugConsoleState.pollTimer) return;
  clearInterval(debugConsoleState.pollTimer);
  debugConsoleState.pollTimer = null;
}

export function openDebugConsole() {
  debugConsoleState.open = true;
  debugConsoleState.unseenPriority = false;
  document.body.classList.add('debug-console-open');
  debugConsolePanel?.classList.add('open');
  debugConsolePanel?.setAttribute('aria-hidden', 'false');
  debugConsoleBtn?.classList.add('active');
  updateDebugConsoleIndicator();
  refreshDebugConsole(debugConsoleState.entries.length === 0 ? true : false);
  startDebugConsolePolling();
}

export function closeDebugConsole() {
  debugConsoleState.open = false;
  document.body.classList.remove('debug-console-open');
  debugConsolePanel?.classList.remove('open');
  debugConsolePanel?.setAttribute('aria-hidden', 'true');
  debugConsoleBtn?.classList.remove('active');
  stopDebugConsolePolling();
}

export function toggleDebugConsole() {
  if (debugConsoleState.open) closeDebugConsole();
  else openDebugConsole();
}

export function initDebugConsoleEvents() {
  debugConsoleBtn?.addEventListener('click', (e) => {
    e.stopPropagation();
    toggleDebugConsole();
  });

  debugConsoleCloseBtn?.addEventListener('click', () => closeDebugConsole());
  debugConsoleRefreshBtn?.addEventListener('click', () => refreshDebugConsole(true));

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && debugConsoleState.open) {
      closeDebugConsole();
    }
  });
}

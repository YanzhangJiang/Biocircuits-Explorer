import { api } from './api.js';
import { API, ensureDebugClientId } from './state.js';
import { createLocalDesignJobRunner } from './local-design-job-core.js';

async function localJob(id, suffix = '', signal) {
  const response = await fetch(`${API}/api/v1/jobs/${encodeURIComponent(id)}${suffix}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Biocircuits-Explorer-Debug-Client': ensureDebugClientId(),
      'X-ROP-Debug-Client': ensureDebugClientId(),
    },
    body: '{}',
    signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(30000)]) : AbortSignal.timeout(30000),
  });
  if (!response.headers.get('content-type')?.includes('application/json')) {
    const error = new Error('Local design backend did not return JSON.');
    error.status = response.status;
    throw error;
  }
  const payload = await response.json();
  if (!response.ok || payload?.error) {
    const error = new Error(typeof payload.error === 'string' ? payload.error :
      payload.error?.message || `Local design request failed (${response.status}).`);
    error.status = response.status;
    throw error;
  }
  return payload;
}

function wait(ms, signal) {
  return new Promise((resolve, reject) => {
    const finish = () => { signal?.removeEventListener('abort', aborted); resolve(); };
    const timer = setTimeout(finish, ms);
    const aborted = () => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', aborted);
      reject(new DOMException('Design cancelled.', 'AbortError'));
    };
    signal?.addEventListener('abort', aborted, { once: true });
    if (signal?.aborted) aborted();
  });
}

export const runLocalDesignJob = createLocalDesignJobRunner({
  submit: (request, options) => api('design_network', request, options),
  inspect: (id, signal) => localJob(id, '', signal),
  result: async (id, signal) => {
    const envelope = await localJob(id, '/result', signal);
    if (envelope.job?.job_id !== id || envelope.job?.status !== 'succeeded' || !envelope.result) {
      throw new Error('Local design result does not belong to a completed job.');
    }
    return envelope.result;
  },
  cancel: id => localJob(id, '/cancel'),
  wait,
});

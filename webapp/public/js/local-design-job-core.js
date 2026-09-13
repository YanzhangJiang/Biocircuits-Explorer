// A local job remains owned by its originating target, including while the
// submission is in flight. Never route a desktop-local design through Cloud.
const TERMINAL = new Set(['succeeded', 'failed', 'cancelled']);
const KNOWN = new Set(['queued', 'running', 'cancel_requested', ...TERMINAL]);

export function createLocalDesignJobRunner({ submit, inspect, result, cancel, wait }) {
  return async function run(request, { signal, statusIsCurrent, onProgress } = {}) {
    let id = null, status = null, cancellationSent = false;
    const current = () => {
      if (signal?.aborted) return false;
      try { return typeof statusIsCurrent !== 'function' || !!statusIsCurrent(); }
      catch { return false; }
    };
    const abort = () => {
      const error = new Error('Design request cancelled or replaced.');
      error.name = 'AbortError';
      return error;
    };
    const retire = async () => {
      if (id && !cancellationSent && !TERMINAL.has(status)) {
        cancellationSent = true;
        try { await cancel(id); } catch { /* An obsolete run must not disturb its successor. */ }
      }
    };
    const check = () => { if (!current()) throw abort(); };
    const acceptStatus = job => {
      if (job?.job_id !== id) throw new Error('Design job identity does not match this request.');
      const next = String(job.status || '').toLowerCase();
      if (!KNOWN.has(next)) throw new Error(`Unknown design job status: ${next || '(missing)'}`);
      status = next;
      return job;
    };
    try {
      check();
      // Retain the submission response even if its owner is invalidated, so an
      // already accepted job can still be cancelled by its exact returned id.
      let job = await submit(request, { statusIsCurrent: current });
      if (job?.status === 'ok' && job.selected_network) { check(); return job; }
      id = job?.job_id;
      if (typeof id !== 'string' || !id) throw new Error('Backend did not return a design job id.');
      acceptStatus(job);
      check();
      let failures = 0;
      while (!TERMINAL.has(status)) {
        check();
        onProgress?.(job);
        await wait(350, signal);
        check();
        try {
          job = acceptStatus(await inspect(id, signal));
          failures = 0;
        } catch (error) {
          check();
          const retryable = error instanceof TypeError || error.name === 'TimeoutError' ||
            [408, 425, 429, 500, 502, 503, 504].includes(error.status);
          if (!retryable || ++failures > 2) throw error;
        }
      }
      check();
      onProgress?.(job);
      if (status !== 'succeeded') {
        const message = typeof job.error === 'string' ? job.error : job.error?.message;
        throw new Error(message || `Design job ${status}.`);
      }
      const payload = await result(id, signal);
      check();
      return payload;
    } catch (error) {
      await retire();
      throw current() ? error : abort();
    }
  };
}

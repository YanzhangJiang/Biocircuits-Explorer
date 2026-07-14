import assert from 'node:assert/strict';

global.window = {
  matchMedia: () => null,
  crypto: { randomUUID: () => 'debug-test' },
  sessionStorage: { getItem: () => null, setItem: () => {} },
};
global.document = {
  getElementById: () => null,
  documentElement: {
    dataset: {},
    style: { setProperty: () => {} },
  },
};

const { state, nodeRegistry, cloudComputeState } = await import('../public/js/state.js');
const {
  api,
  apiSilent,
  computeApi,
  enrichModelRequestPayload,
  optimizeRopShape,
  setStatus,
  submitJob,
} = await import('../public/js/api.js');

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

function jsonResponse(json, { status = 200, ok = status >= 200 && status < 300 } = {}) {
  return {
    ok,
    status,
    headers: { get: () => 'application/json' },
    json: async () => json,
  };
}

test('model requests include NetworkIR recovery data for matching node context', () => {
  Object.keys(nodeRegistry).forEach((key) => delete nodeRegistry[key]);
  state.model = null;
  nodeRegistry.builder = {
    data: {
      modelContext: {
        sessionId: 'stale-session',
        networkIrHash: 'hash-1',
        networkIr: { reactions: [{ formula: 'A + B <-> AB', kd: 1 }] },
      },
    },
  };

  const original = { session_id: 'stale-session', output_exprs: ['A'] };
  const enriched = enrichModelRequestPayload(original);

  assert.notEqual(enriched, original);
  assert.equal(enriched.session_id, 'stale-session');
  assert.equal(enriched.network_ir_hash, 'hash-1');
  assert.deepEqual(enriched.network, { reactions: [{ formula: 'A + B <-> AB', kd: 1 }] });
  assert.equal(original.network, undefined, 'must not mutate caller payload');
});

test('model requests keep explicit network payloads untouched', () => {
  const payload = { session_id: 'stale-session', network: { label: 'explicit' } };
  assert.equal(enrichModelRequestPayload(payload), payload);
});

{
  const priorFetch = globalThis.fetch;
  const calledUrls = [];
  try {
    globalThis.fetch = async (url) => {
      calledUrls.push(url);
      return {
        ok: true,
        status: 200,
        headers: { get: () => 'application/json; charset=utf-8' },
        json: async () => ({ status: 'ok' }),
      };
    };
    await apiSilent('version', {});
    await api('build_model', {});
    await apiSilent('v1/import/sbml', {});
    await apiSilent('/api/v1/version', {});
    assert.deepEqual(calledUrls, [
      '/api/v1/version',
      '/api/v1/build_model',
      '/api/v1/import/sbml',
      '/api/v1/version',
    ]);
    await assert.rejects(() => apiSilent('', {}), /endpoint must not be empty/);
    await assert.rejects(() => apiSilent('/api/v10/version', {}), /Unsupported API version v10/);
    await assert.rejects(() => apiSilent('v2/version', {}), /Unsupported API version v2/);
    passed += 1;
    console.log('  ok - shared browser API helpers use canonical v1 routes');
  } finally {
    globalThis.fetch = priorFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  try {
    globalThis.fetch = async (url) => {
      if (url === '/api/v1/auth/config') {
        return {
          ok: true,
          status: 200,
          headers: { get: () => 'application/json' },
          json: async () => ({ enabled: false }),
        };
      }
      assert.equal(url, '/api/v1/jobs');
      return {
        ok: false,
        status: 422,
        headers: { get: () => 'application/json' },
        json: async () => ({
          error: { message: 'Job specification is invalid', code: 'invalid_job_spec', retryable: false },
        }),
      };
    };
    let error;
    try {
      await submitJob('build_atlas', {}, { mode: 'local_async' });
    } catch (caught) {
      error = caught;
    }
    assert.equal(error?.message, 'Job specification is invalid');
    assert.equal(error?.status, 422);
    assert.equal(error?.code, 'invalid_job_spec');
    assert.equal(error?.retryable, false);
    passed += 1;
    console.log('  ok - job API preserves structured backend error metadata');
  } finally {
    globalThis.fetch = priorFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  const priorGetElementById = globalThis.document.getElementById;
  const badge = { className: '', textContent: '' };
  let rejectOldFetch;
  let oldRequestIsCurrent = true;
  try {
    globalThis.document.getElementById = id => id === 'status-badge' ? badge : null;
    globalThis.fetch = async () => new Promise((_, reject) => {
      rejectOldFetch = reject;
    });
    const request = {
      schema_version: 'bne-rop-shape-optimize-request/v1.0.0',
      edit_intent: { kind: 'broaden_both_ears' },
    };
    const oldRequest = optimizeRopShape(request, {
      statusIsCurrent: () => oldRequestIsCurrent,
    });
    await Promise.resolve();
    oldRequestIsCurrent = false;
    setStatus('done', 'Newer request done');
    rejectOldFetch(new Error('obsolete failure'));
    await assert.rejects(() => oldRequest, /obsolete failure/);
    assert.equal(badge.textContent, 'Newer request done');
    assert.doesNotMatch(badge.className, /error/);
    setStatus('working', 'cleanup');
    passed += 1;
    console.log('  ok - stale specialized API failures cannot overwrite newer status');
  } finally {
    globalThis.fetch = priorFetch;
    globalThis.document.getElementById = priorGetElementById;
  }
}

{
  const priorFetch = globalThis.fetch;
  cloudComputeState.enabled = true;
  let requestIsCurrent = true;
  const calls = [];
  try {
    globalThis.fetch = async (url) => {
      calls.push(url);
      if (url === '/api/v1/auth/config') return jsonResponse({ enabled: false });
      if (url === '/api/v1/jobs') {
        return {
          ...jsonResponse(null),
          json: async () => {
            requestIsCurrent = false;
            return { job_id: 'stale-after-submit', status: 'queued' };
          },
        };
      }
      if (url === '/api/v1/jobs/stale-after-submit/cancel') {
        return jsonResponse({ job_id: 'stale-after-submit', status: 'cancel_requested' });
      }
      throw new Error(`Unexpected URL ${url}`);
    };

    const result = await computeApi('build_atlas', { stale: 'after-submit' }, {
      statusIsCurrent: () => requestIsCurrent,
      sleepFn: async () => { throw new Error('stale submission must not enter polling'); },
    });
    assert.equal(result, null);
    assert.equal(calls.filter(url => url.endsWith('/cancel')).length, 1);
    assert.equal(calls.some(url => url.includes('/stale-after-submit/result')), false);
    passed += 1;
    console.log('  ok - stale immediately after submit cancels the nonterminal job exactly once');
  } finally {
    cloudComputeState.enabled = false;
    globalThis.fetch = priorFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  cloudComputeState.enabled = true;
  try {
    for (const submittedStatus of ['queued', 'running']) {
      let requestIsCurrent = true;
      let cancelCount = 0;
      let pollCount = 0;
      globalThis.fetch = async (url) => {
        if (url === '/api/v1/auth/config') return jsonResponse({ enabled: false });
        if (url === '/api/v1/jobs') {
          return jsonResponse({ job_id: `stale-${submittedStatus}`, status: submittedStatus });
        }
        if (url === `/api/v1/jobs/stale-${submittedStatus}/cancel`) {
          cancelCount += 1;
          return jsonResponse({ status: 'cancel_requested' });
        }
        if (url === `/api/v1/jobs/stale-${submittedStatus}`) {
          pollCount += 1;
          return jsonResponse({ job_id: `stale-${submittedStatus}`, status: submittedStatus });
        }
        throw new Error(`Unexpected URL ${url}`);
      };

      const result = await computeApi('build_atlas', { stale: submittedStatus }, {
        statusIsCurrent: () => requestIsCurrent,
        sleepFn: async () => { requestIsCurrent = false; },
      });
      assert.equal(result, null);
      assert.equal(cancelCount, 1, `${submittedStatus} stale owner must cancel exactly once`);
      assert.equal(pollCount, 0, `${submittedStatus} stale owner must stop before polling`);
    }
    passed += 1;
    console.log('  ok - stale queued and running owners each cancel exactly once');
  } finally {
    cloudComputeState.enabled = false;
    globalThis.fetch = priorFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  cloudComputeState.enabled = true;
  try {
    for (const terminalStatus of ['succeeded', 'failed', 'cancelled']) {
      let requestIsCurrent = true;
      let cancelCount = 0;
      globalThis.fetch = async (url) => {
        if (url === '/api/v1/auth/config') return jsonResponse({ enabled: false });
        if (url === '/api/v1/jobs') {
          return {
            ...jsonResponse(null),
            json: async () => {
              requestIsCurrent = false;
              return { job_id: `terminal-${terminalStatus}`, status: terminalStatus };
            },
          };
        }
        if (url.endsWith('/cancel')) {
          cancelCount += 1;
          return jsonResponse({ status: 'cancel_requested' });
        }
        throw new Error(`Unexpected URL ${url}`);
      };

      const result = await computeApi('build_atlas', { terminalStatus }, {
        statusIsCurrent: () => requestIsCurrent,
        sleepFn: async () => {},
      });
      assert.equal(result, null);
      assert.equal(cancelCount, 0, `${terminalStatus} must not be cancelled after owner loss`);
    }
    passed += 1;
    console.log('  ok - terminal stale jobs are never cancelled');
  } finally {
    cloudComputeState.enabled = false;
    globalThis.fetch = priorFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  const priorGetElementById = globalThis.document.getElementById;
  const badge = { className: '', textContent: '' };
  cloudComputeState.enabled = true;
  let requestIsCurrent = true;
  try {
    globalThis.document.getElementById = id => id === 'status-badge' ? badge : null;
    globalThis.fetch = async (url) => {
      if (url === '/api/v1/auth/config') return jsonResponse({ enabled: false });
      if (url === '/api/v1/jobs') {
        return {
          ...jsonResponse(null),
          json: async () => {
            requestIsCurrent = false;
            return { job_id: 'cancel-failure', status: 'queued' };
          },
        };
      }
      if (url === '/api/v1/jobs/cancel-failure/cancel') {
        return jsonResponse(
          { error: { message: 'cancel temporarily unavailable', retryable: true } },
          { status: 503, ok: false },
        );
      }
      throw new Error(`Unexpected URL ${url}`);
    };

    const result = await computeApi('build_atlas', { cancel: 'fails' }, {
      statusIsCurrent: () => requestIsCurrent,
      sleepFn: async () => {},
    });
    assert.equal(result, null);
    assert.doesNotMatch(badge.className, /error/);

    let finishNextRequest;
    globalThis.fetch = async () => new Promise(resolve => { finishNextRequest = resolve; });
    const nextRequest = api('version', {});
    assert.equal(badge.textContent, 'Computing...', 'cancel failure must leave stale activity settled');
    finishNextRequest(jsonResponse({ version: 'after-cancel-failure' }));
    await nextRequest;
    setStatus('working', 'cleanup');
    passed += 1;
    console.log('  ok - stale cancellation failure is silent and activity settles once');
  } finally {
    cloudComputeState.enabled = false;
    globalThis.fetch = priorFetch;
    globalThis.document.getElementById = priorGetElementById;
  }
}

{
  const priorFetch = globalThis.fetch;
  cloudComputeState.enabled = true;
  let predicateCalls = 0;
  let cancelCount = 0;
  try {
    globalThis.fetch = async (url) => {
      if (url === '/api/v1/auth/config') return jsonResponse({ enabled: false });
      if (url === '/api/v1/jobs') return jsonResponse({ job_id: 'predicate-throw', status: 'queued' });
      if (url === '/api/v1/jobs/predicate-throw/cancel') {
        cancelCount += 1;
        return jsonResponse({ status: 'cancel_requested' });
      }
      throw new Error(`Unexpected URL ${url}`);
    };
    const result = await computeApi('build_atlas', { predicate: 'throws' }, {
      statusIsCurrent: () => {
        predicateCalls += 1;
        if (predicateCalls >= 2) throw new Error('predicate failure');
        return true;
      },
      sleepFn: async () => {},
    });
    assert.equal(result, null);
    assert.equal(cancelCount, 1);
    passed += 1;
    console.log('  ok - a throwing owner predicate fails closed and retires the cloud job');
  } finally {
    cloudComputeState.enabled = false;
    globalThis.fetch = priorFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  cloudComputeState.enabled = true;
  try {
    for (const retryableFailures of [1, 2]) {
      const jobId = `poll-recovers-${retryableFailures}`;
      let pollCalls = 0;
      let cancelCount = 0;
      const waits = [];
      globalThis.fetch = async (url, options) => {
        if (url === '/api/v1/auth/config') return jsonResponse({ enabled: false });
        if (url === '/api/v1/jobs') return jsonResponse({ job_id: jobId, status: 'queued' });
        if (url === `/api/v1/jobs/${jobId}`) {
          pollCalls += 1;
          if (pollCalls <= retryableFailures) {
            return jsonResponse(
              { error: { message: 'poll temporarily unavailable', retryable: true } },
              { status: 503, ok: false },
            );
          }
          return jsonResponse({ job_id: jobId, status: 'succeeded' });
        }
        if (url === `/api/v1/jobs/${jobId}/result-url`) {
          return jsonResponse({ result_url: `https://artifacts.example/${jobId}.json` });
        }
        if (url === `https://artifacts.example/${jobId}.json`) {
          assert.deepEqual(options, { method: 'GET' });
          return jsonResponse({ recovered_after: retryableFailures });
        }
        if (url.endsWith('/cancel')) {
          cancelCount += 1;
          return jsonResponse({ status: 'cancel_requested' });
        }
        throw new Error(`Unexpected URL ${url}`);
      };

      const result = await computeApi('build_atlas', { retryableFailures }, {
        sleepFn: async delay => { waits.push(delay); },
      });
      assert.deepEqual(result, { recovered_after: retryableFailures });
      assert.equal(pollCalls, retryableFailures + 1);
      assert.equal(cancelCount, 0);
      assert.equal(waits.length, retryableFailures + 1, 'normal poll plus each bounded retry backoff');
    }

    let resetPollCalls = 0;
    globalThis.fetch = async (url, options) => {
      if (url === '/api/v1/auth/config') return jsonResponse({ enabled: false });
      if (url === '/api/v1/jobs') return jsonResponse({ job_id: 'poll-reset', status: 'queued' });
      if (url === '/api/v1/jobs/poll-reset') {
        resetPollCalls += 1;
        if ([1, 2, 4, 5].includes(resetPollCalls)) {
          return jsonResponse(
            { error: { message: 'retry before and after success', retryable: true } },
            { status: 503, ok: false },
          );
        }
        return jsonResponse({
          job_id: 'poll-reset',
          status: resetPollCalls === 3 ? 'queued' : 'succeeded',
        });
      }
      if (url === '/api/v1/jobs/poll-reset/result-url') {
        return jsonResponse({ result_url: 'https://artifacts.example/poll-reset.json' });
      }
      if (url === 'https://artifacts.example/poll-reset.json') {
        assert.deepEqual(options, { method: 'GET' });
        return jsonResponse({ retry_counter_reset: true });
      }
      throw new Error(`Unexpected URL ${url}`);
    };
    assert.deepEqual(
      await computeApi('build_atlas', { retries: 'reset-after-success' }, { sleepFn: async () => {} }),
      { retry_counter_reset: true },
    );
    assert.equal(resetPollCalls, 6, 'a successful queued poll must reset the consecutive retry budget');
    passed += 1;
    console.log('  ok - one or two retryable poll failures recover and success resets the budget');
  } finally {
    cloudComputeState.enabled = false;
    globalThis.fetch = priorFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  cloudComputeState.enabled = true;
  let pollCalls = 0;
  try {
    globalThis.fetch = async (url) => {
      if (url === '/api/v1/auth/config') return jsonResponse({ enabled: false });
      if (url === '/api/v1/jobs') return jsonResponse({ job_id: 'poll-limit', status: 'queued' });
      if (url === '/api/v1/jobs/poll-limit') {
        pollCalls += 1;
        return jsonResponse(
          { error: { message: 'poll retry limit reached', retryable: true } },
          { status: 503, ok: false },
        );
      }
      throw new Error(`Unexpected URL ${url}`);
    };
    await assert.rejects(
      () => computeApi('build_atlas', { retries: 'exhausted' }, { sleepFn: async () => {} }),
      /poll retry limit reached/,
    );
    assert.equal(pollCalls, 3, 'initial poll plus two retries must be the hard limit');
    passed += 1;
    console.log('  ok - retryable polling fails after the consecutive retry limit');
  } finally {
    cloudComputeState.enabled = false;
    globalThis.fetch = priorFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  cloudComputeState.enabled = true;
  let pollCalls = 0;
  try {
    globalThis.fetch = async (url) => {
      if (url === '/api/v1/auth/config') return jsonResponse({ enabled: false });
      if (url === '/api/v1/jobs') return jsonResponse({ job_id: 'poll-nonretryable', status: 'queued' });
      if (url === '/api/v1/jobs/poll-nonretryable') {
        pollCalls += 1;
        return jsonResponse(
          { error: { message: 'poll contract failure', retryable: false } },
          { status: 422, ok: false },
        );
      }
      throw new Error(`Unexpected URL ${url}`);
    };
    await assert.rejects(
      () => computeApi('build_atlas', { retries: 'forbidden' }, { sleepFn: async () => {} }),
      /poll contract failure/,
    );
    assert.equal(pollCalls, 1);
    passed += 1;
    console.log('  ok - non-retryable polling errors fail without retry');
  } finally {
    cloudComputeState.enabled = false;
    globalThis.fetch = priorFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  cloudComputeState.enabled = true;
  try {
    let resultUrlCalls = 0;
    const directUrls = [];
    globalThis.fetch = async (url, options) => {
      if (url === '/api/v1/auth/config') return jsonResponse({ enabled: false });
      if (url === '/api/v1/jobs') return jsonResponse({ job_id: 'result-url-retry', status: 'succeeded' });
      if (url === '/api/v1/jobs/result-url-retry/result-url') {
        resultUrlCalls += 1;
        if (resultUrlCalls === 1) {
          return jsonResponse(
            { error: { message: 'presign temporarily unavailable', retryable: true } },
            { status: 503, ok: false },
          );
        }
        return jsonResponse({ result_url: 'https://artifacts.example/result-url-retry-fresh.json' });
      }
      if (url === 'https://artifacts.example/result-url-retry-fresh.json') {
        directUrls.push(url);
        assert.deepEqual(options, { method: 'GET' });
        return jsonResponse({ result_url_retry: true });
      }
      throw new Error(`Unexpected URL ${url}`);
    };
    assert.deepEqual(
      await computeApi('build_atlas', { resultUrl: 'retry' }, { sleepFn: async () => {} }),
      { result_url_retry: true },
    );
    assert.equal(resultUrlCalls, 2);
    assert.equal(directUrls.length, 1);

    let directAttempt = 0;
    resultUrlCalls = 0;
    globalThis.fetch = async (url, options) => {
      if (url === '/api/v1/auth/config') return jsonResponse({ enabled: false });
      if (url === '/api/v1/jobs') return jsonResponse({ job_id: 'direct-retry', status: 'succeeded' });
      if (url === '/api/v1/jobs/direct-retry/result-url') {
        resultUrlCalls += 1;
        return jsonResponse({ result_url: `https://artifacts.example/direct-retry-${resultUrlCalls}.json` });
      }
      if (url.startsWith('https://artifacts.example/direct-retry-')) {
        directAttempt += 1;
        assert.deepEqual(options, { method: 'GET' });
        if (directAttempt === 1) {
          return jsonResponse({ error: 'expired signature' }, { status: 403, ok: false });
        }
        return jsonResponse({ direct_retry: true });
      }
      if (url === '/api/v1/jobs/direct-retry/result') {
        throw new Error('broker relay must never be used');
      }
      throw new Error(`Unexpected URL ${url}`);
    };
    assert.deepEqual(
      await computeApi('build_atlas', { direct: 'retry' }, { sleepFn: async () => {} }),
      { direct_retry: true },
    );
    assert.equal(resultUrlCalls, 2, 'direct retry must obtain a fresh pre-signed URL');
    assert.equal(directAttempt, 2);
    passed += 1;
    console.log('  ok - transient result URL and direct GET failures retry once with a fresh URL');
  } finally {
    cloudComputeState.enabled = false;
    globalThis.fetch = priorFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  cloudComputeState.enabled = true;
  const calledUrls = [];
  let submittedStatus = 'SUCCEEDED';
  try {
    globalThis.fetch = async (url, options) => {
      calledUrls.push(url);
      let json;
      if (url === '/api/v1/auth/config') json = { enabled: false };
      else if (url === '/api/v1/jobs') json = { job_id: 'job-1', status: submittedStatus };
      else if (url === '/api/v1/jobs/job-1/result-url') {
        json = { result_url: 'https://artifacts.example/job-1/result.json?signature=one' };
      } else if (url === 'https://artifacts.example/job-1/result.json?signature=one') {
        assert.deepEqual(options, { method: 'GET' });
        json = { ok: true };
      }
      else throw new Error(`Unexpected URL ${url}`);
      return {
        ok: true,
        status: 200,
        headers: { get: () => 'application/json' },
        json: async () => json,
      };
    };

    const cloudResult = await computeApi('/api/v1/build_atlas', { spec: true });
    assert.deepEqual(cloudResult, { ok: true });
    assert.deepEqual(calledUrls, [
      '/api/v1/jobs',
      '/api/v1/jobs/job-1/result-url',
      'https://artifacts.example/job-1/result.json?signature=one',
    ]);
    assert.equal(calledUrls.some(url => url === '/api/v1/jobs/job-1/result'), false);

    calledUrls.length = 0;
    submittedStatus = 'mystery';
    await assert.rejects(
      () => computeApi('build_atlas', { spec: true }),
      /unknown job status: mystery/,
    );
    assert.deepEqual(calledUrls, ['/api/v1/jobs']);

    calledUrls.length = 0;
    submittedStatus = 'queued';
    let currentChecks = 0;
    const staleResult = await computeApi('build_atlas', { spec: true }, {
      statusIsCurrent: () => currentChecks++ === 0,
    });
    assert.equal(staleResult, null);
    assert.deepEqual(calledUrls, ['/api/v1/jobs', '/api/v1/jobs/job-1/cancel']);
    passed += 1;
    console.log('  ok - cloud dispatch normalizes v1 ids and cancels stale nonterminal jobs');
  } finally {
    cloudComputeState.enabled = false;
    globalThis.fetch = priorFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  cloudComputeState.enabled = true;
  const cases = [
    {
      name: 'HTTP failure',
      response: {
        ok: false,
        status: 503,
        headers: { get: () => 'application/json' },
        json: async () => ({ error: 'unavailable' }),
      },
      message: /Cloud result download failed \(503\)\./,
    },
    {
      name: 'non-JSON media type',
      response: {
        ok: true,
        status: 200,
        headers: { get: () => 'text/html' },
        json: async () => ({ should_not_parse: true }),
      },
      message: /did not return application\/json/,
    },
    {
      name: 'invalid JSON',
      response: {
        ok: true,
        status: 200,
        headers: { get: () => 'application/json; charset=utf-8' },
        json: async () => { throw new SyntaxError('bad json'); },
      },
      message: /returned invalid JSON/,
    },
  ];
  try {
    for (const scenario of cases) {
      const calledUrls = [];
      globalThis.fetch = async (url, options) => {
        calledUrls.push(url);
        if (url === '/api/v1/jobs') {
          return {
            ok: true,
            status: 200,
            headers: { get: () => 'application/json' },
            json: async () => ({ job_id: 'job-errors', status: 'succeeded' }),
          };
        }
        if (url === '/api/v1/jobs/job-errors/result-url') {
          return {
            ok: true,
            status: 200,
            headers: { get: () => 'application/json' },
            json: async () => ({ result_url: 'https://artifacts.example/job-errors/result.json' }),
          };
        }
        if (url === 'https://artifacts.example/job-errors/result.json') {
          assert.deepEqual(options, { method: 'GET' }, `${scenario.name} must stay a plain GET`);
          return scenario.response;
        }
        throw new Error(`Unexpected URL ${url}`);
      };

      await assert.rejects(
        () => computeApi('build_atlas', { spec: scenario.name }),
        scenario.message,
      );
      assert.equal(
        calledUrls.some(url => url === '/api/v1/jobs/job-errors/result'),
        false,
        `${scenario.name} must not fall back to the broker result body`,
      );
    }
    passed += 1;
    console.log('  ok - direct cloud result downloads fail closed without broker fallback');
  } finally {
    cloudComputeState.enabled = false;
    globalThis.fetch = priorFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  const priorGetElementById = globalThis.document.getElementById;
  const badge = { className: '', textContent: '' };
  let requestIsCurrent = true;
  let finishResultUrl;
  let markResultUrlStarted;
  const resultUrlStarted = new Promise(resolve => { markResultUrlStarted = resolve; });
  const resultUrlReply = new Promise(resolve => { finishResultUrl = resolve; });
  const calledUrls = [];
  cloudComputeState.enabled = true;
  try {
    globalThis.document.getElementById = id => id === 'status-badge' ? badge : null;
    globalThis.fetch = async (url) => {
      calledUrls.push(url);
      if (url === '/api/v1/jobs') {
        return {
          ok: true,
          status: 200,
          headers: { get: () => 'application/json' },
          json: async () => ({ job_id: 'job-stale-url', status: 'succeeded' }),
        };
      }
      if (url === '/api/v1/jobs/job-stale-url/result-url') {
        markResultUrlStarted();
        return resultUrlReply;
      }
      throw new Error(`Unexpected URL ${url}`);
    };

    const pending = computeApi('build_atlas', { spec: true }, {
      statusIsCurrent: () => requestIsCurrent,
    });
    await resultUrlStarted;
    requestIsCurrent = false;
    setStatus('done', 'Newer request done');
    finishResultUrl({
      ok: true,
      status: 200,
      headers: { get: () => 'application/json' },
      json: async () => ({ result_url: 'https://artifacts.example/must-not-run.json' }),
    });

    assert.equal(await pending, null);
    assert.equal(calledUrls.includes('https://artifacts.example/must-not-run.json'), false);
    assert.equal(badge.textContent, 'Newer request done');
    assert.doesNotMatch(badge.className, /error/);
    setStatus('working', 'cleanup');
    passed += 1;
    console.log('  ok - stale cloud owner stops after the result-URL request settles');
  } finally {
    cloudComputeState.enabled = false;
    globalThis.fetch = priorFetch;
    globalThis.document.getElementById = priorGetElementById;
  }
}

{
  const priorFetch = globalThis.fetch;
  const priorGetElementById = globalThis.document.getElementById;
  const badge = { className: '', textContent: '' };
  let requestIsCurrent = true;
  let finishDirectGet;
  let markDirectGetStarted;
  const directGetStarted = new Promise(resolve => { markDirectGetStarted = resolve; });
  const directGetReply = new Promise(resolve => { finishDirectGet = resolve; });
  let directOptions = null;
  const calledUrls = [];
  cloudComputeState.enabled = true;
  try {
    globalThis.document.getElementById = id => id === 'status-badge' ? badge : null;
    globalThis.fetch = async (url, options) => {
      calledUrls.push(url);
      if (url === '/api/v1/jobs') {
        return {
          ok: true,
          status: 200,
          headers: { get: () => 'application/json' },
          json: async () => ({ job_id: 'job-stale-get', status: 'succeeded' }),
        };
      }
      if (url === '/api/v1/jobs/job-stale-get/result-url') {
        return {
          ok: true,
          status: 200,
          headers: { get: () => 'application/json' },
          json: async () => ({ result_url: 'https://artifacts.example/job-stale-get.json' }),
        };
      }
      if (url === 'https://artifacts.example/job-stale-get.json') {
        directOptions = options;
        markDirectGetStarted();
        return directGetReply;
      }
      throw new Error(`Unexpected URL ${url}`);
    };

    const pending = computeApi('build_atlas', { spec: true }, {
      statusIsCurrent: () => requestIsCurrent,
    });
    await directGetStarted;
    requestIsCurrent = false;
    setStatus('done', 'Replacement result done');
    finishDirectGet({
      ok: true,
      status: 200,
      headers: { get: () => 'application/json' },
      json: async () => ({ obsolete: true }),
    });

    assert.equal(await pending, null);
    assert.deepEqual(directOptions, { method: 'GET' });
    assert.equal(calledUrls.some(url => url === '/api/v1/jobs/job-stale-get/result'), false);
    assert.equal(badge.textContent, 'Replacement result done');
    assert.doesNotMatch(badge.className, /error/);

    let finishNextRequest;
    globalThis.fetch = async () => new Promise(resolve => { finishNextRequest = resolve; });
    const nextRequest = api('version', {});
    assert.equal(badge.textContent, 'Computing...', 'stale cloud activity must settle exactly once');
    finishNextRequest({
      ok: true,
      status: 200,
      headers: { get: () => 'application/json' },
      json: async () => ({ version: 'test' }),
    });
    await nextRequest;
    setStatus('working', 'cleanup');
    passed += 1;
    console.log('  ok - stale cloud owner stops after direct GET and settles activity once');
  } finally {
    cloudComputeState.enabled = false;
    globalThis.fetch = priorFetch;
    globalThis.document.getElementById = priorGetElementById;
  }
}

{
  const priorFetch = globalThis.fetch;
  try {
    globalThis.fetch = async () => ({
      ok: false,
      status: 503,
      headers: { get: () => 'application/json' },
      json: async () => ({ status: 'warming' }),
    });
    await assert.rejects(() => apiSilent('version', {}), /Backend request failed \(503\)/);
    await assert.rejects(() => api('build_model', {}), /Backend request failed \(503\)/);
    passed += 1;
    console.log('  ok - HTTP failures cannot pass as successful JSON payloads');
  } finally {
    globalThis.fetch = priorFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  const request = {
    schema_version: 'bne-rop-shape-optimize-request/v1.0.0',
    edit_intent: { kind: 'broaden_both_ears' },
  };
  let calledUrl = null;
  try {
    globalThis.fetch = async (url, options) => {
      calledUrl = url;
      assert.equal(options.method, 'POST');
      assert.deepEqual(JSON.parse(options.body), request);
      return {
        ok: true,
        status: 200,
        headers: { get: () => 'application/json; charset=utf-8' },
        json: async () => ({ schema_version: 'bne-rop-shape-optimization/v1.0.0' }),
      };
    };
    const result = await optimizeRopShape(request);
    assert.equal(calledUrl, '/api/v1/rop_shape_optimize');
    assert.notEqual(calledUrl, '/api/rop_shape_optimize');
    assert.equal(result.schema_version, 'bne-rop-shape-optimization/v1.0.0');
    await assert.rejects(() => optimizeRopShape(null), /request must be an object/);
    globalThis.fetch = async () => ({
      ok: true,
      status: 200,
      headers: { get: () => 'application/json' },
      json: async () => null,
    });
    await assert.rejects(() => optimizeRopShape(request), /invalid ROP shape optimization payload/);
    passed += 1;
    console.log('  ok - ROP shape client uses only the canonical versioned route');
  } finally {
    globalThis.fetch = priorFetch;
  }
}

console.log(`\nAll ${passed} model request tests passed.`);

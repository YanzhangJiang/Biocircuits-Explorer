# 0004: Serialize job state by stable stripes and bound the record cache

- Status: accepted in the 2026-07-11 working tree
- Date: 2026-07-11
- Verified against: current working tree; commit pending
- Historical implementation anchor: `1177a3d`
- Owners: `backend-runtime`, `batch-hpc`
- Supersedes: process-wide serialization of canonical job state through
  `JOBS_LOCK`
- Superseded by: none

## Problem in plain language

One slow job file must not pause every unrelated job in the process. The old
registry lock covered canonical reads, JSON parsing, snapshots, atomic writes,
directory synchronization, and status-projection repair. A delayed filesystem
operation therefore blocked unrelated status reads, transitions, cancellation,
and cloud reconciliation. The same in-memory dictionary also retained every
job record ever submitted or cold-loaded.

The decision is to serialize state only for the same canonical job identity,
keep the process-wide lock for short registry bookkeeping, and treat the
in-memory record dictionary as a bounded, disposable cache over canonical
files.

## Context and observed constraints

- [`webapp/src/jobs.jl`](../../webapp/src/jobs.jl) owns a canonical
  `record.json`, a rebuildable public `status.json`, local worker/task metadata,
  AWS polling and cancellation claims, and atomic file publication.
- Canonical writes may execute file `fsync`, rename, parent-directory `fsync`,
  and projection repair. These operations can block independently for one job
  and must not run while the global registry lock is held.
- A mutable cached record cannot be read or changed concurrently for the same
  job. Snapshots must be made while that job's state owner is held, but they do
  not need to exclude unrelated jobs.
- A lock dictionary keyed by every historical job would reproduce the
  unbounded-memory problem. A fixed stripe table bounds lock metadata while
  still giving each job one deterministic in-process owner.
- Local task handles, cancellation tokens, admission reservations, AWS initial
  submission ownership, and describe/cancel claims are process lifecycle state,
  not cached record content. They must survive record eviction.
- A nonterminal local job or prepared AWS submission cold-loaded after a real
  process restart needs recovery. The same record evicted inside a live process
  must not be mistaken for a restart.
- The concurrency and eviction fixtures are in
  [`webapp/test/jobs_cache_concurrency_contract.jl`](../../webapp/test/jobs_cache_concurrency_contract.jl).

## Decision

### Use a fixed stable stripe table

The process owns 128 `ReentrantLock` stripes. A stable FNV-1a byte hash maps the
canonical job ID to one stripe; Julia's process-randomized `hash` is not used.
Every canonical load, recovery decision, transition, commit, projection check,
and record snapshot for a job runs under that stripe. Jobs mapped to different
stripes can progress independently; a deliberate stripe collision serializes
those jobs without allocating another lock.

The stripes are a single-process correctness mechanism. They do not claim
cross-process or multi-broker exclusion over a shared directory.

### Keep the registry lock short

`JOBS_LOCK` owns only short in-memory operations:

- bounded record-cache and LRU metadata;
- local task, cancellation-token, and admission registries;
- AWS initial-submission, describe, reconciliation, and cancellation claims;
  and
- small projection-dirty metadata.

Canonical file reads, JSON parsing, `deepcopy`, rename/file/directory `fsync`,
projection inspection or repair, solver work, semaphore waiting, and AWS/S3
calls do not execute under `JOBS_LOCK`.

The allowed nesting direction is a job stripe followed by a short registry
operation. Code holding the registry or durability metadata lock must not
acquire a job stripe. External work releases both before it begins and reloads
canonical state under the stripe before merging a result.

### Make `JOBS` a bounded LRU cache

`record.json` remains the source of truth. `JOBS` and its access-clock table
have identical keys and a hard capacity. The strict
`BIOCIRCUITS_EXPLORER_JOB_CACHE_CAPACITY` setting defaults to 1,024 entries and
accepts values from 1 through 65,536.

A cache hit performs an O(1) lookup and access-clock update. A capacity change
or overflowing publication performs eviction; ordinary hits do not scan the
cache. Eviction removes only the cached record and LRU timestamp. It never
deletes canonical files, status-projection repair state, durability state,
tasks, tokens, admission reservations, or AWS claims.

Running and queued records are not pinned. Pinning could make the declared hard
limit false during a burst. A caller already holding the stripe may continue
using its record reference; its next commit republishes the current record.
Later callers cold-load canonical state under the same stripe.

### Separate live ownership from cache residency

Cold local-job recovery checks the independent task/token/admission registries.
A prepared AWS submission checks an independent initial-submission owner that
is registered before publication and cleared in a `finally` block. Only a
nonterminal record with no matching live process owner is settled as interrupted
after restart.

This separation permits eviction during local execution, S3 input publication,
AWS polling, reconciliation, or cancellation without inventing a restart or
duplicating an external action.

### Validate cold canonical identity

A cold-loaded payload must be a JSON object with an integer revision contract
and a `job_id` equal to the requested directory identity. A mismatched record is
rejected and is not published under the requested cache key. Projection repair
continues to compare full public content and revision against canonical state.

## Alternatives considered

### Keep one global lock and shorten only obvious external calls

AWS and solver calls were already outside the lock, but canonical disk reads,
snapshots, writes, and repair could still block every job. This was rejected
because the observed bottleneck was the remaining file/state critical section,
not only external services.

### Allocate one lock per job ID

This gives ideal independence between IDs, but lock lifecycle then needs its own
safe reclamation protocol and can grow with the durable job population. Fixed
stripes provide bounded metadata and simpler ownership at the cost of rare,
deterministic false sharing.

### Pin active records in the LRU

Pinning would make active access cheap, but enough queued or running jobs could
exceed the configured capacity and invalidate the hard-bound claim. Independent
process registries make pinning unnecessary.

### Reconstruct state only from memory until restart

This would avoid cold reads but make eviction impossible and leave memory as an
undeclared second authority. It was rejected because the canonical atomic file
already owns restart and durability semantics.

### Scan the full LRU table on every hit

An early repair-oriented design normalized keys and pruned on every access. At
the maximum capacity it would turn each hit into an O(n) global critical
section. Cache helpers instead maintain their invariants on publication/removal,
and pruning runs only when capacity changes or overflows.

## Consequences

### Benefits

- A blocked canonical read, rename, directory synchronization, projection
  repair, or large snapshot for one job does not hold the process-wide registry
  lock or stop a job on another stripe.
- Same-job transitions, cancellation, finish, and cold recovery remain
  serialized and revision-monotonic.
- Concurrent cold misses for one job parse canonical state once.
- Cache memory has an explicit hard entry bound, while evicted records retain
  complete canonical state and can be reloaded.
- Active local and AWS workflows retain their process ownership independently
  of record residency.
- Cache-hit work is constant time apart from the practically unreachable access
  clock renormalization.

### Costs and risks

- Different job IDs that hash to the same stripe serialize even though their
  files are unrelated.
- The capacity is an entry count, not a byte budget. Large allowed request
  specifications can still make 1,024 records consume substantial memory.
- LRU access order is process-local and disappears on restart, which is safe
  because it is not part of canonical semantics.
- The cache and stripes do not coordinate two broker processes sharing one job
  store. That deployment remains unsupported without another ownership layer.
- Tests and maintenance helpers that directly mutate `JOBS` must also maintain
  LRU metadata; production code uses the cache helpers instead.

## Migration and rollback

Existing canonical files need no rewrite. Missing legacy `state_revision`
continues to read as revision zero under the existing migration rule. The first
cold load validates identity, repairs the status projection if needed, and
publishes the record into the bounded cache.

Operators may set `BIOCIRCUITS_EXPLORER_JOB_CACHE_CAPACITY` before the process
starts. Invalid or out-of-range values fail before a new canonical job is
published. A later valid capacity change is applied on the next cache access and
evicts the least-recently-used entries as needed.

Rolling back to an implementation that assumes `JOBS` retains every active
record is unsafe once eviction is enabled. A replacement must preserve the
independent task/token/admission/AWS-owner checks or disable eviction before the
old process starts.

## Verification

From the repository root:

```text
julia --project=webapp webapp/test/jobs_cache_concurrency_contract.jl
julia --project=webapp webapp/test/jobs_cancellation_contract.jl
julia --project=webapp webapp/test/jobs_artifact_validity_contract.jl
julia --project=webapp webapp/test/jobs_submission_reconciliation_contract.jl
JULIA_NUM_THREADS=auto julia --project=webapp webapp/test/runtests.jl
```

The focused cache contract proves cross-job progress while one job blocks on
I/O, same-job serialization, stable stripe mapping, single-flight cold load,
canonical-ID rejection, exact LRU eviction/reload, projection repair after
eviction, hard-cap/key invariants under stress, active local-job eviction,
post-commit cache configuration behavior, and AWS submit/cancel ownership across
eviction. Julia 1.10 and the current Julia line both execute the focused job
contracts; the broader suite remains the final integration gate.

## Follow-ups

- [ ] Add a byte-aware cache policy only with a deterministic size/accounting
  contract; do not infer memory from entry count alone (`backend-runtime`).
- [ ] Define multi-broker canonical-store ownership before two processes share
  one job directory (`batch-hpc`).
- [ ] Revisit the fixed stripe count only with measured contention and the same
  stable-mapping/race contracts (`backend-runtime`).

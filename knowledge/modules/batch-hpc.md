# Batch and HPC

`module_id: batch-hpc`

## Purpose

Run long work outside an HTTP request: manage local asynchronous jobs through a
race-safe backend state machine, and provide headless Slurm entry points for
atlas construction, phenotype generation, merging, and benchmarks.

Dependency compatibility now has a narrow, testable claim. CI is configured to
select and load the Julia 1.10 and 1.12 lock files. A local Julia 1.10.11 audit
also selected `Manifest-v1.10.toml` and loaded the headless engine. None of that
proves a real scheduler or cluster filesystem.

## Non-goals

- It does not own numerical definitions, atlas identity, or paper claim
  strength; those belong to engine, atlas, and research contracts.
- A dependency lock that loads is not evidence that a full campaign completes
  under a site's scheduler and storage policies.
- It does not prove that a cluster is correctly provisioned.

## Owner paths

- Job state machine and routes: `webapp/src/jobs.jl`,
  `webapp/src/routing.jl`, `webapp/src/config.jl`
- Cooperative cancellation: `webapp/src/cancellation.jl`
- Browser API client: `webapp/public/js/api.js`
- Result envelope and asynchronous commit marker:
  `schemas/result-artifact.schema.json` and
  `schemas/job-result-manifest.schema.json`
- Headless environment and version-specific locks: `webapp_hpc/Project.toml`,
  `webapp_hpc/Manifest-v1.10.toml`, `webapp_hpc/Manifest.toml`,
  `Bnc_julia_headless/`
- Compatibility gate: `.github/workflows/ci.yml`, job
  `test-hpc-environment`
- Cluster entrypoints: `slurm/`

## Inputs

- A normalized job request and a local execution mode (`local` or
  `local_async`).
- For offline work: a tracked checkout, atlas specification, scheduler
  environment, matching Julia lock, and explicit input/output locations.

## Outputs

- Persisted job records with monotonic status, progress, cancellation outcome,
  and result/error metadata under the configured local job store. New jobs
  publish a bounded result manifest after the result file and before the job
  reports success.
- Atlas SQLite files or independent shards, merge summaries, phenotype shards,
  dataset splits, benchmark reports, logs, and scheduler exit status.

## Contract sources

- State transitions, locking, cancellation, persistence, and route payloads:
  `webapp/src/jobs.jl`
- Cooperative cancellation: `webapp/src/cancellation.jl` and its callers
- Result publication protocol: `schemas/result-artifact.schema.json` and
  `schemas/job-result-manifest.schema.json`
- Shared headless numerical source: `Bnc_julia_headless/src/BindingAndCatalysis.jl`
- Julia compatibility and lock selection: `webapp_hpc/Project.toml`, both
  manifests, and the `test-hpc-environment` CI job
- Scheduler behavior: executable files under `slurm/`, not readiness wording in
  comments or README files

## Tests

- `webapp/test/jobs_cancellation_contract.jl` covers queued/start and
  cancel/finish races, terminal snapshot immutability, task registration,
  process-restart recovery of local jobs, retired-executor settlement of
  historical records, atomic canonical publication, directory-durability
  retries, state revisions, and projection repair.
- `webapp/test/jobs_cache_concurrency_contract.jl` covers cross-job progress
  during blocked persistence, same-job serialization, cold-load single-flight,
  wrong-directory identity rejection, hard LRU eviction/reload, and projection
  repair after eviction.
- `webapp/test/cooperative_cancel_checkpoints_contract.jl` covers cancellation
  tokens across dispatch and parallel workers.
- `webapp/test/runtests.jl` covers the local asynchronous job lifecycle through
  the HTTP route surface.
- `webapp/test/jobs_artifact_validity_contract.jl` covers local worker
  publication: directory-durability ordering, manifest-protocol commit order,
  and failure injection.
- `webapp/test/model-request.test.mjs` covers browser request identity,
  payload enrichment and recovery, canonical v1 routing, and stale-status
  ownership.
- The HPC CI command asserts that Julia below 1.11 selects
  `Manifest-v1.10.toml`, Julia 1.12 selects `Manifest.toml`, instantiates the
  selected environment, and imports `BindingAndCatalysis`.
- Local evidence on this audit host: Julia 1.10.11 selected
  `Manifest-v1.10.toml`, Julia 1.12.6 selected `Manifest.toml`, and both loaded
  `BindingAndCatalysis`.

## CI

`.github/workflows/ci.yml` configures `test-hpc-environment` for Julia 1.10 and
1.12. Each matrix entry verifies the expected lock selection, instantiates it,
and loads the shared headless engine. The main Julia job also runs the local
job contracts.

No checked-in workflow submits a SLURM job. Version compatibility is
CI-configured; real HPC execution remains unknown.

## Invariants

- Julia 1.10 uses `webapp_hpc/Manifest-v1.10.toml`; Julia 1.12 uses
  `webapp_hpc/Manifest.toml`. CI must fail if Julia selects the wrong lock or the
  headless engine cannot load.
- The headless wrapper loads the shared numerical source with visualization
  disabled; it must not fork numerical semantics.
- Job status is monotonic once terminal; late progress or completion cannot
  mutate a terminal snapshot.
- A succeeded local job is not complete until the v1 result manifest exists and
  matches the canonical job identity, submitted config hash, result path, byte
  length, and result SHA-256. The worker publishes result, then manifest, then
  succeeded status. Only records without a protocol field use the legacy inline
  JSON validator. Missing or invalid artifacts fail the record at read time.
  Manifest `created_at` values use canonical second-precision UTC with a
  trailing `Z`.
- Local asynchronous execution admits a bounded total number of queued/running
  jobs and uses a separate fixed semaphore for active computation. Capacity
  exhaustion is a retryable structured 429 and occurs before job-store
  publication.
- On macOS/Linux, canonical job files commit through same-directory file fsync,
  one no-fallback atomic rename, and parent-directory fsync. A post-rename
  directory failure degrades readiness until retried but cannot roll the live
  state back. Local artifacts are stricter: input/result/manifest/success
  publication cannot advance until the relevant directory is durable.
- Canonical records own a monotonic `state_revision`; local `status.json` is a
  repairable projection with the same revision and full public content. It is
  rebuilt from canonical state after corruption or drift, and is never used as
  a fallback when `record.json` is absent or invalid.
- Canonical state work is serialized by a stable 128-stripe job-ID mapping.
  The process-wide registry lock is limited to short cache/claim/owner metadata
  sections; disk reads, JSON parsing, snapshots, atomic publication, and
  projection repair run outside it. The `JOBS` record cache is strict LRU state
  (1,024 records by default, hard maximum 65,536), and active local records may
  be evicted because durable canonical state and independent task ownership
  drive recovery.
- ROP shape jobs normalize their submitted request once before persistence and
  worker handoff. The resulting config hash is stored in the canonical record;
  verification never regenerates timestamped provenance to guess that identity.
- A nonterminal `local_async` record cold-loaded without a live in-process
  worker or cancellation token settles durably after restart: queued/running
  becomes failed and cancel-requested becomes cancelled. A nonterminal record
  written by a retired executor fails closed with `executor_retired` on cold
  load; terminal historical records are untouched.
- Local cancellation is cooperative and must be observed at explicit compute
  checkpoints; it does not asynchronously interrupt arbitrary Julia work.
- Scale-out atlas builds use one SQLite writer per shard and merge afterward;
  multiple jobs do not write one SQLite database concurrently.
- The training scheduler entrypoint intentionally exits unsuccessfully until a
  real training implementation exists.

## Known gaps

- P2 — The 1.10/1.12 dependency gate loads the environment and engine but does
  not execute every atlas, phenotype, merge, or benchmark campaign on both
  Julia lines.
- P2 — No real SLURM submission has been verified. Site modules, partitions,
  resource requests, dependency chains, resume behavior, filesystem semantics,
  and partial-copy recovery remain cluster-specific and unknown.
- P2 — Scheduler comments and historical phase labels can drift from executable
  behavior; a run is evidence only when its revision, configuration, logs,
  outputs, and verification command are captured together.
- P2 — Site-specific campaign scripts are intentionally untracked. Capture a
  portable, reviewed entry point before promoting any workflow into this module.

## Change protocol

1. Regenerate the matching version-specific lock intentionally and keep the CI
   selection assertion when changing headless dependencies or Julia support.
2. Preserve the transition table and add an adversarial race test before
   changing job lifecycle, locks, retries, or cancellation.
3. Publish state only through guarded transition helpers.
4. Change pipeline output only with a versioned result/manifest contract and a
   verifier that detects incomplete shards or mixed source revisions.
5. Treat each cluster run as unverified until its configuration, code
   revision, logs, outputs, and verification command are captured together.

## Verified against

- Source commit: `f2ca13c`
- Evidence inspected: job state/contracts, result schema, both HPC lock
  files, headless wrapper, Slurm entrypoints, and CI wiring.
- Local evidence: Julia 1.10.11 selected `Manifest-v1.10.toml`, Julia 1.12.6
  selected `Manifest.toml`, and both imported the shared headless engine.
- Boundary: Julia 1.10/1.12 lock selection and engine loading are CI-configured;
  no actual Slurm flow is claimed verified; historical workstation campaigns
  remain outside the current contract. The retired AWS Batch lane is recorded
  in `decisions/0003-aws-batch-at-most-once-submission.md` as superseded.

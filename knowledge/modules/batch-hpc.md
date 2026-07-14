# Batch and HPC

`module_id: batch-hpc`

## Purpose

Run long work outside an HTTP request: manage local or AWS Batch jobs through a
race-safe backend state machine, and provide headless Slurm entry points for
atlas construction, phenotype generation, merging, and benchmarks.

Dependency compatibility now has a narrow, testable claim. CI is configured to
select and load the Julia 1.10 and 1.12 lock files. A local Julia 1.10.11 audit
also selected `Manifest-v1.10.toml` and loaded the headless engine. None of that
proves a real scheduler, cluster filesystem, or AWS account.

## Non-goals

- It does not own numerical definitions, atlas identity, or paper claim
  strength; those belong to engine, atlas, and research contracts.
- A dependency lock that loads is not evidence that a full campaign completes
  under a site's scheduler and storage policies.
- It does not prove that an AWS account, Batch queue, object store, quota table,
  or cluster is correctly provisioned.

## Owner paths

- Job state machine and routes: `webapp/src/jobs.jl`,
  `webapp/src/routing.jl`, `webapp/src/config.jl`
- Worker and browser controls: `webapp/scripts/run_batch_job.jl`,
  `webapp/public/js/cloud-compute.js`, and `webapp/public/js/api.js`
- Result envelope and asynchronous commit marker:
  `schemas/result-artifact.schema.json` and
  `schemas/job-result-manifest.schema.json`
- Headless environment and version-specific locks: `webapp_hpc/Project.toml`,
  `webapp_hpc/Manifest-v1.10.toml`, `webapp_hpc/Manifest.toml`,
  `Bnc_julia_headless/`
- Compatibility gate: `.github/workflows/ci.yml`, job
  `test-hpc-environment`
- Cluster entrypoints: `slurm/`
- Cloud bootstrap and validation: `deploy/setup_aws_batch.sh`,
  `deploy/validate_aws_batch_state.py`, and `deploy/aws-runtime.env.example`

## Inputs

- A normalized job request, authenticated user identity when configured, and an
  execution mode selecting local or AWS Batch.
- For cloud work: trusted runtime queue/definition/artifact settings, CLI,
  credentials, and optional quotas. Request-level overrides are ignored unless
  the operator explicitly enables them.
- For offline work: a tracked checkout, atlas specification, scheduler
  environment, matching Julia lock, and explicit input/output locations.

## Outputs

- Persisted job records with monotonic status, progress, cancellation outcome,
  and result/error metadata.
- Cloud worker input/status/result objects under a per-user, per-job artifact
  prefix. New AWS jobs also publish a bounded result manifest after the result
  object and before the worker reports success.
- Atlas SQLite files or independent shards, merge summaries, phenotype shards,
  dataset splits, benchmark reports, logs, and scheduler exit status.

## Contract sources

- State transitions, locking, cancellation, AWS translation, persistence, and
  route payloads: `webapp/src/jobs.jl`
- Cooperative cancellation: `webapp/src/cancellation.jl` and its callers
- Worker artifact protocol: `webapp/scripts/run_batch_job.jl`,
  `schemas/result-artifact.schema.json`, and
  `schemas/job-result-manifest.schema.json`
- Shared headless numerical source: `Bnc_julia_headless/src/BindingAndCatalysis.jl`
- Julia compatibility and lock selection: `webapp_hpc/Project.toml`, both
  manifests, and the `test-hpc-environment` CI job
- Scheduler behavior: executable files under `slurm/`, not readiness wording in
  comments or README files

## Tests

- `webapp/test/jobs_cancellation_contract.jl` covers queued/start and
  cancel/finish races, terminal snapshot immutability, task registration,
  retry/escalation, process-restart recovery of local-only jobs, atomic
  canonical publication, directory-durability retries, state revisions,
  projection repair, and the rule that AWS calls do not hold the job lock.
- `webapp/test/jobs_submission_reconciliation_contract.jl` covers the durable
  AWS submission plan, the single SubmitJob boundary, ambiguous-response
  adoption, paginated exact-name discovery, strict candidate identity,
  zero/multiple-candidate outcomes, cancellation races, legacy records,
  concurrent reconciliation, and recovery after runtime configuration drift.
- `webapp/test/cooperative_cancel_checkpoints_contract.jl` covers cancellation
  tokens across dispatch and parallel workers.
- `webapp/test/runtests.jl` covers local jobs and mocked AWS Batch/S3 behavior,
  including the explicit opt-in for request-supplied cloud settings and
  resources.
- `webapp/test/jobs_artifact_validity_contract.jl` adversarially covers S3
  not-found versus retryable probe/download errors, submit-time ROP config
  identity, strict manifest fields, byte length/SHA/media-type checks, bounded
  manifest reads, canonical UTC manifest timestamps,
  result-before-manifest publication order, legacy-record fallback, absence of
  broker result downloads for the new protocol, and lock-free external
  verification I/O.
- `webapp/test/model-request.test.mjs` covers browser cloud-job ownership,
  best-effort stale cancellation, terminal-state retirement, bounded retryable
  polling, retry-budget reset, and completed-result retrieval. Result retries
  obtain a fresh pre-signed URL, keep direct GETs header-free, validate the JSON
  media type, and never fall back to relaying a large body through the broker.
- `tests/test_setup_aws_batch.py` and
  `tests/test_validate_aws_batch_state.py` cover fail-closed setup/reconciliation
  with fixtures and mocked commands.
- The HPC CI command asserts that Julia below 1.11 selects
  `Manifest-v1.10.toml`, Julia 1.12 selects `Manifest.toml`, instantiates the
  selected environment, and imports `BindingAndCatalysis`.
- Local evidence on this audit host: Julia 1.10.11 selected
  `Manifest-v1.10.toml`, Julia 1.12.6 selected `Manifest.toml`, and both loaded
  `BindingAndCatalysis`.

## CI

`.github/workflows/ci.yml` configures `test-hpc-environment` for Julia 1.10 and
1.12. Each matrix entry verifies the expected lock selection, instantiates it,
and loads the shared headless engine. The main Julia job also runs the local and
mocked-cloud job contracts.

No checked-in workflow submits a SLURM job, provisions AWS, submits a real
Batch worker, transfers an artifact through live S3, or exercises a quota
table. Version compatibility is CI-configured; real HPC/cloud execution remains
unknown.

## Invariants

- Julia 1.10 uses `webapp_hpc/Manifest-v1.10.toml`; Julia 1.12 uses
  `webapp_hpc/Manifest.toml`. CI must fail if Julia selects the wrong lock or the
  headless engine cannot load.
- The headless wrapper loads the shared numerical source with visualization
  disabled; it must not fork numerical semantics.
- Job status is monotonic once terminal; late progress or completion cannot
  mutate a terminal snapshot.
- Each new AWS job persists its complete queue/definition/name/tag/command and
  artifact plan, Batch region, and optional account identity before remote I/O.
  After the input object is published, a canonical `dispatch_started` rename
  permits one application-level SubmitJob call only after the exact parent
  directory is durably synced; an unconfirmed boundary performs zero submits
  and becomes reconciliation-only. SDK attempts remain fixed to one. A missing
  or ambiguous response is reconciled by exact job name and strict
  DescribeJobs identity; every describe chunk must completely and uniquely
  cover its requested IDs before any candidate can be adopted. Persisted full
  ARNs require full equality, while name-to-ARN matches retain the persisted
  region/account boundary. Zero candidates remain retryable and become
  explicitly `unknown` after a bounded observation count; multiple exact
  candidates become `conflict` without choosing a winner.
  Legacy records without either an external ID or this identity are marked
  `legacy_submission_unknown` instead of guessed from current configuration.
- AWS Batch `SUCCEEDED` is not terminal application success until the v1 result
  manifest exists and matches the canonical job identity, submitted config
  hash, result URI, byte length, JSON media type, positive payload-key count,
  and result-object SHA metadata. The worker publishes result, then manifest,
  then succeeded status. Only records without a protocol field use the legacy
  inline JSON validator. Explicit not-found and invalid artifacts fail;
  permissions, CLI, network, and throttling preserve a nonterminal state for
  retry. A broker reading a persisted unknown future protocol also waits for a
  compatible verifier; a worker receiving an unsupported protocol fails, so
  rolling deployment order is worker image/job definition before broker.
  Manifest `created_at` values use canonical second-precision UTC with a
  trailing `Z`.
- A completed cloud result is not parsed and serialized again by the broker.
  The browser asks for `/api/jobs/<id>/result-url`, downloads the object
  directly, requires an `application/json` response, and never silently falls
  back to the broker result route. A retryable result-URL or direct-download
  failure permits one bounded retry using a newly obtained pre-signed URL. The
  artifact bucket therefore needs the documented GET/HEAD browser-origin rule
  for each deployed UI origin.
- A browser cloud-job request is current only while its owner predicate returns
  true; a thrown predicate fails closed. Once a job ID exists, owner loss makes
  one best-effort cancellation request for a last-known nonterminal job and
  settles browser activity before waiting for cancellation. Known terminal jobs
  are retired without cancellation. Polling retries at most two consecutive
  explicitly retryable failures with bounded backoff, checks owner state around
  every wait and response, and resets the retry budget after a successful poll.
- Local asynchronous execution admits a bounded total number of queued/running
  jobs and uses a separate fixed semaphore for active computation. Capacity
  exhaustion is a retryable structured 429 and occurs before quota consumption
  or job-store publication.
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
  sections; disk reads, JSON parsing, snapshots, atomic publication, projection
  repair, and external calls run outside it. The `JOBS` record cache is strict
  LRU state (1,024 records by default, hard maximum 65,536), and active local or
  AWS records may be evicted because durable canonical state and independent
  task/submission ownership drive recovery.
- ROP shape jobs normalize their submitted request once before persistence and
  worker handoff. The resulting config hash is stored in the canonical record;
  verification never regenerates timestamped provenance to guess that identity.
- A nonterminal `local_async` record cold-loaded without a live in-process
  worker or cancellation token settles durably after restart: queued/running
  becomes failed and cancel-requested becomes cancelled. AWS Batch records stay
  nonterminal until refreshed from their external owner.
- External AWS CLI calls never execute while the in-process job registry lock
  is held.
- A persisted cancel-dispatch claim prevents duplicate remote cancellation; a
  failed dispatch releases the claim for retry and an abandoned claim expires.
- Local cancellation is cooperative and must be observed at explicit compute
  checkpoints; it does not asynchronously interrupt arbitrary Julia work.
- Scale-out atlas builds use one SQLite writer per shard and merge afterward;
  multiple jobs do not write one SQLite database concurrently.
- Request-supplied AWS queue, definition, artifact prefix, job-name prefix,
  environment, vCPU, and memory settings are ignored unless
  `BIOCIRCUITS_EXPLORER_ALLOW_AWS_BATCH_REQUEST_CONFIG` is explicitly enabled.
- The training scheduler entrypoint intentionally exits unsuccessfully until a
  real training implementation exists.

## Known gaps

- P2 — The 1.10/1.12 dependency gate loads the environment and engine but does
  not execute every atlas, phenotype, merge, or benchmark campaign on both
  Julia lines.
- P2 — No real SLURM submission has been verified. Site modules, partitions,
  resource requests, dependency chains, resume behavior, filesystem semantics,
  and partial-copy recovery remain cluster-specific and unknown.
- P2 — No live AWS Batch, S3, Cognito, quota, or IAM integration has been run;
  current cloud evidence is source-level and mocked.
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
3. Keep external calls outside locks and publish state only through guarded
   transition helpers.
4. Change pipeline output only with a versioned result/manifest contract and a
   verifier that detects incomplete shards or mixed source revisions.
5. Treat each cluster or cloud run as unverified until its configuration, code
   revision, logs, outputs, and verification command are captured together.

## Verified against

- Source commit: `f2ca13c`
- Evidence inspected: job state/contracts, worker/result schema, both HPC lock
  files, headless wrapper, Slurm entrypoints, AWS setup validators, and CI
  wiring.
- Local evidence: Julia 1.10.11 selected `Manifest-v1.10.toml`, Julia 1.12.6
  selected `Manifest.toml`, and both imported the shared headless engine.
- Boundary: Julia 1.10/1.12 lock selection and engine loading are CI-configured;
  no actual Slurm or live AWS Batch/S3/Cognito/quota flow is claimed verified;
  historical workstation campaigns remain outside the current contract.

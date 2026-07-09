# Batch and HPC

`module_id: batch-hpc`

## Purpose

Run long work outside a request thread: manage local or AWS Batch jobs through a
race-safe backend state machine, and provide headless workstation/SLURM entry
points for atlas construction, phenotype generation, merging, and benchmarks.

## Non-goals

- It does not own numerical definitions, atlas identity, or paper claim
  strength; those belong to engine, atlas, and research contracts.
- It does not prove a cluster site, AWS account, queue, object store, or resource
  policy is correctly provisioned.
- Scheduler comments saying “ready” or “pending” are not status authority.

## Owner paths

- Job state machine and routes: `webapp/src/jobs.jl`,
  `webapp/src/routing.jl`, `webapp/src/config.jl`
- Worker and browser controls: `webapp/scripts/run_batch_job.jl`,
  `webapp/public/js/cloud-compute.js`
- Result envelope: `schemas/result-artifact.schema.json`
- Headless environment: `webapp_hpc/`, `Bnc_julia_headless/`
- Cluster entrypoints: `slurm/`
- Workstation orchestration: `workstation/`
- Cloud bootstrap boundary: `deploy/setup_aws_batch.sh` and
  `deploy/aws-runtime.env.example`

## Inputs

- A normalized job request, authenticated user identity when configured, and an
  execution mode selecting local or AWS Batch.
- For cloud work: queue, job definition, artifact prefix, CLI, credentials, and
  optional quota configuration from runtime environment.
- For offline pipelines: a transferred tracked checkout, atlas spec, scheduler
  environment, headless Julia project, and explicit output locations.

## Outputs

- Persisted job records with monotonic status, progress, cancellation outcome,
  and result/error metadata.
- Cloud worker input/status/result objects under a per-job artifact prefix.
- Atlas SQLite files or independent shards, merge summaries, phenotype shards,
  dataset splits, benchmark reports, logs, and scheduler exit status.

## Contract sources

- State transitions, lock boundary, cancellation, AWS translation, persistence,
  and route payloads: `webapp/src/jobs.jl`
- Cooperative compute checkpoints: `webapp/src/cancellation.jl` and the callers
  covered by `webapp/test/cooperative_cancel_checkpoints_contract.jl`
- Worker artifact protocol: `webapp/scripts/run_batch_job.jl` and
  `schemas/result-artifact.schema.json`
- Headless source sharing: `Bnc_julia_headless/src/BindingAndCatalysis.jl` and
  `webapp_hpc/Project.toml`
- Scheduler behavior: the executable files in `slurm/`, not `slurm/README.md`

## Tests

- `webapp/test/jobs_cancellation_contract.jl` covers queued/start and
  cancel/finish races, terminal snapshot immutability, task registration,
  retries/escalation, and the rule that AWS calls do not hold the job lock.
- `webapp/test/cooperative_cancel_checkpoints_contract.jl` covers cancellation
  tokens across dispatch and parallel workers.
- These are deterministic local contracts with mocked external operations; no
  scheduler, AWS Batch, S3, or quota-table integration is exercised.

## CI

The Julia test workflow includes both job contract files. No checked-in workflow
submits SLURM jobs, runs workstation campaigns, provisions AWS, submits a real
Batch job, transfers artifacts through S3, or tests the headless project on a
cluster. HPC and cloud execution are therefore not CI-verified capabilities.

## Invariants

- Job status is monotonic once terminal; terminal snapshots cannot be mutated by
  a late progress or completion event.
- External AWS CLI calls never execute while the in-process job registry lock is
  held.
- A persisted cancel-dispatch claim prevents concurrent callers from issuing
  duplicate AWS cancel/terminate calls; a failed dispatch releases the claim for
  retry and an abandoned claim expires after a bounded interval.
- Local cancellation is cooperative and must be observed at explicit compute
  checkpoints; it does not use asynchronous task interruption.
- Scale-out atlas builds use one SQLite writer per shard and merge afterward;
  multiple jobs must not write one SQLite database concurrently.
- The headless wrapper loads the shared numerical source with visualization
  disabled; it must not fork numerical semantics.
- The training scheduler entrypoint intentionally exits unsuccessfully until a
  real training implementation exists.

## Known gaps

- SLURM scripts embed site-specific modules, partitions, resource requests, and
  storage guards; portability has not been abstracted or tested.
- Shell syntax, scheduler dependency chains, resume behavior, partial-copy
  recovery, and output manifests have no automated integration gate.
- Cloud auth, quotas, CLI failure modes, Batch-to-worker execution, and artifact
  retrieval are not tested against live services.
- Several scheduler comments retain historical phase labels that disagree with
  files now present; use source behavior and generated run evidence, not those
  labels, to assess readiness.

## Change protocol

1. Preserve the transition table and add an adversarial race test before
   changing job lifecycle, locks, retries, or cancellation.
2. Keep external calls outside locks and publish state only through the guarded
   transition helpers.
3. Change a pipeline output only with a versioned result/manifest contract and a
   verifier that detects incomplete shards or mixed source revisions.
4. Treat each cluster/cloud run as unverified until its config, code revision,
   logs, outputs, and verification command are captured together.

## Verified against

- Source commit: `f9c65a5`
- Evidence inspected: job state source and contracts, worker/result schema,
  headless wrappers, SLURM/workstation scripts, and CI workflow wiring.
- Boundary: local state-machine contracts are verified; no actual SLURM,
  workstation campaign, AWS Batch, S3, Cognito, or quota integration is claimed.

# 0003: Reconcile ambiguous AWS Batch submissions without resubmitting

- Status: accepted in the 2026-07-11 working tree
- Date: 2026-07-11
- Verified against: `f2ca13c`
- Historical implementation anchor: `1177a3d`
- Owners: `backend-runtime`, `batch-hpc`
- Supersedes: implicit retry of an unconfirmed Batch submission
- Superseded by: none

## Problem in plain language

If the broker sends a Batch job successfully but loses the response, it cannot
tell whether the job exists. Sending the same request again may start the same
scientific computation twice, consume quota twice, and produce competing
artifacts. Refusing every retry avoids duplication but can also leave a real
remote job detached from its local record.

The decision is to permit one remote submission attempt per canonical local job
identity. An uncertain outcome is recovered by finding and validating that
specific remote job, never by issuing another blind submission.

## Context and observed constraints

- AWS Batch `SubmitJob` does not provide a caller-supplied idempotency token that
  the broker can bind to its canonical job ID. A transport error or interrupted
  process can therefore leave the remote outcome ambiguous.
- The broker already owns durable canonical state in
  [`webapp/src/jobs.jl`](../../webapp/src/jobs.jl). The local job ID, queue,
  definition, command, tags, and artifact locations are sufficient to create a
  frozen submission identity before remote work begins.
- The AWS CLI retries operations by default. A retry hidden inside one CLI
  invocation would violate an application-level one-attempt rule, so the
  submission call must explicitly use one SDK attempt.
- A Batch job name is not a unique key. Reconciliation must validate the job ID
  returned by `ListJobs` with `DescribeJobs` and compare the persisted queue,
  definition, canonical tag, and exact worker command.
- `ListJobs` is paginated and status-scoped. A correct recovery path must inspect
  every bounded page and every relevant status rather than treating the first
  result page as the complete population.
- Cancellation can race with response loss and reconciliation. A late remote
  observation must not overwrite a newer local cancellation state.
- The executable state machine and its crash/race fixtures are in
  [`webapp/test/jobs_submission_reconciliation_contract.jl`](../../webapp/test/jobs_submission_reconciliation_contract.jl)
  and [`webapp/test/jobs_cancellation_contract.jl`](../../webapp/test/jobs_cancellation_contract.jl).

## Decision

### Freeze submission identity before remote I/O

The first canonical record stores a versioned `submission_plan` containing the
complete canonical job ID-derived name, queue, definition, Batch region,
optional 12-digit account identity, artifact URIs, tags, container overrides,
and exact worker command. Recovery uses this persisted plan even if the process
configuration later changes.

The broker publishes the worker input and then commits a `dispatch_started`
state before calling `SubmitJob`. Atomic rename is only the logical local
commit; a one-use in-memory submission authorization is issued only after the
exact parent directory fsync is confirmed. If that fsync and its exact retry
both fail, the committed record remains reconcilable but the process performs
zero `SubmitJob` calls. The CLI invocation sets `AWS_MAX_ATTEMPTS=1`, and no
code path may call `SubmitJob` again for that canonical job ID after the
boundary has committed.

### Treat an ambiguous response as recoverable state

A valid returned Batch job ID advances the record to `accepted`. A transport
failure, malformed response, process interruption after the dispatch boundary,
or other outcome that cannot prove non-acceptance advances the record to
`reconciling`. It is not treated as a definite submission failure and it is not
retried.

A record interrupted before `dispatch_started` is a known zero-attempt case and
becomes `failed_before_dispatch`. This intentionally prefers a possible
zero-run outcome over an uncontrolled duplicate run.

### Reconcile by strict persisted identity

Recovery lists jobs in the persisted queue and region, across the bounded
relevant status population and all pages, using the persisted exact job name.
Every DescribeJobs chunk must return exactly one well-formed detail for every
requested ListJobs ID; missing, duplicate, malformed, or unrequested details
make the entire observation retryable. Only a complete observation may apply
the zero/one/many rule. Every candidate job ID must then match all of the
following:

- the ID came from the current `ListJobs` result set;
- exact job name;
- canonical `BneJobId` tag;
- persisted queue and job definition; and
- exact persisted worker command.

Persisted full queue/definition ARNs compare only by full equality. Plain
resource names may match an ARN resource name, but any returned ARN must remain
in the persisted region and, when configured, the persisted account.

Zero matches keep the record retryably reconciling. After the declared attempt
threshold the public diagnostic becomes `unknown`, but later reads may still
adopt a subsequently visible exact match. One exact match is adopted. Multiple
exact matches become `conflict`, and the broker records their IDs without
choosing one.

### Preserve newer local intent

External AWS calls run without the shared job registry lock. When a response
returns, the broker reloads current canonical state and merges only transitions
that are still valid. A cancellation or terminal state committed meanwhile has
priority over a late submission, describe, or reconciliation response. If an
adopted remote job is already locally cancel-requested, the existing remote
cancellation path is invoked instead of restoring an active local status.

### Keep legacy uncertainty explicit

A legacy record that already contains a remote Batch job ID may be observed as
accepted. A legacy record without enough persisted identity is labeled
`legacy_submission_unknown`; the broker does not invent a plan from current
configuration and does not submit again.

## Alternatives considered

### Retry `SubmitJob` after timeout

This could reduce zero-run outcomes, but without an idempotency key it can start
duplicate jobs. It was rejected because duplicate compute, quota, and artifact
publication are harder to detect and repair than an explicit unknown state.

### Use only the Batch job name for reconciliation

The name is searchable and convenient, but it is not guaranteed unique and can
collide with manual or historical jobs. It was rejected as insufficient proof
of ownership; the tag, queue, definition, command, and listed job ID are all
required.

### Generate a new local job after an ambiguous result

This would give the retry a new identity but would not cancel or account for the
possibly accepted first job. It remains a deliberate user-level resubmission,
not an automatic recovery mechanism.

### Claim exactly-once execution

No local protocol can prove exactly-once remote execution across the
dispatch-before-call crash window and an external service boundary. The chosen
claim is at-most-one broker submission attempt per canonical job ID, plus
reconciliation of a potentially accepted attempt.

## Consequences

### Benefits

- Response loss and process restart cannot trigger an automatic duplicate
  `SubmitJob` call for the same canonical job ID.
- Recovery uses immutable request identity rather than current environment
  configuration.
- Zero, one, and multiple candidates remain distinguishable and auditable.
- Cancellation and other newer canonical state cannot be erased by a stale AWS
  response.
- Legacy records fail explicitly when they lack enough identity to reconcile.

### Costs and risks

- A crash after committing `dispatch_started` but before AWS receives the call
  can produce a zero-run job that remains unknown. This is an intentional
  at-most-once trade-off.
- Reconciliation performs bounded `ListJobs` and `DescribeJobs` calls and can be
  delayed by eventual visibility or external service failures.
- The persisted submission plan is part of durable compatibility. Changing its
  command or identity semantics requires a protocol version and migration.
- The contract is process-local coordination around one canonical store. It
  does not establish multi-broker ownership of the same job directory.
- Local mocked contracts do not prove behavior in a live AWS account.

## Migration and rollback

New jobs use `bne-aws-batch-submission/v1.1.0`. Existing records with a Batch
job ID remain readable and use an explicitly labelled legacy runtime-region
path. Older plans without persisted region/account identity are not guessed
into v1.1 reconciliation; records without sufficient identity retain explicit
legacy uncertainty and are not retroactively resubmitted.

Rollback may continue to read the ordinary job status fields, but it must not
silently resume retrying `SubmitJob` for records that contain a dispatch marker.
Replacing this protocol requires evidence for an equivalent or stronger
idempotency boundary and fixtures for every crash and cancellation race named
above.

## Verification

From the repository root:

```text
julia --project=webapp webapp/test/jobs_submission_reconciliation_contract.jl
julia --project=webapp webapp/test/jobs_cancellation_contract.jl
JULIA_NUM_THREADS=auto julia --project=webapp webapp/test/runtests.jl
```

The focused contract proves one submission attempt, one CLI retry attempt,
pre-dispatch failure, dispatch-directory durability failure/retry,
response-loss adoption, cold recovery, pagination, complete DescribeJobs chunk
coverage, strict region/account/ARN candidate rejection, zero/one/many
outcomes, cancellation races, legacy behavior, concurrent reconciliation
ownership, and persisted-plan behavior after configuration drift. The broader
jobs suite protects terminal monotonicity and external-call lock boundaries.

## Follow-ups

- [ ] Record live AWS observations separately if an operator runs the protocol;
  do not promote mocked evidence into a deployment claim (`batch-hpc`).
- [ ] Define multi-broker canonical-store ownership before running two brokers
  against the same job directory (`backend-runtime`).
- [ ] Add a separately versioned client submission-idempotency contract only if
  callers need safe HTTP request retries before a canonical job ID is returned
  (`backend-runtime`).

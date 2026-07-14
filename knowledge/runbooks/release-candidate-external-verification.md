---
title: Release-candidate external verification
status: prepared-not-executed
---

# Release-candidate external verification

This runbook qualifies one immutable release candidate across the Registry,
Compose/TLS, AWS, Slurm, and macOS lanes. It is a procedure, not evidence that
any external service accepted or executed the candidate.

## Authorization and identity gate

Do not start an external lane without an operator naming the environment and
authorizing its credentials, quota, and possible cost. Freeze one clean commit,
one configuration hash, one OCI digest, and one macOS artifact hash before the
first lane. Copy `operations/evidence/release-candidate.template.json` to an
untracked run directory and validate it against
`schemas/release-candidate-evidence.schema.json` after every lane.

Evidence logs must be redacted before hashing. Never store bearer tokens,
cookies, credentials, signed URLs, private host paths, or raw secret-bearing
environment output. A static check, generated command, or mocked service does
not change a lane from `not_run` to `passed`.

## Registry lane

1. Build the source image and record the source commit, OCI labels, and local
   digest.
2. Generate an SBOM for that digest and retain both the document and its hash.
3. Push by digest to the authorized registry, pull the same digest into a clean
   daemon, and prove the pulled manifest digest is identical.
4. Sign the digest with the authorized identity, verify the signature against
   the expected issuer/subject policy, and retain the redacted verification
   output.
5. Exercise deletion or channel rollback according to registry policy; do not
   overwrite the immutable digest.

## Compose and TLS lane

1. Pin Compose to the qualified image digest and hash the rendered,
   secret-redacted configuration.
2. On the authorized domain, start the complete Compose/Nginx stack and wait for
   `/ready`; verify `/health`, `/api/v1/version`, and the browser entrypoint.
3. Verify certificate chain, hostname, expiry, HTTP-to-HTTPS redirect, renewal
   dry-run, and post-renewal reload.
4. Test JSON bodies immediately below, at, and above the 1 MiB boundary and
   confirm the Nginx and application error contracts agree.
5. Deploy the previous digest and prove readiness and data access after rollback.

## AWS lane

1. Record account/region aliases, deployed configuration hash, ECR digest, job
   definition revision, and all redacted resource identifiers.
2. Verify Cognito sign-in/callback/logout and reject a wrong origin, audience,
   issuer, and expired token.
3. Verify quota admission and exhaustion without holding the job-store lock
   during external calls.
4. Submit one idempotency key concurrently and prove at-most-once Batch dispatch;
   reconcile the dispatch claim with the returned external job ID.
5. Exercise queued/running cancellation races and prove terminal-state
   monotonicity.
6. Verify the S3 result manifest, byte length, content type, SHA-256 metadata,
   pre-signed download, and failure behavior for missing or mismatched objects.
7. Roll back broker and worker revisions in the documented order and rerun the
   readiness and one-job smoke.

## Slurm lane

1. Record cluster/partition aliases, module/container identity, commit, and
   submission configuration hash.
2. Submit a bounded fixture, observe queued and running states, and retrieve a
   hash-addressed result artifact.
3. Cancel one queued and one running job; verify scheduler state, worker exit,
   broker reconciliation, and absence of a falsely successful artifact.
4. Submit an invalid job and prove failure is bounded and diagnostic.
5. Restore the prior adapter/configuration and repeat the bounded fixture.

## macOS lane

1. Build from the same commit with the frozen backend resources and record the
   app/DMG hashes before signing.
2. Sign every Mach-O and bundle with the authorized Developer ID, verify the
   sealed resources, submit for notarization, staple the accepted ticket, and
   run Gatekeeper assessment.
3. On a clean supported host, install from the DMG, launch the app, verify the
   loopback backend and authenticated Design Chat boundary, save/reopen a v2
   workspace, and reject a future workspace version.
4. Remove the app and install the previous qualified build; verify project-file
   compatibility and launch after rollback.

## Completion rule

Set `overall_status` to `passed` only when all five lane records refer to the
same frozen identities and each required external observation has a hashed,
redacted evidence record. A skipped, unavailable, or unauthorized environment
stays `not_run` or `blocked`; it is never inferred from local tests.
